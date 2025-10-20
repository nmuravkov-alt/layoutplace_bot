import os
import json
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pytz
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputMediaPhoto,
    InputMediaVideo,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ======================
# ЛОГГЕР
# ======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s"
)
log = logging.getLogger("layoutplace_bot")
log_sched = logging.getLogger("layoutplace_scheduler")

# ======================
# ENV
# ======================
TOKEN = os.getenv("TOKEN")
ADMINS = [int(x) for x in os.getenv("ADMINS", "").split(",") if x.strip()]
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
ALBUM_URL = os.getenv("ALBUM_URL")
CONTACT = os.getenv("CONTACT")
POST_TIMES = [s.strip() for s in os.getenv("POST_TIMES", "12:00,16:00,20:00").split(",")]
PREVIEW_BEFORE_MIN = int(os.getenv("PREVIEW_BEFORE_MIN", "45"))
TZ = os.getenv("TZ", "Europe/Moscow")

if not TOKEN:
    raise RuntimeError("ENV TOKEN пуст или имеет неверный формат. Задайте корректный токен бота.")

bot = Bot(
    token=TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()
tz = pytz.timezone(TZ)

scheduler = AsyncIOScheduler(timezone=tz)

# ======================
# АДАПТЕР ДЛЯ БД
# ======================
# Ожидаем таблицу queue с полями:
# id INTEGER PK, items_json TEXT, caption TEXT, src_chat_id INTEGER NULL, src_msg_id INTEGER NULL, created_at INTEGER
# и функции:
# - peek_oldest(): dict | None
# - dequeue_oldest(): dict | None
# - remove_by_id(qid: int) -> None
# - enqueue(items: list[dict], caption: str, src: tuple[int|None,int|None]) -> int
# - get_count() -> int

def _import_db():
    # пытаемся разные имена из твоих прошлых версий
    mod = __import__("storage.db", fromlist=["*"])

    def pick(*names):
        for n in names:
            if hasattr(mod, n):
                return getattr(mod, n)
        return None

    return {
        "init_db": pick("init_db"),
        "peek_oldest": pick("peek_oldest", "get_oldest"),
        "dequeue_oldest": pick("dequeue_oldest"),
        "remove_by_id": pick("remove_by_id", "delete_post"),
        "enqueue": pick("enqueue", "add_post"),
        "get_count": pick("get_count"),
    }

_db = _import_db()

if _db["init_db"]:
    try:
        _db["init_db"]()
    except Exception as e:
        log.warning(f"init_db() failed: {e}")

def db_peek_oldest() -> Optional[dict]:
    f = _db["peek_oldest"]
    return f() if f else None

def db_dequeue_oldest() -> Optional[dict]:
    f = _db["dequeue_oldest"]
    return f() if f else None

def db_remove_by_id(qid: int):
    f = _db["remove_by_id"]
    if not f:
        raise RuntimeError("remove_by_id() не найден в storage.db")
    return f(qid)

def db_enqueue(items: List[dict], caption: str, src: Optional[tuple]) -> int:
    """
    items: [{type: 'photo'|'video', file_id: str}, ...]
    caption: str
    src: (src_chat_id, src_msg_id) or (None, None)
    """
    f = _db["enqueue"]
    if not f:
        raise RuntimeError("enqueue() / add_post() не найден в storage.db")
    # попробуем разные сигнатуры
    try:
        return f(items=items, caption=caption, src=src)
    except TypeError:
        try:
            # некоторые версии ожидают распакованные значения
            if src is None:
                return f(items=items, caption=caption, src_chat_id=None, src_msg_id=None)
            else:
                return f(items=items, caption=caption, src_chat_id=src[0], src_msg_id=src[1])
        except TypeError:
            # самые старые могли принимать payload/json
            payload = json.dumps(items, ensure_ascii=False)
            return f(payload=payload, caption=caption, src=src)

def db_get_count() -> int:
    f = _db["get_count"]
    try:
        return int(f()) if f else 0
    except Exception:
        return 0

# ======================
# ХЕЛПЕРЫ ДЛЯ ТЕКСТА/МЕДИА
# ======================

def fixed_footer() -> str:
    return (
        f"\n\nОбщий альбом: {ALBUM_URL}\n"
        f"Покупка/вопросы: {CONTACT}"
    )

def build_final_caption(raw_caption: Optional[str]) -> str:
    raw_caption = (raw_caption or "").strip()
    lines = [l.strip() for l in raw_caption.splitlines()]
    lines = [l for l in lines if l]
    body = "\n".join(lines)
    return (body + fixed_footer()).strip()

def build_media_group(items: List[dict], caption: Optional[str]):
    media = []
    for idx, it in enumerate(items):
        t = (it.get("type") or "").lower()
        if t == "photo":
            if idx == 0 and caption:
                media.append(InputMediaPhoto(media=it["file_id"], caption=caption))
            else:
                media.append(InputMediaPhoto(media=it["file_id"]))
        elif t == "video":
            if idx == 0 and caption:
                media.append(InputMediaVideo(media=it["file_id"], caption=caption))
            else:
                media.append(InputMediaVideo(media=it["file_id"]))
    return media

# ======================
# ПРЕВЬЮ: КНОПКИ
# ======================

def preview_kb(qid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="✅ Опубликовать", callback_data=f"preview:post:{qid}"),
        InlineKeyboardButton(text="🕓 Отложить", callback_data=f"preview:skip:{qid}"),
    )
    kb.row(InlineKeyboardButton(text="🗑 Удалить", callback_data=f"preview:delete:{qid}"))
    return kb.as_markup()

# ======================
# ПУБЛИКАЦИЯ
# ======================

async def _delete_old_source_if_possible(task: dict):
    try:
        src_chat_id = task.get("src_chat_id")
        src_msg_id = task.get("src_msg_id")
        if not src_chat_id or not src_msg_id:
            return
        if int(src_chat_id) != int(CHANNEL_ID):
            return
        try:
            await bot.delete_message(chat_id=CHANNEL_ID, message_id=src_msg_id)
        except Exception as e:
            log_sched.warning(f"Не смог удалить старое сообщение {CHANNEL_ID}/{src_msg_id}: {e}")
    except Exception:
        pass

async def _publish_task(task: dict):
    items = json.loads(task["items_json"]) if task.get("items_json") else []
    final_caption = build_final_caption(task.get("caption") or "")

    # Сначала удалим старый дубликат (если это тот же канал и у нас есть id)
    await _delete_old_source_if_possible(task)

    if len(items) >= 2:
        media = build_media_group(items, caption=final_caption)
        await bot.send_media_group(chat_id=CHANNEL_ID, media=media)
    elif len(items) == 1:
        it = items[0]
        t = (it.get("type") or "").lower()
        if t == "photo":
            await bot.send_photo(CHANNEL_ID, it["file_id"], caption=final_caption)
        elif t == "video":
            await bot.send_video(CHANNEL_ID, it["file_id"], caption=final_caption)
        else:
            await bot.send_message(CHANNEL_ID, final_caption)
    else:
        await bot.send_message(CHANNEL_ID, final_caption)

# ======================
# ПЛАНИРОВЩИК: ПРЕВЬЮ + СЛОТЫ
# ======================

_PREVIEW_SENT: set[int] = set()

async def send_preview_to_admins(task: dict):
    try:
        items = json.loads(task["items_json"]) if task.get("items_json") else []
    except Exception:
        items = []
    final_caption = build_final_caption(task.get("caption") or "")

    for admin_id in ADMINS:
        try:
            if len(items) >= 2:
                media = build_media_group(items, caption=final_caption)
                await bot.send_media_group(chat_id=admin_id, media=media)
                await bot.send_message(
                    chat_id=admin_id,
                    text=f"Предпросмотр к посту ID <code>{task['id']}</code>",
                    reply_markup=preview_kb(int(task["id"]))
                )
            elif len(items) == 1:
                it = items[0]
                t = (it.get("type") or "").lower()
                if t == "photo":
                    await bot.send_photo(admin_id, it["file_id"], caption=final_caption, reply_markup=preview_kb(int(task["id"])))
                elif t == "video":
                    await bot.send_video(admin_id, it["file_id"], caption=final_caption, reply_markup=preview_kb(int(task["id"])))
                else:
                    await bot.send_message(admin_id, final_caption, reply_markup=preview_kb(int(task["id"])))
            else:
                await bot.send_message(admin_id, final_caption, reply_markup=preview_kb(int(task["id"])))
        except Exception as e:
            log.warning(f"Не удалось отправить превью админу {admin_id}: {e}")

def _parse_hhmm(s: str):
    h, m = s.split(":")
    return int(h), int(m)

async def preview_job():
    task = db_peek_oldest()
    if not task:
        return

    qid = int(task["id"])
    if qid in _PREVIEW_SENT:
        return

    now = datetime.now(tz)
    for hhmm in POST_TIMES:
        h, m = _parse_hhmm(hhmm)
        slot_dt = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if slot_dt <= now:
            slot_dt = slot_dt + timedelta(days=1)
        preview_dt = slot_dt - timedelta(minutes=PREVIEW_BEFORE_MIN)
        if abs((now - preview_dt).total_seconds()) <= 59:
            await send_preview_to_admins(task)
            _PREVIEW_SENT.add(qid)
            break

async def scheduled_post():
    # стандартный автопост в слот: публикуем самый старый и удаляем
    task = db_dequeue_oldest()
    if not task:
        return
    await _publish_task(task)

# ======================
# CALLBACK-и превью
# ======================

@dp.callback_query(F.data.startswith("preview:"))
async def on_preview_buttons(cq: CallbackQuery):
    try:
        _, action, sid = cq.data.split(":")
        qid = int(sid)
    except Exception:
        await cq.answer("Не понял действие", show_alert=True)
        return

    if action == "post":
        task = db_dequeue_oldest()
        if not task or int(task["id"]) != qid:
            await cq.answer("Этот элемент уже не первый в очереди", show_alert=True)
            return
        await _publish_task(task)
        await cq.message.answer(f"✅ Опубликовано и удалено из очереди: ID {qid}")
        await cq.answer()
    elif action == "delete":
        try:
            db_remove_by_id(qid)
            await cq.message.answer(f"🗑 Удалено из очереди: ID {qid}")
        except Exception as e:
            await cq.message.answer(f"Не удалось удалить: {e}")
        await cq.answer()
    else:
        await cq.answer("Оставил в очереди", show_alert=False)

# ======================
# ОБРАБОТКА ВХОДЯЩИХ (пересылаемые посты/альбомы)
# ======================

# Буфер альбомов: media_group_id -> {items:[], caption:str, src:(chat_id,msg_id), touched:datetime}
_ALBUM_BUF: Dict[str, dict] = {}

def _src_from_message(m: Message):
    # если пересылка из канала — запомним для попытки удаления дубля при публикации
    try:
        if m.forward_from_chat and m.forward_from_chat.type == ChatType.CHANNEL:
            return (m.forward_from_chat.id, m.forward_from_message_id or m.message_id)
    except Exception:
        pass
    return (None, None)

def _append_item_from_message(m: Message) -> Optional[dict]:
    # Возвращает dict с media или None
    if m.photo:
        # лучшее качество — последний элемент
        return {"type": "photo", "file_id": m.photo[-1].file_id}
    if m.video:
        return {"type": "video", "file_id": m.video.file_id}
    return None

async def _flush_album_group(group_id: str):
    data = _ALBUM_BUF.pop(group_id, None)
    if not data:
        return
    items = data["items"]
    caption = data["caption"]
    src = data["src"]
    qid = db_enqueue(items=items, caption=caption, src=src)
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, f"Добавлен в очередь (альбом) ID {qid}. Сейчас в очереди: {db_get_count()}")
        except Exception:
            pass

async def _album_collector_loop():
    # Периодически проверяет группы и сбрасывает те, которые «затихли»
    while True:
        try:
            now = datetime.now(tz)
            stale: List[str] = []
            for gid, data in list(_ALBUM_BUF.items()):
                if (now - data["touched"]).total_seconds() >= 1.2:
                    stale.append(gid)
            for gid in stale:
                await _flush_album_group(gid)
        except Exception as e:
            log.warning(f"album collector loop error: {e}")
        await asyncio.sleep(0.6)

@dp.message(F.media_group_id)
async def on_album_piece(m: Message):
    gid = m.media_group_id
    it = _append_item_from_message(m)
    if gid not in _ALBUM_BUF:
        _ALBUM_BUF[gid] = {
            "items": [],
            "caption": (m.caption or "").strip(),
            "src": _src_from_message(m),
            "touched": datetime.now(tz)
        }
    if it:
        _ALBUM_BUF[gid]["items"].append(it)
    # если подпись пришла позже/раньше — обновим
    if m.caption:
        _ALBUM_BUF[gid]["caption"] = (m.caption or "").strip()
    _ALBUM_BUF[gid]["touched"] = datetime.now(tz)

@dp.message(F.photo | F.video)
async def on_single_media(m: Message):
    # одиночное фото/видео (не альбом)
    it = _append_item_from_message(m)
    if not it:
        return
    items = [it]
    caption = (m.caption or "").strip()
    src = _src_from_message(m)
    qid = db_enqueue(items=items, caption=caption, src=src)
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, f"Добавлен в очередь (медиа) ID {qid}. Сейчас в очереди: {db_get_count()}")
        except Exception:
            pass

@dp.message(F.text & ~F.media_group_id)
async def on_text(m: Message):
    # Текстовый пост (редко, но поддержим)
    txt = m.text.strip()
    if txt.startswith("/"):
        return
    items = []  # без медиа
    src = _src_from_message(m)
    qid = db_enqueue(items=items, caption=txt, src=src)
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, f"Добавлен в очередь (текст) ID {qid}. Сейчас в очереди: {db_get_count()}")
        except Exception:
            pass

# ======================
# КОМАНДЫ
# ======================

HELP_TEXT = (
    "Команды:\n"
    "/queue — показать размер очереди\n"
    "/post_oldest — опубликовать самый старый пост сейчас\n"
    "/help — помощь\n\n"
    "Просто пересылай мне посты (одиночные или альбомы) из канала — я положу в очередь, пришлю превью за 45 минут до слота и опубликую в указанное время."
)

@dp.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(HELP_TEXT, disable_web_page_preview=True)

@dp.message(Command("help"))
async def cmd_help(m: Message):
    await m.answer(HELP_TEXT, disable_web_page_preview=True)

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    await m.answer(f"В очереди: {db_get_count()}")

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    task = db_dequeue_oldest()
    if not task:
        await m.answer("Очередь пуста.")
        return
    await _publish_task(task)
    await m.answer(f"Опубликовано: ID {task['id']}")

# ======================
# СТАРТ
# ======================

async def _on_startup():
    log.info("🚀 Стартуем Layoutplace Bot...")
    # планировщик превью раз в минуту
    scheduler.add_job(preview_job, CronTrigger(second="0", minute="*"))
    # слоты автопоста
    for hhmm in POST_TIMES:
        hh, mm = [int(x) for x in hhmm.split(":")]
        scheduler.add_job(scheduled_post, CronTrigger(hour=hh, minute=mm))
    scheduler.start()
    log.info(f"Scheduler TZ={TZ}, times={POST_TIMES}, preview_before={PREVIEW_BEFORE_MIN} мин")
    # альбом-коллектор
    asyncio.create_task(_album_collector_loop())

async def run_bot():
    await _on_startup()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(run_bot())
