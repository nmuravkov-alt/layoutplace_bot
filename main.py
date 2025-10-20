import os
import json
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

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

# ======================
# ENV
# ======================
TOKEN = os.getenv("TOKEN")
if not TOKEN:
    raise RuntimeError("ENV TOKEN пуст или имеет неверный формат. Задайте корректный токен бота.")

ADMINS: List[int] = [int(x) for x in os.getenv("ADMINS", "").split(",") if x.strip()]
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
ALBUM_URL = os.getenv("ALBUM_URL", "").strip()
CONTACT = os.getenv("CONTACT", "").strip()
POST_TIMES: List[str] = [s.strip() for s in os.getenv("POST_TIMES", "12:00,16:00,20:00").split(",") if s.strip()]
PREVIEW_BEFORE_MIN = int(os.getenv("PREVIEW_BEFORE_MIN", "45"))
TZ = os.getenv("TZ", "Europe/Moscow")

tz = pytz.timezone(TZ)

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=tz)

# ======================
# STORAGE DB API
# ======================
# ожидаем файл storage/db.py с функциями:
# init_db(), enqueue(items, caption, src), dequeue_oldest(), peek_all(), delete_by_id(qid), stats()
import storage.db as storage_db

try:
    storage_db.init_db()
    log.info("DB initialized (storage_db.init_db()).")
except Exception as e:
    log.warning(f"init_db failed: {e}")

def db_enqueue(items: List[dict], caption: str, src: Tuple[Optional[int], Optional[int]]) -> int:
    return int(storage_db.enqueue(items, caption, src))

def db_dequeue_oldest() -> Optional[dict]:
    return storage_db.dequeue_oldest()

def db_peek_all() -> List[dict]:
    return storage_db.peek_all()

def db_delete_by_id(qid: int) -> int:
    return int(storage_db.delete_by_id(qid))

def db_stats() -> dict:
    try:
        return storage_db.stats()
    except Exception:
        # совместимость
        return {"queued": len(db_peek_all())}

# ======================
# ТЕКСТ/ПОДПИСИ
# ======================

def fixed_footer() -> str:
    footer = []
    if ALBUM_URL:
        footer.append(f"Общий альбом: {ALBUM_URL}")
    if CONTACT:
        footer.append(f"Покупка/вопросы: {CONTACT}")
    return ("\n\n" + "\n".join(footer)) if footer else ""

def build_final_caption(raw_caption: Optional[str]) -> str:
    raw = (raw_caption or "").strip()
    lines = [l.strip() for l in raw.splitlines() if l.strip()]
    body = "\n".join(lines)
    return (body + fixed_footer()).strip() or fixed_footer().lstrip()

def build_media_group(items: List[dict], caption: Optional[str]):
    media = []
    for idx, it in enumerate(items):
        t = (it.get("type") or "").lower()
        if t == "photo":
            media.append(InputMediaPhoto(media=it["file_id"], caption=caption if idx == 0 and caption else None))
        elif t == "video":
            media.append(InputMediaVideo(media=it["file_id"], caption=caption if idx == 0 and caption else None))
    return media

# ======================
# МЕНЮ
# ======================

def menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="➕ Добавить пост", callback_data="menu:add"),
        InlineKeyboardButton(text="📦 Очередь", callback_data="menu:queue"),
    )
    kb.row(
        InlineKeyboardButton(text="📤 Постить старый", callback_data="menu:post_oldest"),
        InlineKeyboardButton(text="🗑 Удалить пост", callback_data="menu:delete_prompt"),
    )
    kb.row(InlineKeyboardButton(text="🏠 Меню", callback_data="menu:root"))
    return kb.as_markup()

HELP_TEXT = (
    "Это интерактивное меню. Выбирай действие на кнопках ниже:\n\n"
    f"Расписание: {', '.join(POST_TIMES)} (превью за {PREVIEW_BEFORE_MIN} мин)\n"
    "Альбом и контакт внизу подписи — фиксированы."
)

@dp.message(Command("start"))
async def cmd_start(m: Message):
    if ADMINS and m.from_user.id not in ADMINS:
        return
    await m.answer(HELP_TEXT, reply_markup=menu_kb(), disable_web_page_preview=True)

@dp.callback_query(F.data.startswith("menu:"))
async def on_menu(cq: CallbackQuery):
    if ADMINS and cq.from_user.id not in ADMINS:
        await cq.answer()
        return
    action = cq.data.split(":", 1)[1]
    if action == "queue":
        s = db_stats()
        await cq.message.answer(f"Очередь: {s.get('queued', 0)}", reply_markup=menu_kb())
    elif action == "post_oldest":
        task = db_dequeue_oldest()
        if not task:
            await cq.message.answer("Очередь пуста.", reply_markup=menu_kb())
            await cq.answer()
            return
        await publish_task(task)
        await cq.message.answer(f"✅ Опубликовано: ID {task['id']}", reply_markup=menu_kb())
    elif action == "delete_prompt":
        await cq.message.answer("Введи ID из очереди для удаления (смотри /queue).", reply_markup=menu_kb())
    else:
        await cq.message.answer(HELP_TEXT, reply_markup=menu_kb())
    await cq.answer()

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    if ADMINS and m.from_user.id not in ADMINS:
        return
    s = db_stats()
    await m.answer(f"Очередь: {s.get('queued', 0)}")

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    if ADMINS and m.from_user.id not in ADMINS:
        return
    task = db_dequeue_oldest()
    if not task:
        await m.answer("Очередь пуста.")
        return
    await publish_task(task)
    await m.answer(f"✅ Опубликовано: ID {task['id']}")

@dp.message(Command("delete"))
async def cmd_delete(m: Message):
    if ADMINS and m.from_user.id not in ADMINS:
        return
    parts = m.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        await m.answer("Некорректный ID. Попробуй ещё раз.")
        return
    qid = int(parts[1])
    cnt = db_delete_by_id(qid)
    if cnt:
        await m.answer(f"🗑 Удалено: ID {qid}")
    else:
        await m.answer("Не найдено.")

# ======================
# ПРЕВЬЮ
# ======================

def preview_kb(qid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="✅ Опубликовать", callback_data=f"preview:post:{qid}"),
        InlineKeyboardButton(text="🗑 Удалить", callback_data=f"preview:delete:{qid}"),
    )
    return kb.as_markup()

_PREVIEW_SENT: set[int] = set()

def _parse_hhmm(s: str) -> Tuple[int, int]:
    hh, mm = s.split(":")
    return int(hh), int(mm)

async def send_preview_to_admins(task: dict):
    items = json.loads(task.get("payload") or task.get("items_json") or "[]")
    caption = build_final_caption(task.get("caption") or "")
    qid = int(task["id"])

    for admin_id in ADMINS:
        try:
            if len(items) >= 2:
                media = build_media_group(items, caption)
                await bot.send_media_group(admin_id, media)
                await bot.send_message(admin_id, f"Предпросмотр поста ID <code>{qid}</code>", reply_markup=preview_kb(qid))
            elif len(items) == 1:
                it = items[0]
                t = (it.get("type") or "").lower()
                if t == "photo":
                    await bot.send_photo(admin_id, it["file_id"], caption=caption, reply_markup=preview_kb(qid))
                elif t == "video":
                    await bot.send_video(admin_id, it["file_id"], caption=caption, reply_markup=preview_kb(qid))
                else:
                    await bot.send_message(admin_id, caption, reply_markup=preview_kb(qid))
            else:
                await bot.send_message(admin_id, caption, reply_markup=preview_kb(qid))
        except Exception as e:
            log.warning(f"Не смог отправить превью админу {admin_id}: {e}")

async def preview_job():
    posts = db_peek_all()
    if not posts:
        return
    head = posts[0]
    qid = int(head["id"])
    if qid in _PREVIEW_SENT:
        return

    now = datetime.now(tz)
    for slot in POST_TIMES:
        h, m = _parse_hhmm(slot)
        slot_dt = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if slot_dt <= now:
            slot_dt += timedelta(days=1)
        preview_dt = slot_dt - timedelta(minutes=PREVIEW_BEFORE_MIN)
        if abs((now - preview_dt).total_seconds()) <= 59:
            await send_preview_to_admins(head)
            _PREVIEW_SENT.add(qid)
            break

@dp.callback_query(F.data.startswith("preview:"))
async def on_preview_buttons(cq: CallbackQuery):
    if ADMINS and cq.from_user.id not in ADMINS:
        await cq.answer()
        return

    try:
        _, action, sid = cq.data.split(":")
        qid = int(sid)
    except Exception:
        await cq.answer("Некорректные данные", show_alert=True)
        return

    if action == "post":
        posts = db_peek_all()
        if not posts or int(posts[0]["id"]) != qid:
            await cq.answer("Этот пост уже не первый в очереди", show_alert=True)
            return
        task = db_dequeue_oldest()
        await publish_task(task)
        await cq.message.answer(f"✅ Опубликовано и удалено из очереди: ID {qid}")
        await cq.answer()
    elif action == "delete":
        cnt = db_delete_by_id(qid)
        if cnt:
            await cq.message.answer(f"🗑 Удалено из очереди: ID {qid}")
        else:
            await cq.message.answer("Не найдено.")
        await cq.answer()
    else:
        await cq.answer()

# ======================
# ПРИЁМ ВХОДЯЩИХ ПОСТОВ (альбом/медиа/текст)
# ======================

def _src_from_message(m: Message) -> Tuple[Optional[int], Optional[int]]:
    try:
        if m.forward_from_chat and m.forward_from_chat.type == ChatType.CHANNEL:
            return (m.forward_from_chat.id, m.forward_from_message_id or m.message_id)
    except Exception:
        pass
    return (None, None)

def _append_item_from_message(m: Message) -> Optional[dict]:
    if m.photo:
        return {"type": "photo", "file_id": m.photo[-1].file_id}
    if m.video:
        return {"type": "video", "file_id": m.video.file_id}
    return None

# буфер альбомов: media_group_id -> {items, caption, src, touched}
_ALBUM_BUF: Dict[str, dict] = {}

async def _flush_album_group(group_id: str):
    data = _ALBUM_BUF.pop(group_id, None)
    if not data:
        return
    qid = db_enqueue(data["items"], data["caption"], data["src"])
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, f"✅ Альбом добавлен в очередь, ID {qid}. Сейчас в очереди: {db_stats().get('queued', 0)}")
        except Exception:
            pass

async def _album_collector_loop():
    while True:
        try:
            now = datetime.now(tz)
            stale = [gid for gid, d in _ALBUM_BUF.items() if (now - d["touched"]).total_seconds() >= 1.2]
            for gid in stale:
                await _flush_album_group(gid)
        except Exception as e:
            log.warning(f"album collector error: {e}")
        await asyncio.sleep(0.6)

@dp.message(F.media_group_id)
async def on_album_piece(m: Message):
    if ADMINS and m.from_user.id not in ADMINS:
        return
    gid = m.media_group_id
    it = _append_item_from_message(m)
    if gid not in _ALBUM_BUF:
        _ALBUM_BUF[gid] = {
            "items": [],
            "caption": (m.caption or "").strip(),
            "src": _src_from_message(m),
            "touched": datetime.now(tz),
        }
    if it:
        _ALBUM_BUF[gid]["items"].append(it)
    if m.caption:
        _ALBUM_BUF[gid]["caption"] = (m.caption or "").strip()
    _ALBUM_BUF[gid]["touched"] = datetime.now(tz)

@dp.message(F.photo | F.video)
async def on_single_media(m: Message):
    if ADMINS and m.from_user.id not in ADMINS:
        return
    it = _append_item_from_message(m)
    if not it:
        return
    qid = db_enqueue([it], (m.caption or "").strip(), _src_from_message(m))
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, f"✅ Медиа добавлено в очередь, ID {qid}. Сейчас в очереди: {db_stats().get('queued', 0)}")
        except Exception:
            pass

@dp.message(F.text & ~F.media_group_id)
async def on_text(m: Message):
    if ADMINS and m.from_user.id not in ADMINS:
        return
    if m.text.startswith("/"):
        return
    qid = db_enqueue([], (m.text or "").strip(), _src_from_message(m))
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, f"✅ Текст добавлен в очередь, ID {qid}. Сейчас в очереди: {db_stats().get('queued', 0)}")
        except Exception:
            pass

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
            await bot.delete_message(CHANNEL_ID, int(src_msg_id))
        except Exception as e:
            log.warning(f"Не смог удалить старый пост {CHANNEL_ID}/{src_msg_id}: {e}")
    except Exception:
        pass

async def publish_task(task: dict):
    items = json.loads(task.get("payload") or task.get("items_json") or "[]")
    caption = build_final_caption(task.get("caption") or "")

    # попытка удалить исходник в канале, чтобы не было дубля
    await _delete_old_source_if_possible(task)

    if len(items) >= 2:
        media = build_media_group(items, caption)
        await bot.send_media_group(CHANNEL_ID, media)
    elif len(items) == 1:
        it = items[0]
        t = (it.get("type") or "").lower()
        if t == "photo":
            await bot.send_photo(CHANNEL_ID, it["file_id"], caption=caption)
        elif t == "video":
            await bot.send_video(CHANNEL_ID, it["file_id"], caption=caption)
        else:
            await bot.send_message(CHANNEL_ID, caption)
    else:
        await bot.send_message(CHANNEL_ID, caption)

# ======================
# АВТОПОСТ В СЛОТЫ
# ======================

async def scheduled_post():
    task = db_dequeue_oldest()
    if not task:
        return
    await publish_task(task)

# ======================
# СТАРТ
# ======================

async def _on_startup():
    log.info("🚀 Стартуем Layoutplace Bot...")
    # превью — каждый 0-й секунды минуты
    scheduler.add_job(preview_job, CronTrigger(second="0", minute="*"))
    # слоты
    for t in POST_TIMES:
        hh, mm = [int(x) for x in t.split(":")]
        scheduler.add_job(scheduled_post, CronTrigger(hour=hh, minute=mm))
    scheduler.start()
    log.info(f"Scheduler TZ={TZ}, times={POST_TIMES}, preview_before={PREVIEW_BEFORE_MIN} мин")
    # сборщик альбомов
    asyncio.create_task(_album_collector_loop())

async def run_bot():
    await _on_startup()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(run_bot())
