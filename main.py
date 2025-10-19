import os
import json
import time
import asyncio
import logging
from typing import List, Dict, Any, Optional, Tuple

import pytz
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    InputMediaPhoto,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.enums.parse_mode import ParseMode
from aiogram.filters import Command

# ============ ЛОГИ ============
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("layoutplace_bot")

# ============ ENV ============
TOKEN = os.getenv("TOKEN", "").strip()
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("ENV TOKEN пуст или имеет неверный формат. Задайте корректный токен бота.")

ADMINS = [int(x) for x in os.getenv("ADMINS", "").replace(" ", "").split(",") if x]
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1000000000000"))
ALBUM_URL = os.getenv("ALBUM_URL", "").strip()
CONTACT = os.getenv("CONTACT", "").strip()
POST_TIMES = [t.strip() for t in os.getenv("POST_TIMES", "12:00,16:00,20:00").split(",") if t.strip()]
PREVIEW_BEFORE_MIN = int(os.getenv("PREVIEW_BEFORE_MIN", "45"))
TZ_NAME = os.getenv("TZ", "Europe/Moscow")

TZ = pytz.timezone(TZ_NAME)

# ============ TELEGRAM ============
bot = Bot(TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

# ============ ХРАНИЛКА (SQLite без внешних модулей) ============
# DB лежит в /data (Volume Railway), чтобы переживать перезапуски
import sqlite3
DB_PATH = "/data/bot.db"
os.makedirs("/data", exist_ok=True)

def db_connect():
    cx = sqlite3.connect(DB_PATH)
    cx.execute("PRAGMA journal_mode=WAL")
    return cx

def db_init():
    cx = db_connect()
    with cx:
        cx.execute("""
        CREATE TABLE IF NOT EXISTS queue(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            items_json TEXT NOT NULL, -- список dict: {"type":"photo","file_id":"..."}
            caption TEXT NOT NULL,
            src_chat_id INTEGER,
            src_msg_id INTEGER,
            created_at INTEGER NOT NULL,
            posted_at INTEGER
        );
        """)
    cx.close()

def enqueue(items: List[Dict[str, Any]], caption: str, src: Optional[Tuple[int,int]]) -> int:
    cx = db_connect()
    with cx:
        cur = cx.execute(
            "INSERT INTO queue(items_json, caption, src_chat_id, src_msg_id, created_at) VALUES(?,?,?,?,?)",
            (json.dumps(items), caption, src[0] if src else None, src[1] if src else None, int(time.time()))
        )
        qid = cur.lastrowid
    cx.close()
    return qid

def dequeue_oldest() -> Optional[Dict[str, Any]]:
    cx = db_connect()
    row = None
    with cx:
        cur = cx.execute("SELECT id, items_json, caption, src_chat_id, src_msg_id FROM queue WHERE posted_at IS NULL ORDER BY id LIMIT 1")
        row = cur.fetchone()
        if not row:
            return None
        # помечаем как опубликованный сразу — чтобы не задублилось при повторном вызове
        cx.execute("UPDATE queue SET posted_at=? WHERE id=?", (int(time.time()), row[0]))
    cx.close()
    return {
        "id": row[0],
        "items": json.loads(row[1]),
        "caption": row[2],
        "src_chat_id": row[3],
        "src_msg_id": row[4],
    }

def peek_oldest() -> Optional[Dict[str, Any]]:
    cx = db_connect()
    row = None
    with cx:
        cur = cx.execute("SELECT id, items_json, caption, src_chat_id, src_msg_id FROM queue WHERE posted_at IS NULL ORDER BY id LIMIT 1")
        row = cur.fetchone()
    cx.close()
    if not row:
        return None
    return {
        "id": row[0],
        "items": json.loads(row[1]),
        "caption": row[2],
        "src_chat_id": row[3],
        "src_msg_id": row[4],
    }

def count_queue() -> int:
    cx = db_connect()
    c = 0
    with cx:
        cur = cx.execute("SELECT COUNT(*) FROM queue WHERE posted_at IS NULL")
        c = cur.fetchone()[0]
    cx.close()
    return c

# ============ УТИЛИТЫ ТЕКСТА ============
STATIC_SUFFIX = (
    "\n\n"
    "Общий альбом: {album}\n"
    "Покупка/вопросы: {contact}"
)

def normalize_text(text: str) -> str:
    # убираем лишние двойные пробелы по краям
    t = (text or "").replace("\r", "")
    lines = [ln.strip() for ln in t.split("\n")]
    # удаляем пустые строки в начале/конце, ужимаем кратные пустые
    cleaned: List[str] = []
    for ln in lines:
        if ln == "" and (not cleaned or cleaned[-1] == ""):
            continue
        cleaned.append(ln)
    if cleaned and cleaned[0] == "":
        cleaned.pop(0)
    if cleaned and cleaned[-1] == "":
        cleaned.pop()
    # финальный текст
    base = "\n".join(cleaned)
    if STATIC_SUFFIX.strip() not in base:
        base = base + STATIC_SUFFIX.format(album=ALBUM_URL, contact=CONTACT)
    return base

def is_admin(user_id: int) -> bool:
    return user_id in ADMINS

# ============ СБОРЩИК АЛЬБОМОВ ============
# При пересылке альбома, Telegram присылает несколько сообщений с одинаковым media_group_id.
# Мы аккумулируем их на короткое время и затем кладём одним постом.
MEDIA_BUFFER: Dict[str, List[Message]] = {}
MEDIA_BUFFER_TTL = 1.0  # секунд ждать догоняющие части альбома

async def _flush_media_group(group_id: str, owner_msg: Message):
    await asyncio.sleep(MEDIA_BUFFER_TTL)
    msgs = MEDIA_BUFFER.pop(group_id, [])
    if not msgs:
        return

    # собираем фото по возрастанию даты
    msgs.sort(key=lambda m: m.date)
    # caption берём из первого, у альбомов caption обычно только в одном элементе
    first_caption = None
    items: List[Dict[str, Any]] = []
    src_tuple = _src_tuple(msgs[0])

    for m in msgs:
        if m.photo:
            file_id = m.photo[-1].file_id
            items.append({"type": "photo", "file_id": file_id})
        if not first_caption and (m.caption and m.caption.strip()):
            first_caption = m.caption

    final_caption = normalize_text(first_caption or "")
    qid = enqueue(items=items, caption=final_caption, src=src_tuple)
    await _notify_admins(
        f"✅ Добавлен в очередь (альбом) ID #{qid}\n"
        f"Всего в очереди: {count_queue()}\n\n"
        f"Предпросмотр через меню /queue, публикация вручную — /post_oldest"
    )

def _src_tuple(m: Message) -> Optional[Tuple[int, int]]:
    """
    Возвращаем (chat_id, message_id) для последующего удаления исходника,
    если сообщение было переслано из канала.
    """
    # В aiogram 3 forward_from_chat может быть, тип сравниваем строкой
    try:
        if m.forward_from_chat and getattr(m.forward_from_chat, "type", "") == "channel":
            return (m.forward_from_chat.id, m.forward_from_message_id)
    except Exception:
        pass
    return None

# ============ ХЕНДЛЕРЫ ============
@dp.message(Command("start"))
async def cmd_start(m: Message):
    help_text = (
        "Привет! Я бот автопостинга.\n\n"
        "<b>Как пользоваться</b>:\n"
        "• Перешли мне пост из своего канала (фото/альбом + описание) — я добавлю в очередь и приведу текст к единому виду.\n"
        "• Автопубликация в канал по времени: <code>{times}</code>\n"
        f"• За {PREVIEW_BEFORE_MIN} минут пришлю превью в ЛС админам.\n\n"
        "<b>Команды</b>:\n"
        "/queue — показать сколько в очереди\n"
        "/post_oldest — запостить самый старый сейчас\n"
        "/help — помощь\n"
    ).format(times=", ".join(POST_TIMES))
    await m.answer(help_text, disable_web_page_preview=True)

@dp.message(Command("help"))
async def cmd_help(m: Message):
    return await cmd_start(m)

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    if not is_admin(m.from_user.id):
        return
    c = count_queue()
    p = peek_oldest()
    txt = f"В очереди: {c}."
    if p:
        txt += f"\nБлижайший ID #{p['id']} (медиа {len(p['items'])} шт)."
    await m.answer(txt)

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    if not is_admin(m.from_user.id):
        return
    task = dequeue_oldest()
    if not task:
        await m.answer("Очередь пуста.")
        return
    ok, err = await _post_to_channel(task)
    if ok:
        await m.answer(f"Опубликовано в канал. ID #{task['id']}")
    else:
        await m.answer(f"Ошибка публикации: {err or 'unknown'}")

# Ловим альбомы
@dp.message(F.media_group_id)
async def on_media_group(m: Message):
    if m.from_user is None:
        return
    # принимаем только от админов (пересылка должна быть от них)
    if not is_admin(m.from_user.id):
        return
    gid = m.media_group_id
    MEDIA_BUFFER.setdefault(gid, []).append(m)
    # планируем флаш через короткую паузу
    asyncio.create_task(_flush_media_group(gid, m))

# Ловим одиночные фото
@dp.message(F.photo)
async def on_single_photo(m: Message):
    if m.from_user is None:
        return
    if not is_admin(m.from_user.id):
        return
    # если это часть альбома — обработается on_media_group
    if m.media_group_id:
        return
    items = [{"type": "photo", "file_id": m.photo[-1].file_id}]
    src = _src_tuple(m)
    final_caption = normalize_text(m.caption or "")
    qid = enqueue(items, final_caption, src)
    await _notify_admins(
        f"✅ Добавлен одиночный пост ID #{qid}\n"
        f"Всего в очереди: {count_queue()}"
    )

# Ловим просто текст (если вдруг надо)
@dp.message(F.text)
async def on_text(m: Message):
    if m.from_user is None or not is_admin(m.from_user.id):
        return
    # текст без фото — тоже кладём (опубликуется как текст)
    caption = normalize_text(m.text)
    qid = enqueue(items=[], caption=caption, src=_src_tuple(m))
    await _notify_admins(
        f"✅ Добавлен текстовый пост ID #{qid}\n"
        f"Всего в очереди: {count_queue()}"
    )

# ============ ПУБЛИКАЦИЯ ============
async def _post_to_channel(task: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    try:
        items = task["items"]
        caption = task["caption"]
        if items:
            if len(items) == 1 and items[0]["type"] == "photo":
                await bot.send_photo(CHANNEL_ID, photo=items[0]["file_id"], caption=caption)
            else:
                media = []
                # caption ставим на первый элемент
                for idx, it in enumerate(items):
                    if it["type"] == "photo":
                        if idx == 0:
                            media.append(InputMediaPhoto(media=it["file_id"], caption=caption))
                        else:
                            media.append(InputMediaPhoto(media=it["file_id"]))
                await bot.send_media_group(CHANNEL_ID, media=media)
        else:
            await bot.send_message(CHANNEL_ID, caption)

        # попытка удалить исходник (если есть)
        if task.get("src_chat_id") and task.get("src_msg_id"):
            try:
                await bot.delete_message(task["src_chat_id"], task["src_msg_id"])
            except Exception as e:
                # не критично — просто лог
                log.warning(f"Не смог удалить старое сообщение {task['src_chat_id']}/{task['src_msg_id']}: {e}")

        return True, None
    except Exception as e:
        log.exception("Ошибка постинга")
        return False, str(e)

# ============ ПРЕВЬЮ ============
async def _send_preview():
    p = peek_oldest()
    if not p:
        return
    text = (
        "🔔 Предпросмотр ближайшего поста:\n"
        f"ID #{p['id']}\n\n"
        f"{p['caption']}"
    )
    # если есть медиа — отправим первое фото с подписью-обрезкой
    for aid in ADMINS:
        try:
            if p["items"]:
                photo_id = None
                for it in p["items"]:
                    if it["type"] == "photo":
                        photo_id = it["file_id"]
                        break
                if photo_id:
                    await bot.send_photo(aid, photo=photo_id, caption=text[:1024])
                    continue
            await bot.send_message(aid, text[:4096])
        except Exception as e:
            log.warning(f"Админ {aid} недоступен: {e}")

# ============ SCHEDULER ============
def _today_times() -> List[datetime]:
    now = datetime.now(TZ)
    res = []
    for t in POST_TIMES:
        hh, mm = t.split(":")
        dt = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
        if dt < now:
            # уже прошёл — сдвигаем на завтра, но это обработаем в основной логике вызовом каждый цикл
            pass
        res.append(dt)
    return res

async def scheduler_loop():
    log.info("Scheduler запущен.")
    last_preview_for_slot: Dict[str, datetime] = {}
    last_post_for_slot: Dict[str, datetime] = {}

    while True:
        try:
            now = datetime.now(TZ)
            for t in POST_TIMES:
                hh, mm = map(int, t.split(":"))
                slot = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
                # если слот уже прошёл сегодня — считаем следующий день
                if slot < now and (now - slot) > timedelta(minutes=1):
                    slot = slot + timedelta(days=1)

                # превью
                preview_time = slot - timedelta(minutes=PREVIEW_BEFORE_MIN)
                key_prev = f"{preview_time.strftime('%Y-%m-%d %H:%M')}"
                if preview_time <= now <= preview_time + timedelta(seconds=55):
                    if last_preview_for_slot.get(key_prev) is None:
                        log.info(f"Время превью для слота {slot.strftime('%H:%M')}")
                        await _send_preview()
                        last_preview_for_slot[key_prev] = now

                # постинг
                key_post = f"{slot.strftime('%Y-%m-%d %H:%M')}"
                if slot <= now <= slot + timedelta(seconds=55):
                    if last_post_for_slot.get(key_post) is None:
                        if count_queue() > 0:
                            log.info(f"Публикация по слоту {slot.strftime('%H:%M')}")
                            task = dequeue_oldest()
                            if task:
                                await _post_to_channel(task)
                        last_post_for_slot[key_post] = now
        except Exception:
            log.exception("Ошибка в scheduler_loop")

        await asyncio.sleep(5)

# ============ УВЕДОМЛЕНИЯ ============
async def _notify_admins(text: str):
    for aid in ADMINS:
        try:
            await bot.send_message(aid, text, disable_web_page_preview=True)
        except Exception as e:
            log.warning(f"Админ {aid} недоступен: {e}")

# ============ RUN ============
async def run_bot():
    db_init()
    # Критично: убираем возможный старый вебхук и хвосты, чтобы не было конфликтов
    await bot.delete_webhook(drop_pending_updates=True)
    # Стартуем планировщик в том же процессе
    asyncio.create_task(scheduler_loop())
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    log.info("🚀 Стартуем Layoutplace Bot...")
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        pass
