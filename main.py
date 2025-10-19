# main.py
import os
import re
import json
import pytz
import time
import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    Message, InputMediaPhoto, InputMediaVideo
)

# -------------------- ENV --------------------

TOKEN = os.getenv("TOKEN", "").strip()
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("ENV TOKEN пуст или имеет неверный формат. Задайте корректный токен бота.")

CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
if CHANNEL_ID == 0:
    raise RuntimeError("ENV CHANNEL_ID не задан. Пример: -1001758490510")

ADMINS = [int(x) for x in os.getenv("ADMINS", "").replace(" ", "").split(",") if x]
TZ = os.getenv("TZ", "Europe/Moscow")
POST_TIMES = [t.strip() for t in os.getenv("POST_TIMES", "12:00,16:00,20:00").split(",") if t.strip()]
PREVIEW_BEFORE_MIN = int(os.getenv("PREVIEW_BEFORE_MIN", "45"))
ALBUM_URL = os.getenv("ALBUM_URL", "").strip()
CONTACT = os.getenv("CONTACT", "@layoutplacebuy").strip()
DB_PATH = os.getenv("DB_PATH", "/data/data.db")

# -------------------- BOT --------------------

bot = Bot(TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

logger = logging.getLogger("layoutplace_bot")

# -------------------- DB (встроенный) --------------------

def _cx() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    cx = sqlite3.connect(DB_PATH)
    cx.row_factory = sqlite3.Row
    return cx

def db_init() -> None:
    cx = _cx()
    with cx:
        cx.execute("""
        CREATE TABLE IF NOT EXISTS queue(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL,    -- JSON [{'type':'photo','file_id':'...'}, ...] или [{'type':'text','text':'...'}]
            caption TEXT,
            created_at INTEGER NOT NULL
        )
        """)
        cx.execute("""
        CREATE TABLE IF NOT EXISTS meta(
            k TEXT PRIMARY KEY,
            v TEXT
        )
        """)
    cx.close()

def q_enqueue(payload: List[Dict[str, Any]], caption: Optional[str]) -> int:
    cx = _cx()
    with cx:
        cur = cx.execute(
            "INSERT INTO queue(payload, caption, created_at) VALUES(?,?,?)",
            (json.dumps(payload, ensure_ascii=False), caption or "", int(time.time()))
        )
        qid = cur.lastrowid
    cx.close()
    return qid

def q_dequeue_oldest() -> Optional[Dict[str, Any]]:
    cx = _cx()
    with cx:
        row = cx.execute("SELECT * FROM queue ORDER BY id LIMIT 1").fetchone()
        if not row:
            cx.close()
            return None
        cx.execute("DELETE FROM queue WHERE id=?", (row["id"],))
    cx.close()
    return dict(row)

def q_delete(qid: int) -> bool:
    cx = _cx()
    with cx:
        cur = cx.execute("DELETE FROM queue WHERE id=?", (qid,))
        ok = cur.rowcount > 0
    cx.close()
    return ok

def q_list() -> List[Dict[str, Any]]:
    cx = _cx()
    rows = cx.execute("SELECT * FROM queue ORDER BY id").fetchall()
    cx.close()
    return [dict(r) for r in rows]

def meta_get(k: str) -> Optional[str]:
    cx = _cx()
    row = cx.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    cx.close()
    return row["v"] if row else None

def meta_set(k: str, v: str) -> None:
    cx = _cx()
    with cx:
        cx.execute("INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v))
    cx.close()

# -------------------- Нормализация текста --------------------

def _cleanup_text(t: str) -> str:
    t = t.replace("\r", "").strip()
    # Убираем двойные пробелы
    t = re.sub(r"[ \t]+", " ", t)
    # Нормализуем разделители строк
    lines = [ln.strip() for ln in t.split("\n")]
    # Убираем пустые строки по краям
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines)

def normalize_caption(raw: str) -> str:
    """
    Приводим пост к единому виду.
    Снизу всегда: общий альбом + контакт.
    """
    raw = _cleanup_text(raw)
    # Простейшие правки: "Цена - " -> "Цена — "
    raw = re.sub(r"Цена\s*[:-]\s*", "Цена — ", raw, flags=re.IGNORECASE)
    raw = re.sub(r"Состояние\s*[:-]\s*", "Состояние : ", raw, flags=re.IGNORECASE)
    raw = re.sub(r"Размер\s*[:-]\s*", "Размер: ", raw, flags=re.IGNORECASE)

    tail = []
    if ALBUM_URL:
        tail.append(f"Общий альбом: {ALBUM_URL}")
    tail.append(f"Покупка/вопросы: {CONTACT}")

    # Убираем из исходника дубли хвоста, если вдруг были
    t = re.sub(r"Общий альбом:.*\n?", "", raw, flags=re.IGNORECASE)
    t = re.sub(r"Покупка/вопросы:.*\n?", "", t, flags=re.IGNORECASE)

    caption = f"{t}\n\n" + "\n".join(tail)
    return caption.strip()

# -------------------- Медиагруппа: сборщик --------------------

# буфер для медиагрупп: {media_group_id: {'items':[Message,...], 'task': asyncio.Task}}
_media_buf: Dict[str, Dict[str, Any]] = {}

async def _flush_media_group(group_id: str):
    """Склеиваем items медиагруппы в один payload и ставим в очередь."""
    pkg = _media_buf.pop(group_id, None)
    if not pkg:
        return
    items: List[Message] = pkg["items"]

    # собираем медиа
    media_items: List[Dict[str, Any]] = []
    caption_parts: List[str] = []
    for m in items:
        if m.photo:
            # берем максимум качества
            file_id = m.photo[-1].file_id
            media_items.append({"type": "photo", "file_id": file_id})
        elif m.video:
            media_items.append({"type": "video", "file_id": m.video.file_id})
        # ловим подпись, если попалась
        if m.caption and not caption_parts:
            caption_parts.append(m.caption)

    caption = normalize_caption(caption_parts[0]) if caption_parts else ""
    qid = q_enqueue(media_items, caption)
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, f"✅ Альбом поставлен в очередь как #{qid}")
        except Exception:
            pass

def _schedule_flush(group_id: str, delay: float = 1.2):
    """Отложенный флаш, чтобы дождаться всех сообщений альбома."""
    async def _task():
        try:
            await asyncio.sleep(delay)
            await _flush_media_group(group_id)
        except Exception as e:
            logger.exception("flush media group error: %s", e)
    return asyncio.create_task(_task())

# -------------------- Постинг --------------------

_last_channel_msg_id_key = "last_channel_msg_id"

async def _post_payload_to_channel(payload: List[Dict[str, Any]], caption: str) -> int:
    """
    Отправляет медиагруппу или одиночный пост в канал.
    Возвращает message_id первого поста (или единственного).
    """
    # попытка удалить предыдущий дубликат (best-effort)
    try:
        last_id = meta_get(_last_channel_msg_id_key)
        if last_id:
            await bot.delete_message(chat_id=CHANNEL_ID, message_id=int(last_id))
    except Exception:
        pass

    if len(payload) == 0:
        # текстовый пост
        msg = await bot.send_message(CHANNEL_ID, caption or " ")
        meta_set(_last_channel_msg_id_key, str(msg.message_id))
        return msg.message_id

    if len(payload) == 1 and payload[0]["type"] == "photo":
        msg = await bot.send_photo(CHANNEL_ID, photo=payload[0]["file_id"], caption=caption or None)
        meta_set(_last_channel_msg_id_key, str(msg.message_id))
        return msg.message_id

    # медиагруппа
    media: List[Any] = []
    for i, it in enumerate(payload):
        if it["type"] == "photo":
            media.append(InputMediaPhoto(media=it["file_id"], caption=caption if i == 0 else None))
        elif it["type"] == "video":
            media.append(InputMediaVideo(media=it["file_id"], caption=caption if i == 0 else None))
    msgs = await bot.send_media_group(CHANNEL_ID, media=media)
    first_id = msgs[0].message_id
    meta_set(_last_channel_msg_id_key, str(first_id))
    return first_id

async def _send_preview(qrow: Dict[str, Any], when: str):
    """Превью админам: текст + id очереди."""
    qid = qrow["id"]
    caption = (qrow["caption"] or "").strip()
    payload = json.loads(qrow["payload"])

    txt = f"🟡 Предпросмотр #{qid}\nВремя постинга: {when}\n\n{caption or '(без текста)'}"
    for admin_id in ADMINS:
        try:
            if payload and payload[0]["type"] == "photo":
                # картинка + подпись в ЛС админа
                await bot.send_photo(admin_id, photo=payload[0]["file_id"], caption=txt[:1024] if txt else None)
            else:
                await bot.send_message(admin_id, txt[:4096])
        except Exception:
            pass

# -------------------- Планировщик --------------------

def _now_tz() -> datetime:
    return datetime.now(pytz.timezone(TZ))

def _today_str() -> str:
    return _now_tz().strftime("%Y-%m-%d")

def _slot_key(slot: str) -> str:
    return f"slot:{_today_str()}:{slot}"

def _preview_key(slot: str) -> str:
    return f"preview:{_today_str()}:{slot}"

async def scheduler_loop():
    logger.info("Scheduler запущен.")
    while True:
        try:
            now = _now_tz()
            for slot in POST_TIMES:
                # превью
                try:
                    hh, mm = [int(x) for x in slot.split(":")]
                    slot_dt = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
                    prev_dt = slot_dt - timedelta(minutes=PREVIEW_BEFORE_MIN)
                except Exception:
                    continue

                # превью: единожды
                if prev_dt <= now < slot_dt and meta_get(_preview_key(slot)) != "done":
                    rows = q_list()
                    if rows:
                        await _send_preview(rows[0], when=slot)
                        meta_set(_preview_key(slot), "done")

                # постинг слота: единожды
                if abs((slot_dt - now).total_seconds()) <= 25 and meta_get(_slot_key(slot)) != "done":
                    row = q_dequeue_oldest()
                    if row:
                        payload = json.loads(row["payload"])
                        caption = (row["caption"] or "").strip()
                        await _post_payload_to_channel(payload, caption)
                        for admin_id in ADMINS:
                            try:
                                await bot.send_message(admin_id, f"✅ Опубликован пост из очереди #{row['id']} ({slot})")
                            except Exception:
                                pass
                    meta_set(_slot_key(slot), "done")

            # в полночь сбрасываем ключи превью/слотов
            if now.hour == 0 and now.minute < 2:
                # простая очистка по смене даты
                pass
        except Exception as e:
            logger.exception("scheduler error: %s", e)

        await asyncio.sleep(5)

# -------------------- Команды --------------------

@dp.message(Command("start"))
async def cmd_start(m: Message):
    text = (
        "<b>Layoutplace Bot</b>\n\n"
        "Команды:\n"
        "• <code>/add_post</code> — просто перешли сюда пост/альбом.\n"
        "• <code>/queue</code> — показать очередь.\n"
        "• <code>/post_oldest</code> — запостить самый старый элемент вручную.\n"
        "• <code>/delete &lt;id&gt;</code> — удалить элемент из очереди.\n\n"
        f"Расписание: <code>{', '.join(POST_TIMES)}</code>, превью за <code>{PREVIEW_BEFORE_MIN} мин</code>.\n"
        "Альбом и контакт внизу подписи — фиксированы."
    )
    await m.answer(text)

@dp.message(Command("help"))
async def cmd_help(m: Message):
    await cmd_start(m)

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    rows = q_list()
    if not rows:
        await m.answer("Очередь пуста.")
        return
    lines = [f"Всего: {len(rows)}"]
    for r in rows:
        created = datetime.fromtimestamp(r["created_at"]).strftime("%d.%m %H:%M")
        payload = json.loads(r["payload"])
        kind = "album" if len(payload) > 1 else ("photo" if payload and payload[0]["type"] != "text" else "text")
        lines.append(f"#{r['id']} [{kind}] {created}")
    await m.answer("\n".join(lines))

@dp.message(Command("delete"))
async def cmd_delete(m: Message, command: CommandObject):
    if m.from_user and ADMINS and m.from_user.id not in ADMINS:
        return
    if not command.args or not command.args.strip().isdigit():
        await m.answer("Использование: <code>/delete &lt;id&gt;</code>")
        return
    qid = int(command.args.strip())
    ok = q_delete(qid)
    await m.answer("🗑 Удалено" if ok else "Не найдено")

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    if m.from_user and ADMINS and m.from_user.id not in ADMINS:
        return
    row = q_dequeue_oldest()
    if not row:
        await m.answer("Очередь пуста.")
        return
    payload = json.loads(row["payload"])
    caption = (row["caption"] or "").strip()
    await _post_payload_to_channel(payload, caption)
    await m.answer(f"✅ Опубликован #{row['id']} вручную.")

# -------------------- Приём контента --------------------

@dp.message(F.media_group_id)
async def on_media_group(m: Message):
    gid = str(m.media_group_id)
    pkg = _media_buf.get(gid)
    if not pkg:
        pkg = _media_buf[gid] = {"items": [], "task": None}
    pkg["items"].append(m)
    # переустанавливаем отложенный флаш
    if pkg["task"]:
        pkg["task"].cancel()
    pkg["task"] = _schedule_flush(gid, delay=1.3)

@dp.message(F.photo | F.video)
async def on_single_media(m: Message):
    # одиночная фотка/видео
    if m.media_group_id:
        return  # медиагруппу ловим в другом хэндлере
    payload: List[Dict[str, Any]] = []
    if m.photo:
        payload.append({"type": "photo", "file_id": m.photo[-1].file_id})
    elif m.video:
        payload.append({"type": "video", "file_id": m.video.file_id})

    caption = normalize_caption(m.caption or "")
    qid = q_enqueue(payload, caption)
    await m.answer(f"✅ Пост #{qid} добавлен в очередь и будет опубликован автоматически.")

@dp.message(F.text)
async def on_text(m: Message):
    # текстовый пост
    t = m.text or m.caption or ""
    caption = normalize_caption(t)
    payload = [{"type": "text", "text": caption}]
    qid = q_enqueue(payload, caption)
    await m.answer(f"✅ Текстовый пост #{qid} добавлен в очередь.")

# -------------------- Запуск --------------------

async def run_bot():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s:%(message)s")
    db_init()
    logger.info("🚀 Стартуем Layoutplace Bot...")
    logger.info("Scheduler TZ=%s, times=%s, preview_before=%s мин", TZ, POST_TIMES, PREVIEW_BEFORE_MIN)

    # 👉 критично для устранения 409, если вдруг активен webhook
    await bot.delete_webhook(drop_pending_updates=False)

    asyncio.create_task(scheduler_loop())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(run_bot())
