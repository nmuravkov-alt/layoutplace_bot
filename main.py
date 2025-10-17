# main.py
import os
import json
import time
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Tuple, Dict, Any

import pytz
import sqlite3

from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import Message, InputMediaPhoto
from aiogram.filters import Command, CommandStart
from aiogram.enums import ParseMode

# -------------------- Конфиг из ENV --------------------
TOKEN = os.getenv("TOKEN", "").strip()
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("ENV TOKEN пуст или имеет неверный формат. Задайте корректный токен бота.")

ADMINS = []
_raw_admins = os.getenv("ADMINS", "").replace(" ", "")
if _raw_admins:
    for chunk in _raw_admins.split(","):
        if chunk.isdigit():
            ADMINS.append(int(chunk))
        else:
            # игнорируем мусор
            pass
if not ADMINS:
    logging.warning("ADMINS не задан — превью и уведомления в ЛС отправляться не будут.")

CHANNEL_ID_ENV = os.getenv("CHANNEL_ID", "").strip()
try:
    CHANNEL_ID = int(CHANNEL_ID_ENV)
except Exception:
    raise RuntimeError(
        f"ENV CHANNEL_ID должен быть числом вида -100..., сейчас: {CHANNEL_ID_ENV!r}"
    )

TZ = os.getenv("TZ", "Europe/Moscow").strip()
ALBUM_URL = os.getenv("ALBUM_URL", "").strip()
BUY_CONTACT = os.getenv("BUY_CONTACT", "@layoutplacebuy").strip()
DB_PATH = os.getenv("DB_PATH", "/data/bot.db").strip()

# Расписание (часы/минуты локальной TZ)
SLOTS = [(12, 0), (16, 0), (20, 0)]
PREVIEW_BEFORE_MIN = 45

# -------------------- Логирование --------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("layoutplace_bot")
log_sched = logging.getLogger("layoutplace_scheduler")

# -------------------- Бот/Диспетчер --------------------
bot = Bot(TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# -------------------- База данных (SQLite) --------------------
def _connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    cx = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    cx.row_factory = sqlite3.Row
    return cx

def init_db() -> None:
    cx = _connect()
    cx.execute("""
        CREATE TABLE IF NOT EXISTS queue(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          items_json TEXT NOT NULL,      -- список dict с типом/фото file_id
          caption    TEXT,
          src_chat_id INTEGER,           -- исходный канал (если переслано)
          src_msg_id  INTEGER,           -- id исходного поста
          created_at  INTEGER NOT NULL
        )
    """)
    cx.close()

def enqueue(items: List[Dict[str, Any]], caption: str, src: Optional[Tuple[int,int]]) -> int:
    cx = _connect()
    cur = cx.cursor()
    src_chat_id, src_msg_id = (src or (None, None))
    cur.execute("""
        INSERT INTO queue(items_json, caption, src_chat_id, src_msg_id, created_at)
        VALUES(?,?,?,?,?)
    """, (json.dumps(items, ensure_ascii=False), caption, src_chat_id, src_msg_id, int(time.time())))
    qid = cur.lastrowid
    cx.close()
    return qid

def dequeue_oldest() -> Optional[sqlite3.Row]:
    cx = _connect()
    cur = cx.cursor()
    cur.execute("SELECT * FROM queue ORDER BY id LIMIT 1")
    row = cur.fetchone()
    if row:
        cur.execute("DELETE FROM queue WHERE id=?", (row["id"],))
        cx.commit()
    cx.close()
    return row

def get_count() -> int:
    cx = _connect()
    cur = cx.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM queue")
    c = cur.fetchone()["c"]
    cx.close()
    return int(c)

def peek_oldest() -> Optional[sqlite3.Row]:
    cx = _connect()
    cur = cx.cursor()
    cur.execute("SELECT * FROM queue ORDER BY id LIMIT 1")
    row = cur.fetchone()
    cx.close()
    return row

# -------------------- Нормализация текста --------------------
def _clean_text(s: str) -> str:
    # убираем лишние пробелы, двойные переносы, хвосты
    s = s.replace("\r", "")
    lines = [ln.strip() for ln in s.split("\n")]
    # фильтруем пустые в начале/конце, но сохраняем одинарные пустые внутри
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()
    # склеиваем, убираем двойные пустые
    out = []
    prev_empty = False
    for ln in lines:
        empty = (ln == "")
        if empty and prev_empty:
            continue
        out.append(ln)
        prev_empty = empty
    return "\n".join(out).strip()

def _strip_footer_block(text: str) -> str:
    # вырезает старые "Общий альбом/Покупка/вопросы", если уже есть
    low = text.lower()
    markers = ["общий альбом", "покупка/вопросы", "покупка / вопросы"]
    cut_index = None
    for m in markers:
        idx = low.rfind(m)
        if idx != -1:
            cut_index = idx if (cut_index is None or idx < cut_index) else cut_index
    if cut_index is not None:
        return text[:cut_index].rstrip()
    return text

def format_caption(raw: str) -> str:
    base = _clean_text(raw or "")
    base = _strip_footer_block(base)
    footer_lines = []
    if ALBUM_URL:
        footer_lines.append(f"Общий альбом: {ALBUM_URL}")
    if BUY_CONTACT:
        footer_lines.append(f"Покупка/вопросы: {BUY_CONTACT}")
    footer = "\n".join(footer_lines)
    if footer:
        return f"{base}\n\n{footer}"
    return base

# -------------------- Утилиты Telegram --------------------
def _src_tuple(m: Message) -> Optional[Tuple[int, int]]:
    # безопасно достаём источник при пересылке из канала
    try:
        if m.forward_from_chat and getattr(m.forward_from_chat, "type", None) == "channel":
            # aiogram 3 хранит type строкой
            return (m.forward_from_chat.id, m.forward_from_message_id)
    except Exception:
        pass
    return None

async def _notify_admins(text: str) -> None:
    for aid in ADMINS:
        try:
            await bot.send_message(aid, text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except Exception as e:
            log.warning(f"Админ {aid} недоступен: {e}")

async def _send_preview(row: sqlite3.Row) -> None:
    try:
        items = json.loads(row["items_json"])
        caption = row["caption"] or ""
        text = format_caption(caption)
        # превью: шлём только первому админу одну фотку с текстом, остальным — текст+счётчик
        if ADMINS:
            try:
                # если есть фото — приложим
                first_photo = None
                for it in items:
                    if it.get("type") == "photo":
                        first_photo = it.get("file_id")
                        break
                if first_photo:
                    await bot.send_photo(ADMINS[0], first_photo, caption=text, parse_mode=ParseMode.HTML)
                else:
                    await bot.send_message(ADMINS[0], text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            except Exception as e:
                log.warning(f"Превью админу {ADMINS[0]}: {e}")
            # остальным просто текст
            for aid in ADMINS[1:]:
                try:
                    await bot.send_message(aid, text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                except Exception as e:
                    log.warning(f"Превью админу {aid}: {e}")
    except Exception as e:
        log.warning(f"Не удалось сформировать превью: {e}")

async def _post_row(row: sqlite3.Row) -> None:
    items = json.loads(row["items_json"])
    caption = format_caption(row["caption"] or "")
    # отправка
    media = [it for it in items if it.get("type") == "photo"]
    sent = None
    if len(media) <= 1:
        if media:
            sent = await bot.send_photo(CHANNEL_ID, media[0]["file_id"], caption=caption, parse_mode=ParseMode.HTML)
        else:
            # на всякий — просто текст
            sent = await bot.send_message(CHANNEL_ID, caption, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    else:
        # альбом
        group = []
        for i, it in enumerate(media):
            if i == 0:
                group.append(InputMediaPhoto(type="photo", media=it["file_id"], caption=caption, parse_mode=ParseMode.HTML))
            else:
                group.append(InputMediaPhoto(type="photo", media=it["file_id"]))
        res = await bot.send_media_group(CHANNEL_ID, group)
        sent = res[0] if res else None

    # попробовать удалить исходный пост в канале
    if row["src_chat_id"] and row["src_msg_id"]:
        try:
            await bot.delete_message(row["src_chat_id"], row["src_msg_id"])
        except Exception as e:
            log_sched.warning(f"Не смог удалить старое сообщение {row['src_chat_id']}/{row['src_msg_id']}: {e}")

# -------------------- Сборка медиа/альбомов --------------------
# Буфер альбомов по media_group_id: { id: {"items":[{type,file_id},...], "caption":str, "src":(chat,msg)} }
_album_buffer: Dict[str, Dict[str, Any]] = {}

def _append_photo_item(items: List[Dict[str, Any]], m: Message) -> None:
    # берём максимальный размер фото
    if m.photo:
        fid = m.photo[-1].file_id
        items.append({"type": "photo", "file_id": fid})

# -------------------- Команды --------------------
@router.message(CommandStart())
async def cmd_start(m: Message):
    help_text = (
        "Привет! Я помогу с очередью постов.\n\n"
        "/add_post — пересылай мне пост из канала (фото/альбом + подпись), я добавлю в очередь\n"
        "/post_oldest — запостить самый старый из очереди вручную\n"
        "/queue — показать, сколько постов в очереди\n"
        "/help — краткая справка\n\n"
        "Важно: чтобы получать превью в ЛС за 45 минут до слота, сначала нажми здесь /start."
    )
    await m.answer(help_text, disable_web_page_preview=True)

@router.message(Command("help"))
async def cmd_help(m: Message):
    await cmd_start(m)

@router.message(Command("queue"))
async def cmd_queue(m: Message):
    await m.answer(f"В очереди: {get_count()}.")

@router.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    if m.from_user and m.from_user.id not in ADMINS:
        return
    row = dequeue_oldest()
    if not row:
        await m.answer("Очередь пустая.")
        return
    await _post_row(row)
    await m.answer("Опубликовано.")

@router.message(Command("add_post"))
async def cmd_add_post(m: Message):
    if m.from_user and m.from_user.id not in ADMINS:
        return
    # поддерживаем: пересланное из канала фото/альбом, либо сообщение с media_group_id
    src = _src_tuple(m)
    items: List[Dict[str, Any]] = []
    cap = (m.caption or m.text or "").strip()

    if m.media_group_id:
        key = str(m.media_group_id)
        buf = _album_buffer.get(key, {"items": [], "caption": "", "src": src})
        _append_photo_item(buf["items"], m)
        if cap and not buf["caption"]:
            buf["caption"] = cap
        _album_buffer[key] = buf
        # подождём 1 секунду, чтобы собрать весь альбом
        await asyncio.sleep(1.0)
        buf_final = _album_buffer.pop(key, None)
        if not buf_final or not buf_final["items"]:
            await m.answer("Не удалось собрать альбом.")
            return
        qid = enqueue(buf_final["items"], buf_final["caption"], buf_final["src"])
        await m.answer(f"Добавлено в очередь (альбом). ID={qid}. Сейчас в очереди: {get_count()}.")
        return

    # одиночное фото/сообщение
    _append_photo_item(items, m)
    if not items and not cap:
        await m.answer("Пришли пересланный пост из канала: фото/альбом + подпись.")
        return
    qid = enqueue(items, cap, src)
    await m.answer(f"Добавлено в очередь. ID={qid}. Сейчас в очереди: {get_count()}.")

# -------------------- Автослот + превью --------------------
def _now_tz():
    try:
        tz = pytz.timezone(TZ)
    except Exception:
        tz = pytz.timezone("Europe/Moscow")
    return datetime.now(tz)

def _today_slots() -> List[datetime]:
    now = _now_tz()
    tz = now.tzinfo
    return [now.replace(hour=h, minute=m, second=0, microsecond=0) for (h, m) in SLOTS]

def _next_slot_after(dt: datetime) -> datetime:
    # ближайший слот >= dt; если все прошли — завтра первый
    slots = _today_slots()
    for s in slots:
        if s >= dt:
            return s
    return (slots[0] + timedelta(days=1))

_last_preview_for: Optional[datetime] = None
_last_post_for: Optional[datetime] = None

async def scheduler_loop():
    log.info(f"Scheduler TZ={TZ}, times=" + ",".join([f"{h:02d}:{m:02d}" for h, m in SLOTS]) + f", preview_before={PREVIEW_BEFORE_MIN} min")
    await asyncio.sleep(2)

    global _last_preview_for, _last_post_for
    while True:
        try:
            now = _now_tz()
            slot = _next_slot_after(now - timedelta(minutes=PREVIEW_BEFORE_MIN))
            preview_time = slot - timedelta(minutes=PREVIEW_BEFORE_MIN)

            # превью
            if now >= preview_time and (_last_preview_for is None or preview_time > _last_preview_for):
                row = peek_oldest()
                if row:
                    await _send_preview(row)
                _last_preview_for = preview_time

            # публикация
            if now >= slot and (_last_post_for is None or slot > _last_post_for):
                row2 = dequeue_oldest()
                if row2:
                    await _post_row(row2)
                _last_post_for = slot

        except Exception as e:
            log_sched.error(f"Ошибка в планировщике: {e}")

        await asyncio.sleep(10)  # частота опроса

# -------------------- Точка входа --------------------
async def _run():
    init_db()
    # запускаем планировщик в фоне
    asyncio.create_task(scheduler_loop())
    log.info("Starting bot instance...")
    await dp.start_polling(bot)

# совместимость с runner.py (import run_bot)
run_bot = _run

if __name__ == "__main__":
    asyncio.run(_run())
