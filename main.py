import os
import json
import time
import pytz
import logging
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import (
    Message, InputMediaPhoto, InputFile, Chat, ChatType, ContentType
)
from aiogram.filters import Command, CommandStart
from aiogram.utils.media_group import MediaGroupBuilder
from aiogram.enums import ParseMode

# -----------------------------
# Конфиг из переменных окружения
# -----------------------------
TOKEN = os.getenv("TOKEN", "").strip()
if not TOKEN or ":" not in TOKEN:
    raise SystemExit("ENV TOKEN отсутствует или некорректный.")

# CHANNEL_ID строго как int (-100...)
CHANNEL_ID_ENV = os.getenv("CHANNEL_ID", "").strip()
try:
    CHANNEL_ID = int(CHANNEL_ID_ENV)
except Exception:
    raise SystemExit("ENV CHANNEL_ID должен быть числом вида -100xxxxxxxxxx (не @username).")

ADMINS_ENV = os.getenv("ADMINS", "").strip()
ADMINS = []
if ADMINS_ENV:
    for part in ADMINS_ENV.split(","):
        s = part.strip()
        if s.isdigit():
            ADMINS.append(int(s))

TZ = os.getenv("TZ", "Europe/Moscow").strip()
SLOTS_ENV = os.getenv("SLOTS", "12:00,16:00,20:00")
SLOT_STRINGS = [x.strip() for x in SLOTS_ENV.split(",") if x.strip()]
PREVIEW_MINUTES = int(os.getenv("PREVIEW_MINUTES", "45"))
AUTOPUBLISH = os.getenv("AUTOPUBLISH", "0") == "1"  # по умолчанию выкл, как ты просил

# Постоянные строки для конца поста (как ты просил, без эмодзи)
ALBUM_LINE = "Общий альбом: https://vk.com/market-222108341?screen=group&section=album_26"
CONTACT_LINE = "Покупка/вопросы: @layoutplacebuy"

# -----------------------------
# Логирование
# -----------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("layoutplace_bot")
sched_log = logging.getLogger("layoutplace_scheduler")

# -----------------------------
# Бот/диспетчер
# -----------------------------
bot = Bot(TOKEN)  # parse_mode НЕ включаем по умолчанию, чтобы не ломать русские угловые скобки и т.п.
dp = Dispatcher()
rt = Router()
dp.include_router(rt)

# -----------------------------
# SQLite и хранилище
# -----------------------------
DATA_DIR = Path("./data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "bot.db"

def _cx():
    cx = sqlite3.connect(DB_PATH)
    cx.row_factory = sqlite3.Row
    return cx

def init_db():
    cx = _cx()
    cur = cx.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS queue(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        items_json TEXT NOT NULL,
        caption TEXT NOT NULL,
        src_chat_id INTEGER,
        src_msg_id INTEGER,
        created_at INTEGER NOT NULL
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta(
        key TEXT PRIMARY KEY,
        val TEXT NOT NULL
    )
    """)
    cx.commit()
    cx.close()

def set_meta(key: str, val: str):
    cx = _cx()
    cur = cx.cursor()
    cur.execute("INSERT INTO meta(key,val) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET val=excluded.val", (key, val))
    cx.commit()
    cx.close()

def get_meta(key: str, default: str | None = None) -> str | None:
    cx = _cx()
    cur = cx.cursor()
    cur.execute("SELECT val FROM meta WHERE key=?", (key,))
    row = cur.fetchone()
    cx.close()
    return row["val"] if row else default

def enqueue(items: list[dict], caption: str, src: tuple[int, int] | None):
    src_chat_id, src_msg_id = (src if src else (None, None))
    cx = _cx()
    cur = cx.cursor()
    cur.execute("""
        INSERT INTO queue(items_json, caption, src_chat_id, src_msg_id, created_at)
        VALUES(?,?,?,?,?)
    """, (json.dumps(items, ensure_ascii=False), caption, src_chat_id, src_msg_id, int(time.time())))
    qid = cur.lastrowid
    cx.commit()
    cx.close()
    return qid

def dequeue_oldest():
    cx = _cx()
    cur = cx.cursor()
    cur.execute("SELECT * FROM queue ORDER BY id LIMIT 1")
    row = cur.fetchone()
    if not row:
        cx.close()
        return None
    cur.execute("DELETE FROM queue WHERE id=?", (row["id"],))
    cx.commit()
    cx.close()
    return {
        "id": row["id"],
        "items": json.loads(row["items_json"]),
        "caption": row["caption"],
        "src": (row["src_chat_id"], row["src_msg_id"]) if row["src_chat_id"] and row["src_msg_id"] else None
    }

def get_count() -> int:
    cx = _cx()
    cur = cx.cursor()
    cur.execute("SELECT COUNT(*) c FROM queue")
    c = cur.fetchone()["c"]
    cx.close()
    return int(c)

def list_queue(limit: int = 10) -> list[dict]:
    cx = _cx()
    cur = cx.cursor()
    cur.execute("SELECT * FROM queue ORDER BY id LIMIT ?", (limit,))
    rows = cur.fetchall()
    cx.close()
    out = []
    for r in rows:
        out.append({
            "id": r["id"],
            "items": json.loads(r["items_json"]),
            "caption": r["caption"],
            "src": (r["src_chat_id"], r["src_msg_id"]) if r["src_chat_id"] and r["src_msg_id"] else None
        })
    return out

# -----------------------------
# Утилиты времени
# -----------------------------
def _tz() -> pytz.BaseTzInfo:
    try:
        return pytz.timezone(TZ)
    except Exception:
        return pytz.timezone("Europe/Moscow")

def today_slots() -> list[datetime]:
    tz = _tz()
    now = datetime.now(tz)
    slots = []
    for s in SLOT_STRINGS:
        try:
            hh, mm = s.split(":")
            slot = tz.localize(datetime(now.year, now.month, now.day, int(hh), int(mm), 0))
            slots.append(slot)
        except Exception:
            continue
    return slots

def _slot_key(dt: datetime) -> str:
    # YYYYMMDD-HHMM
    return dt.strftime("%Y%m%d-%H%M")

# -----------------------------
# Нормализация текста
# -----------------------------
def _clean_line(s: str) -> str:
    return s.replace("—", "-").replace("–", "-").replace("  ", " ").strip()

def format_caption(raw: str) -> str:
    """
    Приводим к единому виду без эмодзи и с постоянными концовками.
    Ожидаемый формат на выходе:

    <Название/бренд>
    Состояние: X/10
    Размер: Y
    Цена — N ₽

    #хэштеги (если были строкой)
    Общий альбом: <...>
    Покупка/вопросы: <...>
    """
    if not raw:
        raw = ""
    lines = [l.strip() for l in raw.splitlines() if l.strip()]
    name = ""
    state = ""
    size = ""
    price = ""
    tags = []

    for ln in lines:
        l = _clean_line(ln)

        low = l.lower()
        if not name:
            name = l
            continue

        if low.startswith("состояние"):
            # "Состояние : 9/10"
            state = "Состояние: " + l.split(":", 1)[-1].strip().replace(" ", "")
            continue
        if low.startswith("размер"):
            size = "Размер: " + l.split(":", 1)[-1].strip()
            continue
        if low.startswith("цена"):
            # поддержим "Цена - 4 250 ₽"
            right = l.split(":", 1)[-1].strip() if ":" in l else l.split("-", 1)[-1].strip()
            price = f"Цена — {right}".replace("--", "—")
            continue
        if l.startswith("#"):
            tags.append(l)
            continue

    out_lines = []
    if name:
        out_lines.append(name)
    if state:
        out_lines.append(state)
    if size:
        out_lines.append(size)
    if price:
        out_lines.append(price)

    if tags:
        out_lines.append("")  # пустая строка перед тегами
        out_lines.extend(tags)

    # постоянный хвост
    if out_lines:
        out_lines.append("")
    out_lines.append(ALBUM_LINE)
    out_lines.append(CONTACT_LINE)
    return "\n".join(out_lines).strip()

# -----------------------------
# Сбор альбомов (media_group)
# -----------------------------
# Буферим входящие сообщения с одинаковым media_group_id, собираем за короткое окно.
ALBUM_BUFFER: dict[str, dict] = {}  # key = media_group_id, value={"items":[...], "caption": str, "src": tuple, "ts": time.time(), "owner": admin_id}

def _src_tuple(m: Message) -> tuple[int, int] | None:
    # если переслано из канала — сохраним для удаления
    ch = m.forward_from_chat
    if ch and (getattr(ch, "type", None) == "channel" or getattr(getattr(ch, "type", None), "value", "") == "channel"):
        return (ch.id, m.forward_from_message_id)
    return None

def _append_album_piece(m: Message, owner_id: int):
    mg_id = m.media_group_id
    if not mg_id:
        return False

    buf = ALBUM_BUFFER.get(mg_id)
    if not buf:
        buf = {"items": [], "caption": "", "src": None, "ts": time.time(), "owner": owner_id}
        ALBUM_BUFFER[mg_id] = buf

    if m.photo:
        fid = m.photo[-1].file_id
        buf["items"].append({"type": "photo", "file_id": fid})
    if (m.caption or "") and not buf["caption"]:
        buf["caption"] = m.caption
    if not buf["src"]:
        buf["src"] = _src_tuple(m)
    buf["ts"] = time.time()
    return True

def _flush_albums(timeout_sec: float = 2.5) -> list[int]:
    """Закрываем альбомы, если таймаут истёк. Возвращаем список новых qid"""
    now = time.time()
    made = []
    to_del = []
    for mg_id, buf in ALBUM_BUFFER.items():
        if now - buf["ts"] >= timeout_sec and buf["items"]:
            cap = format_caption(buf["caption"] or "")
            qid = enqueue(buf["items"], cap, buf["src"])
            made.append(qid)
            to_del.append(mg_id)
    for mg_id in to_del:
        ALBUM_BUFFER.pop(mg_id, None)
    return made

# -----------------------------
# Уведомления админам
# -----------------------------
async def _notify_admins(text: str):
    for aid in ADMINS:
        try:
            await bot.send_message(aid, text)
        except Exception as e:
            log.warning(f"Админ {aid} недоступен: {e}")

# -----------------------------
# Превью / Постинг
# -----------------------------
async def _send_preview_for_slot(slot: datetime):
    """шлём превью админам (один элемент из очереди, не вынимая его)"""
    cx = _cx()
    cur = cx.cursor()
    cur.execute("SELECT * FROM queue ORDER BY id LIMIT 1")
    row = cur.fetchone()
    cx.close()
    if not row:
        await _notify_admins(f"Превью {slot.strftime('%H:%M')}: очередь пуста.")
        return

    items = json.loads(row["items_json"])
    caption = row["caption"]
    head = f"Превью {slot.strftime('%H:%M')} (не публикуется автоматически):"
    for aid in ADMINS:
        try:
            if items and len(items) > 1:
                media = []
                for i, it in enumerate(items):
                    if it["type"] == "photo":
                        media.append(InputMediaPhoto(media=it["file_id"], caption=caption if i == 0 else None))
                await bot.send_message(aid, head)
                await bot.send_media_group(aid, media)
            else:
                if items and items[0]["type"] == "photo":
                    await bot.send_message(aid, head)
                    await bot.send_photo(aid, photo=items[0]["file_id"], caption=caption)
                else:
                    await bot.send_message(aid, head + "\n\n" + caption, disable_web_page_preview=True)
        except Exception as e:
            log.warning(f"Не смог отправить превью админу {aid}: {e}")

async def _post_task(task: dict) -> bool:
    """Публикация одного элемента очереди + попытка удалить старый оригинал"""
    caption = format_caption(task["caption"] or "")
    items = task["items"]
    try:
        if items and len(items) > 1:
            media = []
            for i, it in enumerate(items):
                if it["type"] == "photo":
                    media.append(InputMediaPhoto(media=it["file_id"], caption=caption if i == 0 else None))
            await bot.send_media_group(CHANNEL_ID, media)
        else:
            if items and items[0]["type"] == "photo":
                await bot.send_photo(CHANNEL_ID, photo=items[0]["file_id"], caption=caption)
            else:
                await bot.send_message(CHANNEL_ID, caption, disable_web_page_preview=True)

        if task["src"]:
            try:
                await bot.delete_message(task["src"][0], task["src"][1])
            except Exception as e:
                logging.warning(f"Не смог удалить старое сообщение {task['src'][0]}/{task['src'][1]}: {e}")
        return True
    except Exception as e:
        await _notify_admins(f"Публикация не удалась: {e}")
        return False

async def _auto_post_oldest():
    task = dequeue_oldest()
    if not task:
        await _notify_admins("Автопост: очередь пуста, публиковать нечего.")
        return False
    return await _post_task(task)

# -----------------------------
# Планировщик
# -----------------------------
async def scheduler_loop():
    tz = _tz()
    sched_log.info(f"Scheduler TZ={TZ}, times={','.join(SLOT_STRINGS)}, preview_before={PREVIEW_MINUTES} min")
    while True:
        try:
            now = datetime.now(tz)
            for slot in today_slots():
                key = _slot_key(slot)
                preview_key = f"preview::{key}"
                posted_key = f"posted::{key}"

                preview_at = slot - timedelta(minutes=PREVIEW_MINUTES)

                # Превью один раз в окне [-1м..+1м] вокруг времени превью
                if get_meta(preview_key, "0") != "1" and now >= preview_at and now < preview_at + timedelta(minutes=1):
                    await _send_preview_for_slot(slot)
                    set_meta(preview_key, "1")

                # Автопост если включён (по умолчанию выключено)
                if AUTOPUBLISH and get_meta(posted_key, "0") != "1" and now >= slot and now < slot + timedelta(minutes=2):
                    ok = await _auto_post_oldest()
                    if ok:
                        set_meta(posted_key, "1")

            # закрываем альбомы, которые успели собраться
            _flush_albums()

        except Exception as e:
            sched_log.warning(f"scheduler_loop error: {e}")
        finally:
            await asyncio.sleep(5)  # частота проверки
# -----------------------------
# Команды
# -----------------------------
@rt.message(CommandStart())
async def cmd_start(m: Message):
    is_admin = m.from_user and (m.from_user.id in ADMINS)
    text = [
        "Привет! Я бот-очередь для постинга в канал.",
        "",
        "Что я умею:",
        "• Перешли мне пост(ы) из канала — я добавлю в очередь. Поддерживаю альбомы.",
        "• Привожу текст к единому стилю и добавляю хвост:",
        f"  - {ALBUM_LINE}",
        f"  - {CONTACT_LINE}",
        "• За 45 минут до слота пришлю превью в ЛС админам.",
        "• Публикую ТОЛЬКО по команде /post_oldest (автопост выключен).",
        "",
        "Команды:",
        "• /queue — сколько в очереди",
        "• /post_oldest — запостить самый старый",
    ]
    if not is_admin:
        text.append("\nВ доступе на публикацию управляют администраторы.")
    await m.answer("\n".join(text), disable_web_page_preview=True)

@rt.message(Command("queue"))
async def cmd_queue(m: Message):
    if not (m.from_user and m.from_user.id in ADMINS):
        return
    await m.answer(f"В очереди: {get_count()}.")

@rt.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    if not (m.from_user and m.from_user.id in ADMINS):
        return
    task = dequeue_oldest()
    if not task:
        await m.answer("Очередь пуста.")
        return
    ok = await _post_task(task)
    await m.answer("Опубликовано." if ok else "Не удалось опубликовать (смотри лог/уведомление).")

# Приходящие сообщения от админов: собираем альбомы / одиночные
@rt.message(F.from_user & (F.from_user.id.in_(ADMINS)))
async def on_any_from_admin(m: Message):
    # Если это кусок альбома
    if m.media_group_id:
        _append_album_piece(m, m.from_user.id)
        # ничего не отвечаем — альбом закроется сам через таймаут
        return

    # Одиночное фото или текст:
    items = []
    if m.photo:
        fid = m.photo[-1].file_id
        items.append({"type": "photo", "file_id": fid})

    # Поддержим пересланный пост без фото (только текст) — публиковать как текст
    if not items and not (m.text or m.caption):
        # ничего полезного
        return

    raw_caption = m.caption or m.text or ""
    caption = format_caption(raw_caption)
    src = _src_tuple(m)
    qid = enqueue(items, caption, src)
    await m.answer(f"Добавил в очередь (id={qid}). Сейчас в очереди: {get_count()}.")

# -----------------------------
# Запуск
# -----------------------------
import asyncio

async def _run():
    init_db()
    # Запускаем планировщик параллельно с поллингом
    asyncio.create_task(scheduler_loop())
    log.info("Starting bot instance...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(_run())
