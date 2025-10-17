# main.py
import os, asyncio, logging, time, json, sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict, Optional, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import Message, InputMediaPhoto
from aiogram.filters import Command

# ─────────── Настройки из ENV ───────────
def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default)

TOKEN = _env("BOT_TOKEN").strip()
CHANNEL_ID = _env("CHANNEL_ID").strip()           # -100… или @username
TZ = _env("TZ", "Europe/Moscow").strip()

ADMINS = [int(x) for x in _env("ADMINS", "").split(",") if x.strip()]
POST_TIMES = [t.strip() for t in _env("POST_TIMES", "12:00,16:00,20:00").split(",") if t.strip()]
PREVIEW_BEFORE_MIN = int(_env("PREVIEW_BEFORE_MIN", "45"))

ALBUM_URL = _env("ALBUM_URL", "https://vk.com/market-222108341?screen=group&section=album_26").strip()
CONTACT_TEXT = _env("CONTACT_TEXT", "@layoutplacebuy").strip()

DB_PATH = _env("DB_PATH", "/data/data.db").strip()
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

# ─────────── Логирование ───────────
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("layoutplace_bot")

# ─────────── Бот/диспетчер ───────────
bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ─────────── БД (встроено, без отдельных модулей) ───────────
DESIRED_QUEUE_COLS = ["id", "items_json", "caption", "src_chat_id", "src_msg_id", "created_at"]
def _connect():
    cx = sqlite3.connect(DB_PATH, check_same_thread=False)
    cx.row_factory = sqlite3.Row
    return cx

def _migrate():
    cx = _connect()
    cur = cx.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)")
    # Проверим существующую схему queue
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='queue'")
    if cur.fetchone() is None:
        cur.execute("""
            CREATE TABLE queue(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                items_json TEXT NOT NULL,
                caption TEXT,
                src_chat_id INTEGER,
                src_msg_id INTEGER,
                created_at INTEGER NOT NULL
            )
        """)
        cx.commit()
        cx.close()
        return

    # есть таблица queue — проверим колонки
    cur.execute("PRAGMA table_info(queue)")
    cols = [r["name"] for r in cur.fetchall()]
    if cols != DESIRED_QUEUE_COLS:
        # создадим новую таблицу и аккуратно перенесём данные
        cur.execute("""
            ALTER TABLE queue RENAME TO queue_old_bak
        """)
        cur.execute("""
            CREATE TABLE queue(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                items_json TEXT NOT NULL,
                caption TEXT,
                src_chat_id INTEGER,
                src_msg_id INTEGER,
                created_at INTEGER NOT NULL
            )
        """)
        # попытаемся перенести, если возможно
        try:
            cur.execute("""
                INSERT INTO queue(items_json, caption, src_chat_id, src_msg_id, created_at)
                SELECT
                    COALESCE(items_json,
                        CASE
                            WHEN payload IS NOT NULL THEN payload
                            ELSE '[]'
                        END
                    ) as items_json,
                    caption,
                    src_chat_id,
                    src_msg_id,
                    COALESCE(created_at, strftime('%s','now')) as created_at
                FROM queue_old_bak
            """)
        except Exception as e:
            log.warning(f"Миграция без переноса (ок): {e}")
        cx.commit()
        cur.execute("DROP TABLE IF EXISTS queue_old_bak")
        cx.commit()
    cx.close()

def init_db():
    _migrate()

def enqueue(*, items: List[Dict], caption: str, src: Optional[Tuple[int,int]]):
    cx = _connect()
    cur = cx.cursor()
    src_chat_id = src[0] if src else None
    src_msg_id = src[1] if src else None
    cur.execute("""
        INSERT INTO queue(items_json, caption, src_chat_id, src_msg_id, created_at)
        VALUES(?,?,?,?,?)
    """, (json.dumps(items, ensure_ascii=False), caption, src_chat_id, src_msg_id, int(time.time())))
    cx.commit()
    qid = cur.lastrowid
    cx.close()
    return qid

def dequeue_oldest():
    cx = _connect()
    cur = cx.cursor()
    cur.execute("SELECT * FROM queue ORDER BY id LIMIT 1")
    row = cur.fetchone()
    if not row:
        cx.close()
        return None
    cur.execute("DELETE FROM queue WHERE id = ?", (row["id"],))
    cx.commit()
    cx.close()
    items = json.loads(row["items_json"]) if row["items_json"] else []
    src = (row["src_chat_id"], row["src_msg_id"]) if row["src_chat_id"] and row["src_msg_id"] else None
    return {"items": items, "caption": row["caption"] or "", "src": src}

def peek_oldest():
    cx = _connect()
    cur = cx.cursor()
    cur.execute("SELECT * FROM queue ORDER BY id LIMIT 1")
    row = cur.fetchone()
    cx.close()
    if not row:
        return None
    items = json.loads(row["items_json"]) if row["items_json"] else []
    src = (row["src_chat_id"], row["src_msg_id"]) if row["src_chat_id"] and row["src_msg_id"] else None
    return {"items": items, "caption": row["caption"] or "", "src": src}

def get_count():
    cx = _connect()
    cur = cx.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM queue")
    c = cur.fetchone()["c"]
    cx.close()
    return c

def meta_get(k: str) -> Optional[str]:
    cx = _connect()
    cur = cx.cursor()
    cur.execute("SELECT v FROM meta WHERE k = ?", (k,))
    row = cur.fetchone()
    cx.close()
    return row["v"] if row else None

def meta_set(k: str, v: str):
    cx = _connect()
    cur = cx.cursor()
    cur.execute("INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v))
    cx.commit()
    cx.close()

# ─────────── Нормализация текста (единый стиль) ───────────
def normalize_caption(text: str) -> str:
    """
    Приводим к единому стилю без эмодзи.
    В конец добавляем:
      Общий альбом: <ALBUM_URL>
      Покупка/вопросы: <CONTACT_TEXT>
    """
    def strip_emojis(s: str) -> str:
        return "".join(ch for ch in s if ch.isascii() or (31 < ord(ch) < 127) or ch.isalnum() or ch.isspace() or ch in ".,:;!?/()-+@&_\"'—–%₽€$")

    t = (text or "").replace("\r", "").strip()
    t = strip_emojis(t)

    # Убедимся, что цена выделена (просто нормализация дефиса)
    t = t.replace(" - ", " — ")
    # Убираем двойные пустые строки
    lines = [ln.strip() for ln in t.split("\n")]
    compact = []
    for ln in lines:
        if ln:
            compact.append(ln)
        elif compact and compact[-1] != "":
            compact.append("")
    t = "\n".join(compact).strip()

    tail = (
        f"\n\nОбщий альбом: {ALBUM_URL}\n"
        f"Покупка/вопросы: {CONTACT_TEXT}"
    )
    # Не дублировать хвост, если уже вставлен
    if ALBUM_URL in t or CONTACT_TEXT in t:
        return t
    return f"{t}{tail}"

# ─────────── Альбом-кэш (media_group) ───────────
ALBUM_CACHE: dict[tuple[int, str], list[str]] = {}
ALBUM_LAST_SEEN: dict[tuple[int, str], float] = {}
ALBUM_TTL_SEC = 15 * 60

async def _album_gc_loop():
    while True:
        now = time.time()
        for key in list(ALBUM_LAST_SEEN.keys()):
            if now - ALBUM_LAST_SEEN[key] > ALBUM_TTL_SEC:
                ALBUM_LAST_SEEN.pop(key, None)
                ALBUM_CACHE.pop(key, None)
        await asyncio.sleep(30)

@dp.message(F.media_group_id & (F.photo | F.document))
async def on_album_piece(m: Message):
    key = (m.chat.id, m.media_group_id)
    ALBUM_CACHE.setdefault(key, [])
    if m.photo:
        fid = m.photo[-1].file_id
        if fid not in ALBUM_CACHE[key]:
            ALBUM_CACHE[key].append(fid)
    elif m.document and m.document.mime_type and m.document.mime_type.startswith("image/"):
        fid = m.document.file_id
        if fid not in ALBUM_CACHE[key]:
            ALBUM_CACHE[key].append(fid)
    ALBUM_LAST_SEEN[key] = time.time()

# ─────────── Helpers ───────────
def tznow() -> datetime:
    return datetime.now(ZoneInfo(TZ))

def _today_slots() -> List[datetime]:
    base = tznow().date()
    out = []
    for t in POST_TIMES:
        hh, mm = [int(x) for x in t.split(":")]
        out.append(datetime(base.year, base.month, base.day, hh, mm, tzinfo=ZoneInfo(TZ)))
    return out

async def _notify_admins(text: str):
    for uid in ADMINS:
        try:
            await bot.send_message(uid, text, disable_web_page_preview=True)
        except Exception as e:
            log.warning(f"Админ {uid} недоступен: {e}")

async def _send_to_channel(items: List[Dict], caption: str) -> int:
    if len(items) > 1:
        media = []
        for i, it in enumerate(items):
            cap = caption if i == 0 else ""
            media.append(InputMediaPhoto(media=it["file_id"], caption=cap))
        msgs = await bot.send_media_group(CHANNEL_ID, media)
        return msgs[0].message_id
    else:
        it = items[0]
        msg = await bot.send_photo(CHANNEL_ID, it["file_id"], caption=caption)
        return msg.message_id

def _items_from_message_or_album(msg: Message) -> List[Dict]:
    m = msg.reply_to_message or msg
    if m.media_group_id:
        key = (msg.chat.id, m.media_group_id)
        fids = ALBUM_CACHE.get(key, [])
        if fids:
            return [{"type": "photo", "file_id": fid} for fid in fids]
    if m.photo:
        return [{"type": "photo", "file_id": m.photo[-1].file_id}]
    if m.document and m.document.mime_type and m.document.mime_type.startswith("image/"):
        return [{"type": "photo", "file_id": m.document.file_id}]
    return []

def _src_tuple(msg: Message) -> Optional[Tuple[int,int]]:
    m = msg.reply_to_message
    if not m:
        return None
    if m.forward_from_chat and (m.forward_from_chat.type.value == "channel"):
        return (m.forward_from_chat.id, m.forward_from_message_id)
    if m.chat and m.chat.type.value == "channel":
        return (m.chat.id, m.message_id)
    return None

# ─────────── Команды ───────────
@dp.message(Command("start"))
async def cmd_start(m: Message):
    text = (
        "Бот готов.\n\n"
        "/myid — твой ID\n"
        "/add_post — ответом на пересланный пост (фото/альбом)\n"
        "/queue — размер очереди\n"
        "/post_oldest — опубликовать старый пост вручную\n"
        "/clear_queue — очистить очередь\n"
        "/test_preview — отправить тест-превью админам\n"
        "/now — текущее время"
    )
    await m.answer(text, disable_web_page_preview=True)

@dp.message(Command("myid"))
async def cmd_myid(m: Message):
    await m.answer(f"Твой ID: <code>{m.from_user.id}</code>")

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    await m.answer(f"В очереди: {get_count()}.")

@dp.message(Command("clear_queue"))
async def cmd_clear(m: Message):
    removed = 0
    while dequeue_oldest():
        removed += 1
    await m.answer(f"Очищено: {removed}.")

@dp.message(Command("add_post"))
async def cmd_add_post(m: Message):
    if not (m.reply_to_message or m.photo or m.document):
        await m.answer("Сделай /add_post ответом на пересланное из канала сообщение (фото/альбом).")
        return
    items = _items_from_message_or_album(m)
    if not items:
        await m.answer("Не нашёл фото. Пришли пересланный пост с фото/альбомом.")
        return
    src_msg = m.reply_to_message or m
    raw_caption = (src_msg.caption or "").strip()
    caption = normalize_caption(raw_caption)
    qid = enqueue(items=items, caption=caption, src=_src_tuple(m))
    await m.answer(f"Добавлено в очередь (id={qid}). В очереди: {get_count()}.")

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    task = dequeue_oldest()
    if not task:
        await m.answer("Очередь пуста.")
        return
    await _send_to_channel(task["items"], task["caption"])
    if task["src"]:
        try:
            await bot.delete_message(task["src"][0], task["src"][1])
        except Exception as e:
            log.warning(f"Не смог удалить старое сообщение {task['src'][0]}/{task['src'][1]}: {e}")
    await m.answer(f"Опубликовано. Осталось: {get_count()}.")

@dp.message(Command("test_preview"))
async def cmd_test_preview(m: Message):
    await _notify_admins("Тестовое превью — пост будет за 45 минут до слота.\n(Если ты это видишь, то всё ок)")
    await m.answer("Ок, превью отправлено админам (если они нажали /start боту).")

@dp.message(Command("now"))
async def cmd_now(m: Message):
    await m.answer(str(tznow()))

# ─────────── Планировщик ───────────
async def _catch_up_if_needed():
    now = tznow()
    last_key = "last_slot_ts"
    last_ts = int(meta_get(last_key) or "0")
    last_dt = datetime.fromtimestamp(last_ts, tz=ZoneInfo(TZ)) if last_ts else None
    for slot in _today_slots():
        if slot <= now and (not last_dt or slot > last_dt):
            task = dequeue_oldest()
            if not task:
                break
            await _send_to_channel(task["items"], task["caption"])
            if task["src"]:
                try:
                    await bot.delete_message(task["src"][0], task["src"][1])
                except Exception as e:
                    log.warning(f"Catch-up delete failed: {e}")
            meta_set(last_key, str(int(slot.timestamp())))
            log.info(f"Catch-up: опубликован {slot.isoformat()}")

async def run_scheduler():
    log.info(f"Scheduler TZ={TZ}, times={','.join(POST_TIMES)}, preview_before={PREVIEW_BEFORE_MIN} min")
    await _catch_up_if_needed()
    last_key = "last_slot_ts"
    preview_key = "preview_for_ts"
    while True:
        now = tznow()
        # превью
        for slot in _today_slots():
            preview_at = slot - timedelta(minutes=PREVIEW_BEFORE_MIN)
            if preview_at <= now < slot:
                if meta_get(preview_key) != str(int(slot.timestamp())):
                    peek = peek_oldest()
                    if peek:
                        eta = max(0, (slot - now).seconds // 60)
                        await _notify_admins(f"Превью: следующий пост через {eta} мин.\n\n{peek['caption'][:800]}")
                    meta_set(preview_key, str(int(slot.timestamp())))
        # публикация
        for slot in _today_slots():
            last_ts = int(meta_get(last_key) or "0")
            last_dt = datetime.fromtimestamp(last_ts, tz=ZoneInfo(TZ)) if last_ts else None
            if slot <= now and (not last_dt or slot > last_dt):
                task = dequeue_oldest()
                if task:
                    await _send_to_channel(task["items"], task["caption"])
                    if task["src"]:
                        try:
                            await bot.delete_message(task["src"][0], task["src"][1])
                        except Exception as e:
                            log.warning(f"Delete source failed: {e}")
                meta_set(last_key, str(int(slot.timestamp())))
                log.info(f"Posted for slot {slot.isoformat()}")
        await asyncio.sleep(20)

# ─────────── Точка входа ───────────
async def _run():
    if not TOKEN:
        raise SystemExit("BOT_TOKEN is empty. Set it in Railway Variables.")
    init_db()
    asyncio.create_task(_album_gc_loop())
    asyncio.create_task(run_scheduler())
    await dp.start_polling(bot)

if __name__ == "__main__":
    log.info("Starting bot instance...")
    asyncio.run(_run())
