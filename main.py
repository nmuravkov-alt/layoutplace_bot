# main.py
import os
import json
import time
import pytz
import sqlite3
import asyncio
import logging
from typing import List, Optional, Dict, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.types import (
    Message,
    InputMediaPhoto,
    InputMediaDocument,
)

# =========================
# ENV / Конфигурация
# =========================
TOKEN = os.getenv("TOKEN", "").strip()
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("ENV TOKEN пуст или имеет неверный формат. Задайте корректный токен бота.")

CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
if CHANNEL_ID == 0:
    raise RuntimeError("ENV CHANNEL_ID пуст. Пример: -1001758490510")

TZ = os.getenv("TZ", "Europe/Moscow")
POST_TIMES = [s.strip() for s in os.getenv("POST_TIMES", "12:00,16:00,20:00").split(",") if s.strip()]
PREVIEW_BEFORE_MIN = int(os.getenv("PREVIEW_BEFORE_MIN", "45"))

ADMINS = []
_adm = os.getenv("ADMINS", "").replace(" ", "")
if _adm:
    for part in _adm.split(","):
        if part.strip().isdigit():
            ADMINS.append(int(part.strip()))
ADMINS = list(set(ADMINS))

ALBUM_URL = os.getenv("ALBUM_URL", "").strip()
CONTACT = os.getenv("CONTACT", "").strip()
DB_PATH = os.getenv("DB_PATH", "/data/data.db")

# =========================
# Логирование
# =========================
logger = logging.getLogger("layoutplace_bot")
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s",
)

# =========================
# Бот и диспетчер
# =========================
bot = Bot(TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

# =========================
# Хранилище / SQLite
# =========================
def _connect():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    cx = _connect()
    cur = cx.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        items_json TEXT NOT NULL,
        caption TEXT,
        src_chat_id INTEGER,
        src_msg_id INTEGER,
        created_at INTEGER NOT NULL
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stats (
        k TEXT PRIMARY KEY,
        v INTEGER NOT NULL
    );
    """)
    # счётчики
    cur.execute("INSERT OR IGNORE INTO stats(k,v) VALUES('posted',0)")
    cur.execute("INSERT OR IGNORE INTO stats(k,v) VALUES('errors',0)")
    cx.commit()
    cx.close()

def enqueue(items: List[dict], caption: Optional[str], src: Optional[Tuple[int,int]]) -> int:
    cx = _connect()
    cur = cx.cursor()
    src_chat_id = src[0] if src else None
    src_msg_id = src[1] if src else None
    cur.execute("""
        INSERT INTO queue(items_json, caption, src_chat_id, src_msg_id, created_at)
        VALUES(?,?,?,?,?)
    """, (json.dumps(items), caption, src_chat_id, src_msg_id, int(time.time())))
    qid = cur.lastrowid
    cx.commit()
    cx.close()
    return qid

def dequeue_oldest():
    cx = _connect()
    cur = cx.cursor()
    cur.execute("SELECT id, items_json, caption, src_chat_id, src_msg_id FROM queue ORDER BY id LIMIT 1")
    row = cur.fetchone()
    if not row:
        cx.close()
        return None
    qid, items_json, caption, src_chat_id, src_msg_id = row
    cur.execute("DELETE FROM queue WHERE id=?", (qid,))
    cx.commit()
    cx.close()
    items = json.loads(items_json)
    src = (src_chat_id, src_msg_id) if (src_chat_id and src_msg_id) else None
    return {"id": qid, "items": items, "caption": caption, "src": src}

def queue_list():
    cx = _connect()
    cur = cx.cursor()
    cur.execute("SELECT id, created_at FROM queue ORDER BY id")
    rows = cur.fetchall()
    cx.close()
    return rows

def stat_inc(key: str):
    cx = _connect()
    cur = cx.cursor()
    cur.execute("UPDATE stats SET v=v+1 WHERE k=?", (key,))
    cx.commit()
    cx.close()

# =========================
# Утилиты форматирования
# =========================
FOOTER_TEMPLATE = (
    "\n\n"
    "{hashtags}"
    "{album_line}"
    "{contact_line}"
)

def build_final_caption(text: Optional[str]) -> str:
    text = (text or "").strip()

    # Нормализуем дешёвые тире/множественные пробелы
    text = text.replace(" —", " —").replace("–", "—")
    text = "\n".join([ln.rstrip() for ln in text.splitlines()])

    # Хвост — добавляем только если его ещё нет
    hashtags = ""
    album_line = ""
    contact_line = ""

    if ALBUM_URL and (ALBUM_URL not in text):
        album_line = f"\nОбщий альбом: {ALBUM_URL}"
    if CONTACT and (CONTACT not in text):
        contact_line = f"\nПокупка/вопросы: {CONTACT}"

    # Подберём хэштеги из первых строк (если есть)
    # Если хочешь фиксированный, можно задать здесь:
    # hashtags = "\n#толстовки"   # пример
    # Оставим пустым по умолчанию
    if hashtags and (hashtags.strip() not in text):
        hashtags = "\n" + hashtags.strip()

    out = text + FOOTER_TEMPLATE.format(
        hashtags=hashtags,
        album_line=album_line,
        contact_line=contact_line,
    )
    return out.strip()

# =========================
# Формирование медиа
# =========================
def _extract_media_item(m: Message) -> Optional[dict]:
    if m.photo:
        return {"kind": "photo", "file_id": m.photo[-1].file_id}
    if m.document:
        return {
            "kind": "doc",
            "file_id": m.document.file_id,
            "file_name": m.document.file_name or None
        }
    return None

def _build_media_group(items: List[dict], caption: Optional[str]) -> List:
    media = []
    for i, it in enumerate(items):
        cap = caption if i == 0 else None
        if it["kind"] == "photo":
            media.append(InputMediaPhoto(media=it["file_id"], caption=cap))
        else:
            media.append(InputMediaDocument(media=it["file_id"], caption=cap))
    return media

# =========================
# Публикация поста
# =========================
async def publish_one(post: Dict):
    """post = {'id', 'items', 'caption', 'src'}"""
    items: List[dict] = post["items"] or []
    caption: str = post.get("caption") or ""
    src: Optional[Tuple[int,int]] = post.get("src")

    # Удалим старый источник из канала, если это был форвард оттуда
    if src and src[0] == CHANNEL_ID:
        try:
            await bot.delete_message(chat_id=src[0], message_id=src[1])
        except Exception as e:
            logger.warning(f"Не смог удалить старое сообщение {src[0]}/{src[1]}: {e}")

    try:
        if len(items) >= 2:
            media = _build_media_group(items, caption)
            await bot.send_media_group(chat_id=CHANNEL_ID, media=media)
        elif len(items) == 1:
            it = items[0]
            if it["kind"] == "photo":
                await bot.send_photo(chat_id=CHANNEL_ID, photo=it["file_id"], caption=caption)
            else:
                await bot.send_document(chat_id=CHANNEL_ID, document=it["file_id"], caption=caption)
        else:
            # Текстовый пост
            await bot.send_message(chat_id=CHANNEL_ID, text=caption)

        stat_inc("posted")
    except Exception as e:
        logger.exception(f"Ошибка публикации: {e}")
        stat_inc("errors")

# =========================
# Альбом-буфер (склейка по media_group_id)
# =========================
class AlbumBuffer:
    def __init__(self, timeout: float = 1.5):
        self.timeout = timeout
        self._store: Dict[str, Dict] = {}

    def _ensure(self, mgid: str):
        if mgid not in self._store:
            self._store[mgid] = {"items": [], "last": time.monotonic(), "task": None, "caption": None, "src": None}

    def add(self, mgid: str, item: dict, caption: Optional[str], src: Optional[Tuple[int,int]]):
        self._ensure(mgid)
        bucket = self._store[mgid]
        bucket["items"].append(item)
        bucket["last"] = time.monotonic()
        if bucket["caption"] is None and caption:
            bucket["caption"] = caption
        if bucket["src"] is None and src:
            bucket["src"] = src

    def start_timer(self, mgid: str, finalize_cb):
        self._ensure(mgid)
        bucket = self._store[mgid]
        if bucket["task"] and not bucket["task"].done():
            return

        async def waiter():
            last_seen = bucket["last"]
            while True:
                await asyncio.sleep(self.timeout)
                if last_seen == bucket["last"]:
                    break
                last_seen = bucket["last"]
            items = bucket["items"]
            cap = bucket["caption"]
            src = bucket["src"]
            del self._store[mgid]
            await finalize_cb(items, cap, src)

        bucket["task"] = asyncio.create_task(waiter())

album_buffer = AlbumBuffer(timeout=1.5)

async def _finalize_album_and_enqueue(items: List[dict], caption: Optional[str], src: Optional[Tuple[int,int]], reply_to: Optional[Message] = None):
    norm_caption = build_final_caption(caption)
    qid = enqueue(items=items, caption=norm_caption, src=src)
    if reply_to:
        await reply_to.answer(f"✅ Альбом добавлен в очередь как пост #{qid}.")

# =========================
# Команды админа
# =========================
@dp.message(Command("help"))
async def cmd_help(m: Message):
    await m.answer(
        "<b>Команды:</b>\n"
        "/queue — показать очередь\n"
        "/post_oldest — опубликовать самый старый пост прямо сейчас\n\n"
        "Просто перешли из канала сообщение с фото/альбомом и подписью — бот добавит в очередь.\n"
        "Текст будет приведён к единой форме и дополнен ссылками.",
        disable_web_page_preview=True
    )

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    if m.from_user.id not in ADMINS:
        return
    rows = queue_list()
    if not rows:
        await m.answer("Очередь пуста.")
        return
    lines = [f"Всего: {len(rows)}"]
    for qid, ts in rows:
        lines.append(f"#{qid} [queued] {time.strftime('%d.%m %H:%M', time.localtime(ts))}")
    await m.answer("\n".join(lines))

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    if m.from_user.id not in ADMINS:
        return
    post = dequeue_oldest()
    if not post:
        await m.answer("Очередь пуста.")
        return
    await publish_one(post)
    await m.answer(f"✅ Опубликован пост #{post['id']}")

# =========================
# Приём контента от админов
# =========================
@dp.message(F.from_user.id.in_(ADMINS) & (F.photo | F.document))
async def handle_media(m: Message):
    # источник, если это форвард из канала
    src: Optional[Tuple[int,int]] = None
    if m.forward_from_chat and getattr(m.forward_from_chat, "type", None) == "channel":
        src = (m.forward_from_chat.id, m.forward_from_message_id)

    item = _extract_media_item(m)
    if not item:
        return

    caption_raw = (m.caption or "").strip()

    # Альбом
    if m.media_group_id:
        mgid = str(m.media_group_id)
        album_buffer.add(mgid, item, caption_raw or None, src)
        # по таймеру склеим и поставим в очередь
        album_buffer.start_timer(
            mgid,
            lambda items, cap, s: _finalize_album_and_enqueue(items, cap, s, reply_to=m)
        )
        return

    # Одиночное медиа
    norm_caption = build_final_caption(caption_raw)
    qid = enqueue(items=[item], caption=norm_caption, src=src)
    await m.answer(f"✅ Пост #{qid} добавлен в очередь и будет опубликован автоматически.")

@dp.message(F.from_user.id.in_(ADMINS) & F.text)
async def handle_text(m: Message):
    text = (m.text or "").strip()
    if not text:
        return
    norm_caption = build_final_caption(text)
    qid = enqueue(items=[], caption=norm_caption, src=None)
    await m.answer(f"✅ Текстовый пост #{qid} добавлен в очередь.")

# =========================
# Планировщик
# =========================
def _now_in_tz():
    return pytz.timezone(TZ).fromutc(pytz.utc.localize(datetime.utcnow())).replace(tzinfo=None)

from datetime import datetime, timedelta

def _today_times_local() -> List[datetime]:
    today = datetime.now(pytz.timezone(TZ)).date()
    out = []
    for t in POST_TIMES:
        try:
            hh, mm = map(int, t.split(":"))
            dt = datetime(today.year, today.month, today.day, hh, mm, tzinfo=pytz.timezone(TZ))
            out.append(dt)
        except Exception:
            continue
    return out

async def _send_preview():
    rows = queue_list()
    if not rows:
        return
    qid, ts = rows[0]
    msg = f"⏳ Предпросмотр: в ближайшее время запланирована публикация поста #{qid}.\n" \
          f"Остаток в очереди: {len(rows)}"
    for uid in ADMINS:
        try:
            await bot.send_message(uid, msg)
        except Exception:
            pass

async def scheduler_loop():
    logger.info("Scheduler запущен.")
    last_preview_for: Dict[str, str] = {}  # ключ = 'YYYYMMDD HH:MM'

    while True:
        try:
            tz = pytz.timezone(TZ)
            now = datetime.now(tz)

            # Список сегодняшних точек
            slots = _today_times_local()
            for dt in slots:
                # превью
                pv_key = dt.strftime("%Y%m%d %H:%M")
                if dt - now <= timedelta(minutes=PREVIEW_BEFORE_MIN) and dt > now:
                    if pv_key not in last_preview_for:
                        await _send_preview()
                        last_preview_for[pv_key] = "sent"

                # публикация в сам момент
                if abs((dt - now).total_seconds()) < 30:  # окно 30 сек
                    post = dequeue_oldest()
                    if post:
                        await publish_one(post)

        except Exception as e:
            logger.exception(f"Ошибка планировщика: {e}")

        await asyncio.sleep(15)

# =========================
# Старт
# =========================
async def on_startup():
    logger.info("🚀 Стартуем Layoutplace Bot...")
    init_db()
    asyncio.create_task(scheduler_loop())
    logger.info(f"Scheduler TZ={TZ}, times={POST_TIMES}, preview_before={PREVIEW_BEFORE_MIN} мин")

async def main():
    await on_startup()
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
