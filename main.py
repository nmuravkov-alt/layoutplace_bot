import asyncio
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.markdown import hcode
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from contextlib import closing

# ---------- ENV ----------
TOKEN = os.getenv("TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
ADMINS = [int(x) for x in os.getenv("ADMINS", "").split(",") if x]
ALBUM_URL = os.getenv("ALBUM_URL")
CONTACT = os.getenv("CONTACT")
POST_TIMES = os.getenv("POST_TIMES", "12:00,16:00,20:00").split(",")
PREVIEW_BEFORE_MIN = int(os.getenv("PREVIEW_BEFORE_MIN", "45"))
TZ = os.getenv("TZ", "Europe/Moscow")

# ---------- LOGGING ----------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("layoutplace_bot")

# ---------- BOT ----------
bot = Bot(token=TOKEN, parse_mode="HTML")
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone=TZ)

# ---------- DATABASE ----------
DB_PATH = "data.db"
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

def db_connect():
    cx = sqlite3.connect(DB_PATH)
    cx.row_factory = sqlite3.Row
    return cx

def init_db():
    with db_connect() as db:
        db.execute("""
        CREATE TABLE IF NOT EXISTS queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL,
            caption TEXT,
            src_chat_id INTEGER,
            src_msg_id INTEGER,
            created_at TEXT NOT NULL
        );
        """)
        db.commit()

init_db()

# ---------- HELPERS ----------
async def send_album(channel_id, media_group, caption=None):
    try:
        await bot.send_media_group(channel_id, media_group)
        if caption:
            await bot.send_message(channel_id, caption)
    except Exception as e:
        log.error(f"send_album error: {e}")

def add_to_queue(payload, caption, src_chat_id, src_msg_id):
    with db_connect() as db:
        db.execute(
            "INSERT INTO queue (payload, caption, src_chat_id, src_msg_id, created_at) VALUES (?, ?, ?, ?, ?)",
            (payload, caption, src_chat_id, src_msg_id, datetime.now().isoformat()),
        )
        db.commit()

def get_oldest():
    with db_connect() as db:
        row = db.execute("SELECT * FROM queue ORDER BY id ASC LIMIT 1").fetchone()
        return dict(row) if row else None

def delete_post(post_id):
    with db_connect() as db:
        db.execute("DELETE FROM queue WHERE id=?", (post_id,))
        db.commit()

def get_queue():
    with db_connect() as db:
        return db.execute("SELECT * FROM queue ORDER BY id ASC").fetchall()

# ---------- COMMANDS ----------
@dp.message(Command("add_post"))
async def cmd_add_post(m: types.Message):
    await m.answer("📸 Просто перешли сюда пост или альбом — он будет добавлен в очередь.")

@dp.message(F.media_group_id)
async def handle_album(m: types.Message, album: list[types.Message]):
    admin_id = m.from_user.id
    if admin_id not in ADMINS:
        return

    caption = album[-1].caption or ""
    media = []
    for msg in album:
        if msg.photo:
            media.append({"type": "photo", "file_id": msg.photo[-1].file_id})
        elif msg.video:
            media.append({"type": "video", "file_id": msg.video.file_id})

    add_to_queue(str(media), caption, m.chat.id, m.message_id)
    await m.answer(f"✅ Альбом добавлен в очередь.\nВсего сейчас: {len(get_queue())}.")

@dp.message(F.photo | F.video)
async def handle_single_media(m: types.Message):
    admin_id = m.from_user.id
    if admin_id not in ADMINS:
        return

    media_type = "photo" if m.photo else "video"
    file_id = m.photo[-1].file_id if m.photo else m.video.file_id
    caption = m.caption or ""

    add_to_queue(str([{"type": media_type, "file_id": file_id}]), caption, m.chat.id, m.message_id)
    await m.answer(f"✅ Пост добавлен в очередь.\nВсего сейчас: {len(get_queue())}.")

@dp.message(Command("queue"))
async def cmd_queue(m: types.Message):
    rows = get_queue()
    if not rows:
        await m.answer("📭 Очередь пуста.")
        return
    text = "\n".join([f"#{r['id']} — {r['created_at'][:16]}" for r in rows])
    await m.answer(f"<b>Текущая очередь:</b>\n{text}")

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: types.Message):
    post = get_oldest()
    if not post:
        await m.answer("📭 Очередь пуста.")
        return

    import ast
    media = ast.literal_eval(post["payload"])
    caption = f"{post['caption']}\n\n📎 {ALBUM_URL}\n{CONTACT}"

    media_group = [types.InputMediaPhoto(m["file_id"]) if m["type"] == "photo" else types.InputMediaVideo(m["file_id"]) for m in media]
    await send_album(CHANNEL_ID, media_group, caption)
    delete_post(post["id"])
    await m.answer(f"✅ Опубликован пост #{post['id']}")

@dp.message(Command("delete"))
async def cmd_delete(m: types.Message):
    args = m.text.split()
    if len(args) < 2:
        await m.answer("⚠️ Укажи id для удаления. Пример: /delete 3")
        return
    post_id = args[1]
    delete_post(post_id)
    await m.answer(f"🗑 Пост #{post_id} удалён из очереди.")

# ---------- ИНТЕРАКТИВНОЕ МЕНЮ ----------
def _menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ Добавить пост", callback_data="menu_add"),
            InlineKeyboardButton(text="📋 Очередь",       callback_data="menu_queue"),
        ],
        [
            InlineKeyboardButton(text="🕓 Постить старый", callback_data="menu_post_oldest"),
            InlineKeyboardButton(text="❌ Удалить пост",   callback_data="menu_delete"),
        ],
        [
            InlineKeyboardButton(text="🏠 Меню", callback_data="menu_home"),
        ]
    ])

def _home_text() -> str:
    return (
        "<b>Layoutplace Bot</b>\n\n"
        "Это интерактивное меню. Выбирай действие на кнопках ниже 👇\n\n"
        f"⏰ Расписание: {', '.join(POST_TIMES)} (превью за {PREVIEW_BEFORE_MIN} мин)\n"
        "📎 Альбом и контакт внизу подписи — фиксированы."
    )

@dp.message(Command("start"))
async def cmd_start(m: types.Message):
    await m.answer(_home_text(), reply_markup=_menu_kb())

@dp.callback_query(F.data.startswith("menu_"))
async def cb_menu(c: types.CallbackQuery):
    data = c.data
    kb = _menu_kb()

    if data == "menu_home":
        text = _home_text()
    elif data == "menu_add":
        text = (
            "➕ <b>Добавить пост</b>\n\n"
            "1️⃣ Просто перешли пост/альбом сюда.\n"
            "2️⃣ Бот добавит его в очередь.\n"
            "3️⃣ Автопостинг — по расписанию."
        )
    elif data == "menu_queue":
        text = (
            "📋 <b>Очередь</b>\n\n"
            "Посмотреть: /queue\n"
            "Удалить: /delete <id>\n"
            "Постить вручную: /post_oldest"
        )
    elif data == "menu_post_oldest":
        text = (
            "🕓 <b>Постить вручную</b>\n\n"
            "Используй /post_oldest чтобы опубликовать первый пост из очереди."
        )
    elif data == "menu_delete":
        text = (
            "❌ <b>Удалить пост</b>\n\n"
            "Пример: /delete 3\n"
            "Найти id можно через /queue."
        )
    else:
        text = _home_text()

    await c.answer()
    try:
        await c.message.edit_text(text, reply_markup=kb)
    except Exception:
        await c.message.answer(text, reply_markup=kb)

# ---------- SCHEDULER ----------
async def scheduled_post():
    post = get_oldest()
    if not post:
        log.info("Нет постов для публикации.")
        return

    import ast
    media = ast.literal_eval(post["payload"])
    caption = f"{post['caption']}\n\n📎 {ALBUM_URL}\n{CONTACT}"

    media_group = [types.InputMediaPhoto(m["file_id"]) if m["type"] == "photo" else types.InputMediaVideo(m["file_id"]) for m in media]
    await send_album(CHANNEL_ID, media_group, caption)
    delete_post(post["id"])
    log.info(f"Опубликован пост #{post['id']} автоматически")

for t in POST_TIMES:
    h, m = map(int, t.split(":"))
    scheduler.add_job(scheduled_post, CronTrigger(hour=h, minute=m, timezone=TZ))

# ---------- RUN ----------
async def run_bot():
    log.info("🚀 Стартуем Layoutplace Bot...")
    log.info(f"Scheduler TZ={TZ}, times={POST_TIMES}, preview_before={PREVIEW_BEFORE_MIN} мин")
    scheduler.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(run_bot())
