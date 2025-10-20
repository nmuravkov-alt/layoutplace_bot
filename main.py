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

# ======================
# ENV
# ======================
TOKEN = os.getenv("TOKEN")
ADMINS = [int(x) for x in os.getenv("ADMINS", "").split(",") if x.strip()]
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
ALBUM_URL = os.getenv("ALBUM_URL", "https://t.me/PLACE")
CONTACT = os.getenv("CONTACT", "@PLACE")
POST_TIMES = [s.strip() for s in os.getenv("POST_TIMES", "12:00,16:00,20:00").split(",")]
PREVIEW_BEFORE_MIN = int(os.getenv("PREVIEW_BEFORE_MIN", "45"))
TZ = os.getenv("TZ", "Europe/Moscow")

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
tz = pytz.timezone(TZ)
scheduler = AsyncIOScheduler(timezone=tz)

# ======================
# ИМПОРТ БАЗЫ
# ======================
import storage.db as storage_db
storage_db.init_db()
log.info("DB initialized (storage_db.init_db()).")

def db_enqueue(items, caption, src):
    return storage_db.enqueue(items, caption, src)

def db_dequeue_oldest():
    return storage_db.dequeue_oldest()

def db_peek_all():
    return storage_db.peek_all()

def db_get_count():
    return storage_db.stats()["queued"]

def db_delete_by_id(qid):
    return storage_db.delete_by_id(qid)

# ======================
# ХЕЛПЕРЫ
# ======================
def fixed_footer():
    return f"\n\n📎 Альбом: {ALBUM_URL}\n💬 Контакт: {CONTACT}"

def build_final_caption(caption: Optional[str]):
    caption = (caption or "").strip()
    return (caption + fixed_footer()).strip()

def build_media_group(items: List[dict], caption: Optional[str]):
    media = []
    for idx, it in enumerate(items):
        if it["type"] == "photo":
            media.append(
                InputMediaPhoto(media=it["file_id"], caption=caption if idx == 0 else None)
            )
        elif it["type"] == "video":
            media.append(
                InputMediaVideo(media=it["file_id"], caption=caption if idx == 0 else None)
            )
    return media

# ======================
# МЕНЮ
# ======================
def menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="➕ Добавить пост", callback_data="m:add"),
        InlineKeyboardButton(text="📋 Очередь", callback_data="m:queue"),
    )
    kb.row(
        InlineKeyboardButton(text="📤 Постить старый", callback_data="m:post"),
        InlineKeyboardButton(text="🗑 Удалить пост", callback_data="m:delete"),
    )
    return kb.as_markup()

async def send_menu(chat_id: int):
    text = (
        "Это интерактивное меню. Выбирай действие на кнопках ниже 👇\n\n"
        f"🕐 Расписание: {', '.join(POST_TIMES)} (превью за {PREVIEW_BEFORE_MIN} мин)\n"
        "📎 Альбом и контакт внизу подписи — фиксированы."
    )
    await bot.send_message(chat_id, text, reply_markup=menu_kb(), disable_web_page_preview=True)

# ======================
# CALLBACK-И МЕНЮ
# ======================
@dp.callback_query(F.data == "m:add")
async def on_m_add(cq: CallbackQuery):
    await cq.answer()
    await cq.message.answer("Перешли сюда пост/альбом — я сам поставлю в очередь.")

@dp.callback_query(F.data == "m:queue")
async def on_m_queue(cq: CallbackQuery):
    await cq.answer()
    await cq.message.answer(f"📋 В очереди: {db_get_count()}")

@dp.callback_query(F.data == "m:post")
async def on_m_post(cq: CallbackQuery):
    await cq.answer()
    task = db_dequeue_oldest()
    if not task:
        await cq.message.answer("Очередь пуста.")
        return
    await _publish_task(task)
    await cq.message.answer(f"✅ Опубликовано: ID {task['id']}")

@dp.callback_query(F.data == "m:delete")
async def on_m_delete(cq: CallbackQuery):
    await cq.answer()
    await cq.message.answer("Введи ID из очереди для удаления (смотри /queue).")

# ======================
# ПУБЛИКАЦИЯ
# ======================
async def _publish_task(task: dict):
    try:
        items = json.loads(task["payload"])
    except Exception:
        items = []
    caption = build_final_caption(task.get("caption", ""))

    if len(items) >= 2:
        await bot.send_media_group(CHANNEL_ID, build_media_group(items, caption))
    elif len(items) == 1:
        it = items[0]
        if it["type"] == "photo":
            await bot.send_photo(CHANNEL_ID, it["file_id"], caption=caption)
        elif it["type"] == "video":
            await bot.send_video(CHANNEL_ID, it["file_id"], caption=caption)
    else:
        await bot.send_message(CHANNEL_ID, caption)

# ======================
# ОБРАБОТКА МЕДИА
# ======================
@dp.message(F.photo | F.video)
async def on_media(m: Message):
    items = []
    if m.photo:
        items.append({"type": "photo", "file_id": m.photo[-1].file_id})
    elif m.video:
        items.append({"type": "video", "file_id": m.video.file_id})

    caption = (m.caption or "").strip()
    src = (None, None)
    qid = db_enqueue(items, caption, src)
    await m.answer(f"✅ Добавлено в очередь (ID {qid}). Сейчас в очереди: {db_get_count()}")

@dp.message(F.text)
async def on_text(m: Message):
    if m.text.startswith("/"):
        return
    caption = m.text.strip()
    qid = db_enqueue([], caption, (None, None))
    await m.answer(f"✅ Добавлено в очередь (текст) ID {qid}. Сейчас в очереди: {db_get_count()}")

# ======================
# КОМАНДЫ
# ======================
@dp.message(Command("start"))
async def cmd_start(m: Message):
    await send_menu(m.chat.id)

@dp.message(Command("menu"))
async def cmd_menu(m: Message):
    await send_menu(m.chat.id)

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    await m.answer(f"📋 В очереди: {db_get_count()}")

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    task = db_dequeue_oldest()
    if not task:
        await m.answer("Очередь пуста.")
        return
    await _publish_task(task)
    await m.answer(f"✅ Опубликовано: ID {task['id']}")

# ======================
# ПЛАНИРОВЩИК
# ======================
async def scheduled_post():
    task = db_dequeue_oldest()
    if not task:
        return
    await _publish_task(task)
    log.info(f"✅ Автопостинг: опубликовано ID {task['id']}")

async def preview_job():
    log.info("🔄 Проверка на превью (пока выключено для упрощения).")

# ======================
# СТАРТ
# ======================
async def _on_startup():
    log.info("🚀 Стартуем Layoutplace Bot...")
    scheduler.add_job(preview_job, CronTrigger(second="0", minute="*"))
    for hhmm in POST_TIMES:
        hh, mm = [int(x) for x in hhmm.split(":")]
        scheduler.add_job(scheduled_post, CronTrigger(hour=hh, minute=mm))
    scheduler.start()
    log.info(f"Scheduler TZ={TZ}, times={POST_TIMES}, preview_before={PREVIEW_BEFORE_MIN} мин")

async def run_bot():
    await _on_startup()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(run_bot())
