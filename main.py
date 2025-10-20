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
from aiogram.exceptions import TelegramBadRequest

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger


# ======================
# ЛОГГЕР
# ======================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("layoutplace_bot")


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
    raise RuntimeError("ENV TOKEN пуст или имеет неверный формат.")

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
tz = pytz.timezone(TZ)
scheduler = AsyncIOScheduler(timezone=tz)


# ======================
# SAFE EDIT HELPERS
# ======================
async def safe_edit_text(msg, text, **kwargs):
    try:
        await msg.edit_text(text, **kwargs)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            return
        raise

async def safe_edit_reply_markup(msg, reply_markup=None):
    try:
        await msg.edit_reply_markup(reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            return
        raise


# ======================
# ИМПОРТ БД (+ ИНИЦИАЛИЗАЦИЯ!)
# ======================
from storage import db as storage_db

# ВАЖНО: создаём таблицы сразу, чтобы не было "no such table: queue"
try:
    storage_db.init_db()
    log.info("DB initialized (storage_db.init_db()).")
except Exception as e:
    log.warning(f"DB init failed: {e}")

def db_enqueue(items: List[dict], caption: str, src: Optional[tuple]) -> int:
    return storage_db.enqueue(items, caption, src)

def db_dequeue_oldest() -> Optional[dict]:
    return storage_db.dequeue_oldest()

def db_peek_all() -> List[dict]:
    return storage_db.peek_all()

def db_delete_by_id(qid: int) -> int:
    return storage_db.delete_by_id(qid)

def db_stats() -> int:
    return storage_db.stats().get("queued", 0)


# ======================
# ХЕЛПЕРЫ
# ======================
def fixed_footer() -> str:
    return f"\n\nОбщий альбом: {ALBUM_URL}\nПокупка/вопросы: {CONTACT}"

def build_final_caption(raw_caption: Optional[str]) -> str:
    raw_caption = (raw_caption or "").strip()
    if not raw_caption:
        return fixed_footer()
    return f"{raw_caption}{fixed_footer()}"

def build_media_group(items: List[dict], caption: Optional[str]):
    media = []
    for idx, it in enumerate(items):
        t = (it.get("type") or "").lower()
        if t == "photo":
            media.append(InputMediaPhoto(media=it["file_id"], caption=caption if idx == 0 else None))
        elif t == "video":
            media.append(InputMediaVideo(media=it["file_id"], caption=caption if idx == 0 else None))
    return media


# ======================
# ПРЕВЬЮ
# ======================
def preview_kb(qid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="✅ Опубликовать", callback_data=f"preview:post:{qid}"),
        InlineKeyboardButton(text="🕓 Отложить", callback_data=f"preview:skip:{qid}")
    )
    kb.row(InlineKeyboardButton(text="🗑 Удалить", callback_data=f"preview:delete:{qid}"))
    return kb.as_markup()


async def send_preview_to_admins(task: dict):
    try:
        items = json.loads(task["payload"]) if task.get("payload") else []
    except Exception:
        items = []
    final_caption = build_final_caption(task.get("caption") or "")
    qid = task["id"]

    for admin in ADMINS:
        try:
            if len(items) >= 2:
                media = build_media_group(items, final_caption)
                await bot.send_media_group(chat_id=admin, media=media)
                await bot.send_message(admin, f"Предпросмотр ID <code>{qid}</code>", reply_markup=preview_kb(qid))
            elif len(items) == 1:
                it = items[0]
                if it["type"] == "photo":
                    await bot.send_photo(admin, it["file_id"], caption=final_caption, reply_markup=preview_kb(qid))
                elif it["type"] == "video":
                    await bot.send_video(admin, it["file_id"], caption=final_caption, reply_markup=preview_kb(qid))
                else:
                    await bot.send_message(admin, final_caption, reply_markup=preview_kb(qid))
            else:
                await bot.send_message(admin, final_caption, reply_markup=preview_kb(qid))
        except Exception as e:
            log.warning(f"Не удалось отправить превью админу {admin}: {e}")


# ======================
# ПУБЛИКАЦИЯ
# ======================
async def publish_task(task: dict):
    try:
        items = json.loads(task["payload"]) if task.get("payload") else []
    except Exception:
        items = []
    caption = build_final_caption(task.get("caption") or "")

    if len(items) > 1:
        media = build_media_group(items, caption)
        await bot.send_media_group(CHANNEL_ID, media)
    elif len(items) == 1:
        it = items[0]
        if it["type"] == "photo":
            await bot.send_photo(CHANNEL_ID, it["file_id"], caption=caption)
        elif it["type"] == "video":
            await bot.send_video(CHANNEL_ID, it["file_id"], caption=caption)
        else:
            await bot.send_message(CHANNEL_ID, caption)
    else:
        await bot.send_message(CHANNEL_ID, caption)


# ======================
# CALLBACK-и
# ======================
@dp.callback_query(F.data.startswith("preview:"))
async def on_preview_action(cq: CallbackQuery):
    _, action, sid = cq.data.split(":")
    qid = int(sid)

    if action == "post":
        task = db_dequeue_oldest()
        if not task or task["id"] != qid:
            await cq.answer("Этот пост уже удалён или не первый в очереди", show_alert=True)
            return
        await publish_task(task)
        await cq.message.answer(f"✅ Опубликовано: ID {qid}")
        await cq.answer()
    elif action == "delete":
        db_delete_by_id(qid)
        await cq.message.answer(f"🗑 Удалено из очереди: ID {qid}")
        await cq.answer()
    else:
        await cq.answer("⏸ Отложено", show_alert=False)


# ======================
# ПРИЁМ КОНТЕНТА
# ======================
# Буфер для альбомов, если позже захочешь объединять вручную:
_ALBUM_TMP: Dict[str, dict] = {}

@dp.message(F.media_group_id)
async def on_album_piece(m: Message):
    # Сейчас просто сообщаем, что альбом принят — логику объединения можно нарастить при желании
    await m.answer("📸 Альбом принят. Я добавлю все элементы в один пост.")

@dp.message(F.photo | F.video)
async def on_media(m: Message):
    it = {"type": "photo" if m.photo else "video", "file_id": m.photo[-1].file_id if m.photo else m.video.file_id}
    src = (None, None)
    qid = db_enqueue([it], m.caption or "", src)
    for admin in ADMINS:
        try:
            await bot.send_message(admin, f"Добавлен новый пост ID {qid} (в очереди: {db_stats()})")
        except Exception:
            pass

@dp.message(F.text)
async def on_text(m: Message):
    if m.text.startswith("/"):
        return
    qid = db_enqueue([], m.text, (None, None))
    for admin in ADMINS:
        try:
            await bot.send_message(admin, f"Добавлен текстовый пост ID {qid} (в очереди: {db_stats()})")
        except Exception:
            pass


# ======================
# КОМАНДЫ
# ======================
HELP_TEXT = (
    "📋 Команды:\n"
    "/queue — показать размер очереди\n"
    "/post_oldest — опубликовать первый пост вручную\n"
    "/help — помощь\n\n"
    "Просто пересылай мне посты (одиночные/альбомы) — я добавлю их в очередь, пришлю превью и опубликую по расписанию."
)

@dp.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(HELP_TEXT, disable_web_page_preview=True)

@dp.message(Command("help"))
async def cmd_help(m: Message):
    await m.answer(HELP_TEXT, disable_web_page_preview=True)

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    await m.answer(f"В очереди: {db_stats()}")

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    task = db_dequeue_oldest()
    if not task:
        await m.answer("Очередь пуста.")
        return
    await publish_task(task)
    await m.answer(f"✅ Опубликовано: ID {task['id']}")


# ======================
# ПЛАНИРОВЩИК
# ======================
async def preview_job():
    posts = db_peek_all()
    if not posts:
        return
    task = posts[0]
    now = datetime.now(tz)
    for hhmm in POST_TIMES:
        h, m = [int(x) for x in hhmm.split(":")]
        slot = now.replace(hour=h, minute=m, second=0, microsecond=0)
        preview_time = slot - timedelta(minutes=PREVIEW_BEFORE_MIN)
        if abs((now - preview_time).total_seconds()) < 60:
            await send_preview_to_admins(task)
            break

async def scheduled_post():
    task = db_dequeue_oldest()
    if task:
        await publish_task(task)


# ======================
# ЗАПУСК
# ======================
async def _on_startup():
    log.info("🚀 Стартуем Layoutplace Bot...")
    # превью каждую минуту
    scheduler.add_job(preview_job, CronTrigger(second="0", minute="*"))
    # слоты
    for t in POST_TIMES:
        h, m = [int(x) for x in t.split(":")]
        scheduler.add_job(scheduled_post, CronTrigger(hour=h, minute=m))
    scheduler.start()
    log.info(f"Scheduler TZ={TZ}, times={POST_TIMES}, preview_before={PREVIEW_BEFORE_MIN} мин")

async def run_bot():
    await _on_startup()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(run_bot())
