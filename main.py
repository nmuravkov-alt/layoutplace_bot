import os
import asyncio
import logging
import pytz
import time
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, InputMediaPhoto, InputMediaVideo
from aiogram.filters import Command

from storage.db import init_db, enqueue, dequeue_oldest, mark_posted, mark_error, get_count, list_queue

# -----------------------------
# CONFIG
# -----------------------------
TOKEN = os.getenv("TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1001758490510"))
ADMINS = [int(x) for x in os.getenv("ADMINS", "").split(",") if x]
ALBUM_URL = os.getenv("ALBUM_URL", "")
CONTACT = os.getenv("CONTACT", "")
TZ = os.getenv("TZ", "Europe/Moscow")
POST_TIMES = os.getenv("POST_TIMES", "12:00,16:00,20:00").split(",")
PREVIEW_BEFORE_MIN = int(os.getenv("PREVIEW_BEFORE_MIN", "45"))
DB_PATH = os.getenv("DB_PATH", "/data/layoutplace.db")

# Проверка токена
if not TOKEN or not TOKEN.startswith("8256997005:"):
    raise RuntimeError("ENV TOKEN пуст или имеет неверный формат. Задайте корректный токен бота.")

# -----------------------------
# SETUP LOGGING
# -----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("layoutplace_bot")

# -----------------------------
# INIT
# -----------------------------
bot = Bot(TOKEN)
dp = Dispatcher()
init_db(DB_PATH)
logger.info("🚀 Стартуем Layoutplace Bot...")

tz = pytz.timezone(TZ)

# -----------------------------
# HELPERS
# -----------------------------
def normalize_text(text: str) -> str:
    """Приводим подпись к единому формату"""
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    new_lines = []
    for line in lines:
        if line.lower().startswith("цена"):
            line = line.replace(":", "—").replace("-", "—")
        new_lines.append(line)
    new_lines.append("")
    new_lines.append(f"Общий альбом: {ALBUM_URL}")
    new_lines.append(f"Покупка/вопросы: {CONTACT}")
    return "\n".join(new_lines)

async def send_preview_to_admins(text: str):
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, f"🔜 Превью поста:\n\n{text}")
        except Exception as e:
            logger.warning(f"Не удалось отправить превью {admin_id}: {e}")

async def post_to_channel(items, caption, qid=None):
    """Постинг альбома с подписью"""
    media_group = []
    for i, item in enumerate(items):
        if item["type"] == "photo":
            media = InputMediaPhoto(media=item["file_id"], caption=caption if i == 0 else None)
        elif item["type"] == "video":
            media = InputMediaVideo(media=item["file_id"], caption=caption if i == 0 else None)
        else:
            continue
        media_group.append(media)

    if not media_group:
        await bot.send_message(CHANNEL_ID, caption)
        return

    try:
        await bot.send_media_group(CHANNEL_ID, media=media_group)
        if qid:
            mark_posted(qid)
        logger.info(f"✅ Опубликован пост ID={qid}")
    except Exception as e:
        if qid:
            mark_error(qid, str(e))
        logger.error(f"Ошибка публикации: {e}")

# -----------------------------
# COMMANDS
# -----------------------------
@dp.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer("Привет 👋 Я бот Layoutplace.\nПерешли мне пост с фото и описанием — я поставлю его в очередь на автопубликацию.")

@dp.message(Command("queue"))
async def cmd_queue(m: Message):
    total = get_count()
    queued = get_count("queued")
    posted = get_count("posted")
    err = get_count("error")
    rows = list_queue(15)
    lines = [f"Всего: {total} | queued: {queued} | posted: {posted} | error: {err}", ""]
    for r in rows:
        t = time.strftime("%d.%m %H:%M", time.localtime(r["created_at"]))
        lines.append(f"#{r['id']} [{r['status']}] {t}")
    await m.answer("\n".join(lines))

@dp.message(Command("post_oldest"))
async def cmd_post_oldest(m: Message):
    row = dequeue_oldest()
    if not row:
        await m.answer("❌ Очередь пуста.")
        return
    import json
    items = json.loads(row["items_json"])
    await post_to_channel(items, row["caption"], row["id"])
    await m.answer(f"✅ Опубликован пост #{row['id']} вручную.")

# -----------------------------
# HANDLER — ПЕРЕСЫЛКА ПОСТОВ
# -----------------------------
@dp.message(F.forward_from_chat)
async def handle_forwarded_post(m: Message):
    """Добавляем пересланный пост (фото+текст) в очередь"""
    try:
        caption = m.caption or m.text or ""
        caption = normalize_text(caption)
        items = []

        if m.photo:
            items.append({"type": "photo", "file_id": m.photo[-1].file_id})
        elif m.video:
            items.append({"type": "video", "file_id": m.video.file_id})

        qid = enqueue(items, caption, src=(m.forward_from_chat.id, m.forward_from_message_id))
        await m.answer(f"✅ Пост #{qid} добавлен в очередь и будет опубликован автоматически.")
        logger.info(f"Добавлен пост #{qid}")
    except Exception as e:
        await m.answer(f"Ошибка добавления: {e}")
        logger.error(f"Ошибка handle_forwarded_post: {e}")

# -----------------------------
# SCHEDULER
# -----------------------------
async def scheduler():
    """Запускаем планировщик превью и публикаций"""
    logger.info(f"Scheduler TZ={TZ}, times={POST_TIMES}, preview_before={PREVIEW_BEFORE_MIN} мин")
    while True:
        now = datetime.now(tz)
        for t_str in POST_TIMES:
            hh, mm = map(int, t_str.split(":"))
            slot = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if now < slot <= now + timedelta(minutes=PREVIEW_BEFORE_MIN):
                row = dequeue_oldest()
                if row:
                    await send_preview_to_admins(row["caption"])
            if abs((now - slot).total_seconds()) < 30:
                row = dequeue_oldest()
                if row:
                    import json
                    items = json.loads(row["items_json"])
                    await post_to_channel(items, row["caption"], row["id"])
        await asyncio.sleep(30)

# -----------------------------
# START
# -----------------------------
async def main():
    asyncio.create_task(scheduler())
    logger.info("Scheduler запущен.")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
