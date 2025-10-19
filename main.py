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
# CONFIG (читаем из ENV)
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

# Проверка токена (подтв. по твоему формату)
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("ENV TOKEN пуст или имеет неверный формат. Задайте корректный токен бота.")

# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("layoutplace_bot")

# -----------------------------
# INIT
# -----------------------------
bot = Bot(TOKEN)
dp = Dispatcher()

# Инициализируем БД (путь берём из ENV)
init_db(DB_PATH)
logger.info("🚀 Стартуем Layoutplace Bot...")

tz = pytz.timezone(TZ)

# -----------------------------
# HELPERS
# -----------------------------
def normalize_text(text: str) -> str:
    """Приводим подпись к единому формату и добавляем постоянные хвосты."""
    if not text:
        text = ""
    lines = [line.strip() for line in text.split("\n")]
    lines = [l for l in lines if l]  # убираем пустые

    new_lines = []
    for line in lines:
        # Нормализуем 'Цена'
        if line.lower().startswith("цена"):
            # заменяем двоеточие/дефис на длинное тире
            line = line.replace(":", "—").replace("-", "—")
        new_lines.append(line)

    # Пустая строка перед хвостом
    if new_lines and new_lines[-1] != "":
        new_lines.append("")

    # Постоянные хвосты (как просил)
    new_lines.append(f"Общий альбом: {ALBUM_URL}")
    new_lines.append(f"Покупка/вопросы: {CONTACT}")

    return "\n".join(new_lines).strip()

async def send_preview_to_admins(text: str):
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, f"🔜 Превью поста через {PREVIEW_BEFORE_MIN} мин:\n\n{text}")
        except Exception as e:
            logger.warning(f"Не удалось отправить превью {admin_id}: {e}")

async def post_to_channel(items, caption, qid=None):
    """Постинг альбома (или одного медиа) с подписью в канал."""
    media_group = []
    for i, item in enumerate(items or []):
        if item.get("type") == "photo":
            media = InputMediaPhoto(media=item["file_id"], caption=caption if i == 0 else None)
        elif item.get("type") == "video":
            media = InputMediaVideo(media=item["file_id"], caption=caption if i == 0 else None)
        else:
            continue
        media_group.append(media)

    try:
        if media_group:
            await bot.send_media_group(CHANNEL_ID, media=media_group)
        else:
            # нет медиа — публикуем просто текст
            await bot.send_message(CHANNEL_ID, caption)

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
    await m.answer(
        "Привет! Перешли мне пост (из канала) с фотографиями и описанием — я поставлю его в очередь.\n\n"
        "Команды:\n"
        "/queue — показать очередь\n"
        "/post_oldest — опубликовать самый старый пост вручную"
    )

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
    items = json.loads(row["items_json"]) if row.get("items_json") else []
    await post_to_channel(items, row["caption"], row["id"])
    await m.answer(f"✅ Опубликован пост #{row['id']} вручную.")

# -----------------------------
# HANDLER — ПЕРЕСЫЛКА ПОСТОВ
# -----------------------------
@dp.message(F.forward_from_chat)
async def handle_forwarded_post(m: Message):
    """Добавляем пересланный пост (фото/видео + текст) в очередь."""
    try:
        caption_raw = m.caption or m.text or ""
        caption = normalize_text(caption_raw)

        items = []
        # одиночные медиа
        if m.photo:
            items.append({"type": "photo", "file_id": m.photo[-1].file_id})
        elif m.video:
            items.append({"type": "video", "file_id": m.video.file_id})

        # Альбом (media group) — собираем все фото/видео из альбома
        if m.media_group_id:
            # aiogram сам не агрегирует — тут обычно нужен storage для групп.
            # Упростим: если пришло фото/видео как часть альбома, он всё равно попадёт сюда
            # как отдельные сообщения. Для полноценного склеивания альбома нужен хендлер
            # с буфером по media_group_id. (Можно добавить позже.)
            pass

        src_chat_id = m.forward_from_chat.id if m.forward_from_chat else None
        src_msg_id = m.forward_from_message_id if m.forward_from_message_id else None

        qid = enqueue(items, caption, src=(src_chat_id, src_msg_id))
        await m.answer(f"✅ Пост #{qid} добавлен в очередь и будет опубликован автоматически.")
        logger.info(f"Добавлен пост #{qid}")
    except Exception as e:
        await m.answer(f"Ошибка добавления: {e}")
        logger.error(f"Ошибка handle_forwarded_post: {e}")

# -----------------------------
# SCHEDULER
# -----------------------------
async def scheduler():
    """Планировщик: превью за PREVIEW_BEFORE_MIN и публикация в слоты POST_TIMES."""
    logger.info(f"Scheduler TZ={TZ}, times={POST_TIMES}, preview_before={PREVIEW_BEFORE_MIN} мин")
    while True:
        now = datetime.now(tz)
        for t_str in POST_TIMES:
            try:
                hh, mm = map(int, t_str.split(":"))
            except Exception:
                continue

            slot = now.replace(hour=hh, minute=mm, second=0, microsecond=0)

            # Превью: окно (now, now+PREVIEW_BEFORE]
            if now < slot <= now + timedelta(minutes=PREVIEW_BEFORE_MIN):
                row = dequeue_oldest()
                if row:
                    await send_preview_to_admins(row["caption"])

            # Публикация: ±30 cекунд от слота
            if abs((now - slot).total_seconds()) < 30:
                row = dequeue_oldest()
                if row:
                    import json
                    items = json.loads(row["items_json"]) if row.get("items_json") else []
                    await post_to_channel(items, row["caption"], row["id"])

        await asyncio.sleep(30)

# -----------------------------
# START FUNCTIONS
# -----------------------------
async def main():
    # Важно: сбросить webhook и очистить pending, чтобы getUpdates не конфликтовал
    await bot.delete_webhook(drop_pending_updates=True)
    # Запускаем планировщик параллельно
    asyncio.create_task(scheduler())
    logger.info("Scheduler запущен.")
    # Стартуем polling
    await dp.start_polling(bot)

def run_bot():
    """Синхронная обёртка для runner.py"""
    asyncio.run(main())

# Локальный запуск без runner.py
if __name__ == "__main__":
    run_bot()
