import asyncio
import logging
import os
from datetime import datetime, timedelta
import pytz

from aiogram import Bot
from aiogram.enums import ParseMode

from storage.db import init_db, get_oldest, delete_by_id, find_similar_ids, bulk_delete

# ------------------ Конфиг ------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
ADMINS = os.getenv("ADMINS", "").split(",")
TZ = os.getenv("TZ", "Europe/Moscow")

# Настройки расписания — 3 поста в день
POST_TIMES = ["12:00", "16:00", "20:00"]

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN, default=ParseMode.HTML)
tz = pytz.timezone(TZ)


# ------------------ Функции ------------------
async def send_preview(ad):
    """Отправляет превью админам перед постом"""
    preview_text = f"🕒 Через 10 минут автопост:\n\n{ad['text']}"
    for admin in ADMINS:
        if admin.strip():
            try:
                await bot.send_message(int(admin.strip()), preview_text)
            except Exception as e:
                log.warning(f"Не удалось отправить превью админу {admin}: {e}")


async def post_to_channel():
    """Постит самое старое объявление и удаляет похожие"""
    ad = get_oldest()
    if not ad:
        log.info("Нет объявлений для публикации")
        return

    try:
        await bot.send_message(CHANNEL_ID, ad["text"])
        log.info(f"Опубликовано объявление ID={ad['id']}")

        # Удаляем похожие
        similar_ids = find_similar_ids(ad["text"])
        bulk_delete(similar_ids)
        delete_by_id(ad["id"])
        log.info(f"Удалено {len(similar_ids)} похожих объявлений")

        # Уведомляем админа
        for admin in ADMINS:
            if admin.strip():
                try:
                    await bot.send_message(int(admin.strip()), f"✅ Опубликовано объявление ID={ad['id']}")
                except Exception as e:
                    log.warning(f"Ошибка при уведомлении админа {admin}: {e}")
    except Exception as e:
        log.error(f"Ошибка при постинге: {e}")


async def scheduler_loop():
    """Основной цикл расписания"""
    init_db()  # теперь без await, т.к. функция синхронная
    log.info(f"Scheduler TZ={TZ}, times={','.join(POST_TIMES)}")

    while True:
        now = datetime.now(tz)
        next_time = None

        for t in POST_TIMES:
            hour, minute = map(int, t.split(":"))
            candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if candidate > now:
                next_time = candidate
                break

        if not next_time:
            next_time = (now + timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)

        wait_seconds = (next_time - now).total_seconds()
        log.info(f"Следующий пост через {wait_seconds/3600:.2f} часов ({next_time.strftime('%Y-%m-%d %H:%M:%S %Z')})")

        # Отправляем превью за 10 минут до поста
        preview_delay = wait_seconds - 600
        if preview_delay > 0:
            await asyncio.sleep(preview_delay)
            ad = get_oldest()
            if ad:
                await send_preview(ad)

        # Ждём оставшиеся 10 минут
        await asyncio.sleep(max(0, wait_seconds - max(preview_delay, 0)))
        await post_to_channel()


async def main():
    await scheduler_loop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("⏹️ Scheduler остановлен")
