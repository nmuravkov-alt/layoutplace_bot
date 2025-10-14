# scheduler.py
import asyncio
import logging
import os
from datetime import datetime, timedelta
import pytz

from aiogram import Bot
from storage.db import get_oldest, delete_by_id, find_similar_ids, bulk_delete

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
TZ = os.getenv("TZ", "Europe/Moscow")

bot = Bot(BOT_TOKEN)
tz = pytz.timezone(TZ)
logging.basicConfig(level=logging.INFO)

async def post_oldest():
    """Постинг самого старого объявления с удалением похожих"""
    ad = get_oldest()
    if not ad:
        logging.info("Нет объявлений для публикации.")
        return

    text = ad["text"]
    ad_id = ad["id"]
    try:
        await bot.send_message(CHANNEL_ID, text, disable_web_page_preview=False)
        delete_by_id(ad_id)
        similar_ids = find_similar_ids(text, exclude_id=ad_id)
        if similar_ids:
            bulk_delete(similar_ids)
            logging.info(f"Удалено похожих объявлений: {len(similar_ids)}")
        logging.info(f"Опубликовано и удалено объявление ID {ad_id}")
    except Exception as e:
        logging.error(f"Ошибка при публикации: {e}")

async def scheduler():
    """3 поста в день (10:00, 16:00, 22:00 по TZ)"""
    while True:
        now = datetime.now(tz)
        # список часов постинга
        target_hours = [10, 16, 22]
        # найти ближайшее время
        next_hour = min(
            (h for h in target_hours if h > now.hour),
            default=target_hours[0]
        )
        next_time = now.replace(hour=next_hour, minute=0, second=0, microsecond=0)
        if next_time <= now:
            next_time += timedelta(days=1)
        delay = (next_time - now).total_seconds()
        logging.info(f"Следующий пост через {delay/3600:.2f} часов ({next_time})")
        await asyncio.sleep(delay)
        await post_oldest()

if __name__ == "__main__":
    asyncio.run(scheduler())
