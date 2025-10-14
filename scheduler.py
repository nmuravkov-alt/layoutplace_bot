# scheduler.py
import asyncio
import logging
import os
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from html import escape as _escape

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from storage.db import init_db, get_oldest, find_similar_ids, bulk_delete

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
ADMINS_RAW = os.getenv("ADMINS", "").strip()
TZ = os.getenv("TZ", "Europe/Moscow")
POST_TIMES_RAW = os.getenv("POST_TIMES", "12:00,16:00,20:00").strip()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("scheduler")

def safe_html(text: str) -> str:
    return _escape(text, quote=False)

ADMINS: list[int] = []
for piece in (ADMINS_RAW or "").replace(" ", "").split(","):
    if piece:
        try:
            ADMINS.append(int(piece))
        except ValueError:
            pass

def parse_times(s: str) -> list[time]:
    out: list[time] = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        hh, mm = part.split(":")
        out.append(time(hour=int(hh), minute=int(mm)))
    return out

def next_run_at(times: list[time], tz: ZoneInfo) -> datetime:
    now = datetime.now(tz)
    today_times = [datetime.combine(now.date(), t, tzinfo=tz) for t in times]
    for dt in sorted(today_times):
        if dt > now:
            return dt
    return datetime.combine(now.date() + timedelta(days=1), sorted(times)[0], tzinfo=tz)

async def notify_admins(bot: Bot, text: str):
    for uid in ADMINS:
        try:
            await bot.send_message(uid, safe_html(text))
        except Exception:
            pass

async def post_once(bot: Bot):
    ad = get_oldest()
    if not ad:
        await notify_admins(bot, "⛔ Очередь пуста — пост отменён.")
        return
    ad_text = ad["text"]
    await bot.send_message(CHANNEL_ID, safe_html(ad_text))
    similar_ids = set(find_similar_ids(ad_text) or [])
    similar_ids.add(ad["id"])
    bulk_delete(list(similar_ids))
    await notify_admins(bot, f"✅ Опубликовано. Удалено {len(similar_ids)} похожих.")

async def run_scheduler():
    init_db()
    tz = ZoneInfo(TZ)
    times = parse_times(POST_TIMES_RAW)
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

    while True:
        run_at = next_run_at(times, tz)
        sleep_secs = (run_at - datetime.now(tz)).total_seconds()
        log.info(f"Следующий пост в {run_at} (через {round(sleep_secs/3600,2)} ч)")

        # За 2 минуты — превью админу
        while sleep_secs > 0:
            if sleep_secs <= 120:
                ad = get_oldest()
                if ad:
                    await notify_admins(bot, "🕒 Через 2 минуты будет опубликовано:\n\n" + ad["text"][:3900])
                else:
                    await notify_admins(bot, "🕒 Через 2 минуты, но очередь пуста.")
                break
            await asyncio.sleep(min(60, sleep_secs))
            sleep_secs = (run_at - datetime.now(tz)).total_seconds()

        await asyncio.sleep(max(0, (run_at - datetime.now(tz)).total_seconds()))
        await post_once(bot)

async def main():
    await run_scheduler()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
