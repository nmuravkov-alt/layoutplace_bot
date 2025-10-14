# scheduler.py
import asyncio
import logging
import os
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from storage.db import (
    init_db,
    get_oldest,
    delete_by_id,
    find_similar_ids,
    bulk_delete,
)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
TZ = os.getenv("TZ", "Europe/Moscow")
ADMINS_RAW = os.getenv("ADMINS", "").strip()

# часы для постинга по умолчанию: 12:00, 16:00, 20:00
POST_TIMES_RAW = os.getenv("POST_TIMES", "12:00,16:00,20:00").strip()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("scheduler")

# разбор админов
ADMINS: list[int] = []
for piece in (ADMINS_RAW or "").replace(" ", "").split(","):
    if piece:
        try:
            ADMINS.append(int(piece))
        except ValueError:
            pass

def _ad_fields(ad) -> tuple[int, str]:
    if ad is None:
        return (0, "")
    if isinstance(ad, dict):
        return int(ad.get("id", 0)), str(ad.get("text", ""))
    try:
        return int(ad[0]), str(ad[1])
    except Exception:
        return (0, "")

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
        if dt > now + timedelta(seconds=1):
            return dt
    # если сегодня все прошли — берём первое на завтра
    return datetime.combine(now.date() + timedelta(days=1), sorted(times)[0], tzinfo=tz)

async def notify_admins(bot: Bot, text: str):
    for uid in ADMINS:
        try:
            await bot.send_message(uid, text, disable_web_page_preview=True)
        except Exception:
            pass

async def post_once(bot: Bot):
    """Постим самое старое объявление в канал и удаляем похожие."""
    ad = get_oldest()
    ad_id, ad_text = _ad_fields(ad)
    if not ad_id or not ad_text.strip():
        await notify_admins(bot, "⛔ Очередь пуста — автопост отменён.")
        log.info("Очередь пуста — автопост отменён.")
        return

    # превью уже отправляли заранее (в run_scheduler), здесь публикуем
    await bot.send_message(CHANNEL_ID, ad_text, disable_web_page_preview=False)

    similar_ids = set(find_similar_ids(ad_text) or [])
    similar_ids.add(ad_id)
    bulk_delete(list(similar_ids))

    info = f"✅ Опубликовано. Удалено похожих: {len(similar_ids)}"
    await notify_admins(bot, info)
    log.info(info)

async def run_scheduler():
    # init
    init_db()
    tz = ZoneInfo(TZ)
    times = parse_times(POST_TIMES_RAW)

    bot = Bot(
        BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    log.info(f"Scheduler TZ={TZ}, times={POST_TIMES_RAW}")

    while True:
        run_at = next_run_at(times, tz)
        secs = (run_at - datetime.now(tz)).total_seconds()
        log.info(f"Следующий пост через {round(secs/3600, 2)} часов ({run_at.strftime('%Y-%m-%d %H:%M:%S %Z')})")

        # за 2 минуты до поста отправим превью админам
        preview_sent = False
        while secs > 0:
            if not preview_sent and secs <= 120:
                ad = get_oldest()
                _, ad_text = _ad_fields(ad)
                if ad_text.strip():
                    await notify_admins(
                        bot,
                        "🕒 Через ~2 минуты будет опубликовано:\n\n" + ad_text[:3900],
                    )
                else:
                    await notify_admins(bot, "🕒 Через ~2 минуты пост, но очередь пуста.")
                preview_sent = True
            sleep_for = 5 if secs <= 120 else 30
            await asyncio.sleep(min(sleep_for, secs))
            secs = (run_at - datetime.now(tz)).total_seconds()

        try:
            await post_once(bot)
        except Exception as e:
            log.exception("Ошибка автопоста: %s", e)
            try:
                await notify_admins(bot, f"❌ Ошибка автопоста: {e}")
            except Exception:
                pass

async def main():
    await run_scheduler()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
