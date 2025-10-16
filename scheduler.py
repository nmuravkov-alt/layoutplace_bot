# scheduler.py
import asyncio
import logging
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from typing import Dict, List

from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument

from config import TOKEN as BOT_TOKEN, TZ as TZ_NAME, ADMINS, CHANNEL_ID
from storage.db import init_db, get_oldest, get_count, delete_by_id

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("scheduler")

TZ = ZoneInfo(TZ_NAME or "Europe/Moscow")

SLOTS = [dtime(12, 0), dtime(16, 0), dtime(20, 0)]
PREVIEW_BEFORE_MIN = 45

def _now() -> datetime:
    return datetime.now(tz=TZ)

def _next_slot(now: datetime) -> datetime:
    today = now.date()
    candidates = [datetime.combine(today, t, tzinfo=TZ) for t in SLOTS]
    for dt in candidates:
        if dt > now:
            return dt
    # иначе — первый слот завтра
    tomorrow = today + timedelta(days=1)
    return datetime.combine(tomorrow, SLOTS[0], tzinfo=TZ)

async def _notify_admins(bot: Bot, text: str) -> None:
    for aid in ADMINS:
        try:
            await bot.send_message(aid, text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except Exception as e:
            log.warning("Админ %s недоступен (%s). Нажми /start боту в ЛС.", aid, type(e).__name__)

async def _post_item(bot: Bot, item: Dict):
    text = item["text"]
    media = item["media"]

    if not media:
        await bot.send_message(CHANNEL_ID, text, disable_web_page_preview=True)
    else:
        ims = []
        for i, it in enumerate(media):
            t = it["type"]
            fid = it["file_id"]
            if t == "photo":
                ims.append(InputMediaPhoto(media=fid, caption=text if i == 0 else None, parse_mode=ParseMode.HTML))
            elif t == "video":
                ims.append(InputMediaVideo(media=fid, caption=text if i == 0 else None, parse_mode=ParseMode.HTML))
            elif t == "document":
                ims.append(InputMediaDocument(media=fid, caption=text if i == 0 else None, parse_mode=ParseMode.HTML))
        await bot.send_media_group(CHANNEL_ID, ims)

    # удалить исходники (если знаем)
    if item.get("src_chat_id") and item.get("src_msg_ids"):
        for mid in item["src_msg_ids"]:
            try:
                await bot.delete_message(item["src_chat_id"], mid)
            except Exception as e:
                log.warning("Не смог удалить старое сообщение %s: %s", mid, e)

async def run_scheduler():
    init_db()
    bot = Bot(BOT_TOKEN)

    log.info(
        "Scheduler TZ=%s, times=%s, preview_before=%d min",
        TZ.key,
        ",".join(t.strftime("%H:%M") for t in SLOTS),
        PREVIEW_BEFORE_MIN,
    )

    while True:
        now = _now()
        slot = _next_slot(now)
        preview_at = slot - timedelta(minutes=PREVIEW_BEFORE_MIN)

        # Сообщим ближайшие времена
        await _notify_admins(
            bot,
            (
                f"Планировщик активен.\n"
                f"Следующий слот: <b>{slot:%Y-%m-%d %H:%M}</b> ({TZ.key})\n"
                f"Превью в: <b>{preview_at:%Y-%m-%d %H:%M}</b>\n"
                f"В очереди сейчас: <b>{get_count()}</b>."
            ),
        )

        # Ждём превью
        now = _now()
        if preview_at > now:
            await asyncio.sleep((preview_at - now).total_seconds())

        await _notify_admins(bot, f"Предупреждение: до постинга <b>{PREVIEW_BEFORE_MIN} минут</b>. В очереди: <b>{get_count()}</b>.")

        # Ждём сам слот
        now = _now()
        if slot > now:
            await asyncio.sleep((slot - now).total_seconds())

        # Публикуем один самый старый элемент
        rows = get_oldest(1)
        if not rows:
            await _notify_admins(bot, "Очередь пуста — публиковать нечего.")
        else:
            item = rows[0]
            try:
                await _post_item(bot, item)
                delete_by_id(item["id"])
                await _notify_admins(bot, f"✅ Опубликовано. В очереди осталось: <b>{get_count()}</b>.")
            except Exception as e:
                await _notify_admins(bot, f"❌ Ошибка публикации: <code>{e}</code>")

        # Микропаузa, чтобы не уйти в tight loop
        await asyncio.sleep(1)

async def main():
    await run_scheduler()

if __name__ == "__main__":
    asyncio.run(main())
