# scheduler.py
import asyncio
import logging
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument

from config import TOKEN as BOT_TOKEN, CHANNEL_ID, TZ, ADMINS
from storage.db import init_db, get_oldest, get_count, delete_by_id

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("scheduler")

BOT = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
ZONE = ZoneInfo(TZ)

# слоты и превью
SLOTS = [time(12, 0), time(16, 0), time(20, 0)]
PREVIEW_DELTA = timedelta(minutes=45)

async def notify_admins(text: str):
    for aid in ADMINS:
        try:
            await BOT.send_message(aid, text, disable_web_page_preview=True)
        except Exception as e:
            log.warning(f"Админ {aid} недоступен ({e}). Нажми /start боту в ЛС.")

def next_slot(now: datetime) -> datetime:
    today = now.date()
    candidates = [datetime.combine(today, t, tzinfo=ZONE) for t in SLOTS]
    future = [dt for dt in candidates if dt > now]
    if future:
        return future[0]
    # иначе — первый слот следующего дня
    return datetime.combine(today + timedelta(days=1), SLOTS[0], tzinfo=ZONE)

async def post_item(item: Dict):
    text = item["text"]
    media = item["media"]

    if not media:
        await BOT.send_message(CHANNEL_ID, text, disable_web_page_preview=True)
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
        await BOT.send_media_group(CHANNEL_ID, ims)

    # удаляем исходники, если знаем
    if item.get("src_chat_id") and item.get("src_msg_ids"):
        for mid in item["src_msg_ids"]:
            try:
                await BOT.delete_message(item["src_chat_id"], mid)
            except Exception as e:
                log.warning(f"Не смог удалить старое сообщение {mid}: {e}")

async def run_scheduler():
    init_db()
    now = datetime.now(ZONE)
    log.info("Scheduler TZ=%s, times=%s, preview_before=45 min", TZ, ",".join([t.strftime("%H:%M") for t in SLOTS]))

    while True:
        now = datetime.now(ZONE)
        slot = next_slot(now)
        preview_at = slot - PREVIEW_DELTA

        # превью (если ещё не пропустили)
        if now < preview_at:
            await notify_admins(f"Предстоящее окно постинга: <b>{slot.strftime('%Y-%m-%d %H:%M')}</b> ({TZ}). "
                                f"В очереди сейчас: <b>{get_count()}</b>.")

            sleep_sec = (preview_at - now).total_seconds()
            await asyncio.sleep(sleep_sec)

        # перед самим слотом ещё раз проверим и отправим превью, если кто-то не получил
        await notify_admins(f"Слот постинга <b>{slot.strftime('%Y-%m-%d %H:%M')}</b> ({TZ}). "
                            f"Пытаюсь опубликовать самый старый пост. В очереди: <b>{get_count()}</b>.")

        # ждём до точного времени слота
        now2 = datetime.now(ZONE)
        if now2 < slot:
            await asyncio.sleep((slot - now2).total_seconds())

        # берём самый старый
        rows = get_oldest(1)
        if not rows:
            await notify_admins("Очередь пуста — публиковать нечего.")
        else:
            item = rows[0]
            try:
                await post_item(item)
                delete_by_id(item["id"])
                await notify_admins("✅ Опубликовал пост. В очереди осталось: <b>%d</b>." % get_count())
            except Exception as e:
                await notify_admins(f"❌ Ошибка публикации: <code>{e}</code>")

        # маленькая пауза и идём к следующему циклу
        await asyncio.sleep(3)

async def main():
    await run_scheduler()

if __name__ == "__main__":
    asyncio.run(main())
