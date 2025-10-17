import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest

from config import TZ, TIMES, PREVIEW_BEFORE_MIN, ADMINS, CHANNEL_ID
from storage.db import init_db, get_count, dequeue_oldest
from utils.text import build_caption

tz = ZoneInfo(TZ)

def _today_slots() -> list[datetime]:
    today = datetime.now(tz).date()
    slots = []
    for t in TIMES:
        hh, mm = map(int, t.split(":"))
        slots.append(datetime(today.year, today.month, today.day, hh, mm, tzinfo=tz))
    return slots

def _next_slot() -> datetime:
    now = datetime.now(tz)
    slots = sorted(_today_slots())
    for s in slots:
        if s > now:
            return s
    # если все прошли — первый слот завтра
    hh, mm = map(int, TIMES[0].split(":"))
    tomorrow = now.date() + timedelta(days=1)
    return datetime(tomorrow.year, tomorrow.month, tomorrow.day, hh, mm, tzinfo=tz)

async def _send_preview(bot: Bot):
    text = "Тестовое превью\nПост был бы тут за 45 минут до публикации."
    for aid in ADMINS:
        try:
            await bot.send_message(aid, text)
        except Exception as e:
            logging.warning(f"Не смог отправить превью админу {aid}: {e}")

async def _publish(bot: Bot, task: dict):
    """
    task = { id, items:[{type,file_id},...], caption, src_chat_id, src_msg_id }
    """
    caption = build_caption(task.get("caption") or "")
    items = task["items"]

    # 1) постинг
    try:
        if len(items) == 1:
            it = items[0]
            t = it["type"]
            fid = it["file_id"]
            if t == "photo":
                await bot.send_photo(CHANNEL_ID, fid, caption=caption)
            elif t == "video":
                await bot.send_video(CHANNEL_ID, fid, caption=caption)
            elif t == "document":
                await bot.send_document(CHANNEL_ID, fid, caption=caption)
            else:
                await bot.send_message(CHANNEL_ID, caption or "(пустая подпись)")
        else:
            media = []
            from aiogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument
            for idx, it in enumerate(items):
                t = it["type"]; fid = it["file_id"]
                cap = caption if (idx == 0) else None
                if t == "photo":
                    media.append(InputMediaPhoto(media=fid, caption=cap))
                elif t == "video":
                    media.append(InputMediaVideo(media=fid, caption=cap))
                elif t == "document":
                    media.append(InputMediaDocument(media=fid, caption=cap))
            await bot.send_media_group(CHANNEL_ID, media)
    except TelegramBadRequest as e:
        logging.error(f"Ошибка постинга: {e}")
        # не удаляем оригинал, продолжаем
    # 2) попытка удалить старый пост в канале
    src_chat_id = task.get("src_chat_id")
    src_msg_id = task.get("src_msg_id")
    if src_chat_id and src_msg_id:
        try:
            await bot.delete_message(src_chat_id, src_msg_id)
        except Exception as e:
            logging.warning(f"Не смог удалить старое сообщение {src_chat_id}/{src_msg_id}: {e}")

async def run_scheduler(bot: Bot):
    """
    Вызывается из main._run() как background-task.
    Следит за слотами, шлёт превью и публикует по времени.
    """
    logging.info(f"Scheduler TZ={TZ}, times={','.join(TIMES)}, preview_before={PREVIEW_BEFORE_MIN} min")
    init_db()

    preview_sent_for: set[str] = set()  # yyyy-mm-dd HH:MM

    while True:
        try:
            nxt = _next_slot()
            now = datetime.now(tz)
            # превью
            if (nxt - now) <= timedelta(minutes=PREVIEW_BEFORE_MIN):
                key = nxt.strftime("%Y-%m-%d %H:%M")
                if key not in preview_sent_for:
                    await _send_preview(bot)
                    preview_sent_for.add(key)

            # публикация ровно в слот
            if now >= nxt:
                # публикуем 1 элемент из очереди
                task = dequeue_oldest()
                if task:
                    await _publish(bot, task)
                # небольшой слип, чтобы не опрашивать слишком часто
                await asyncio.sleep(2)

            await asyncio.sleep(5)
        except Exception as e:
            logging.exception(f"Scheduler loop error: {e}")
            await asyncio.sleep(5)
