# scheduler.py
import os
import asyncio
import logging
from typing import List, Tuple, Optional
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.types import InputMediaPhoto, InputMediaVideo

# конфиг
try:
    from config import TOKEN, CHANNEL_ID, ADMINS, TZ, POST_TIMES, PREVIEW_MINUTES
except Exception:
    TOKEN = os.getenv("BOT_TOKEN", "")
    CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1000000000000"))
    ADMINS = [int(x) for x in os.getenv("ADMINS", "").replace(" ", "").split(",") if x]
    TZ = os.getenv("TZ", "Europe/Moscow")
    POST_TIMES = os.getenv("POST_TIMES", "12:00,16:00,20:00")
    PREVIEW_MINUTES = int(os.getenv("PREVIEW_MINUTES", "45"))

from storage.db import (
    init_db,
    get_count,
    peek_oldest,
    dequeue_oldest,
)

log = logging.getLogger("layoutplace_scheduler")

def _parse_times(s: str) -> List[Tuple[int,int]]:
    out = []
    for chunk in s.split(","):
        t = chunk.strip()
        if not t:
            continue
        hh, mm = t.split(":")
        out.append((int(hh), int(mm)))
    return out

TZINFO = ZoneInfo(TZ)
SLOTS: List[Tuple[int,int]] = _parse_times(POST_TIMES)

# Чтобы не слать дубли в рамках одного запуска
_sent_preview_keys = set()
_done_post_keys = set()
_last_day_key: Optional[str] = None

def _day_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def _slot_key(dt: datetime, hh: int, mm: int) -> str:
    return f"{dt.strftime('%Y-%m-%d')} {hh:02d}:{mm:02d}"

async def _notify_admins(bot: Bot, text: str):
    for uid in ADMINS:
        try:
            await bot.send_message(uid, text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except Exception as e:
            log.warning(f"Админ {uid} недоступен: {e}")

async def _post_one(bot: Bot):
    task = dequeue_oldest()
    if not task:
        return False

    caption = task.get("caption") or ""
    src = task.get("src")
    items = task.get("items") or []

    if src:
        src_chat_id, src_msg_id = src
        res = await bot.copy_message(
            chat_id=CHANNEL_ID,
            from_chat_id=src_chat_id,
            message_id=src_msg_id,
            caption=caption or None,
            parse_mode=ParseMode.HTML,
            disable_notification=False
        )
        # попытка удалить источник
        try:
            await bot.delete_message(chat_id=src_chat_id, message_id=src_msg_id)
        except Exception as del_err:
            log.warning(f"Не смог удалить старое сообщение {src_chat_id}/{src_msg_id}: {del_err}")
        return True

    # items собранные
    if items:
        if len(items) > 1:
            media = []
            for i, it in enumerate(items):
                t = it.get("type")
                fid = it.get("file_id")
                if not fid:
                    continue
                if t == "photo":
                    media.append(InputMediaPhoto(media=fid, caption=caption if i == 0 else None, parse_mode=ParseMode.HTML))
                elif t == "video":
                    media.append(InputMediaVideo(media=fid, caption=caption if i == 0 else None, parse_mode=ParseMode.HTML))
            if media:
                await bot.send_media_group(chat_id=CHANNEL_ID, media=media)
                return True
        else:
            it = items[0]
            t = it.get("type")
            fid = it.get("file_id")
            if t == "photo":
                await bot.send_photo(chat_id=CHANNEL_ID, photo=fid, caption=caption, parse_mode=ParseMode.HTML)
            elif t == "video":
                await bot.send_video(chat_id=CHANNEL_ID, video=fid, caption=caption, parse_mode=ParseMode.HTML)
            else:
                await bot.send_message(chat_id=CHANNEL_ID, text=caption, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            return True

    # просто текст
    await bot.send_message(chat_id=CHANNEL_ID, text=caption, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    return True

async def run_scheduler():
    init_db()
    bot = Bot(TOKEN)
    log.info(f"Scheduler TZ={TZ}, times={POST_TIMES}, preview_before={PREVIEW_MINUTES} min")

    global _last_day_key, _sent_preview_keys, _done_post_keys
    while True:
        now = datetime.now(TZINFO)
        dk = _day_key(now)
        if _last_day_key != dk:
            # новый день — очищаем флаги
            _sent_preview_keys.clear()
            _done_post_keys.clear()
            _last_day_key = dk

        # если очередь пустая — просто спим
        count = get_count()
        if count == 0:
            await asyncio.sleep(20)
            continue

        for hh, mm in SLOTS:
            slot = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if slot < now - timedelta(hours=12):
                # если почему-то «вчерашнее» — сместим на сегодня
                slot = datetime(now.year, now.month, now.day, hh, mm, tzinfo=TZINFO)
            key = _slot_key(now, hh, mm)

            # превью
            preview_at = slot - timedelta(minutes=PREVIEW_MINUTES)
            if now >= preview_at and key not in _sent_preview_keys:
                task = peek_oldest()
                if task:
                    cap = (task.get("caption") or "").strip()
                    src = task.get("src")
                    kind = "репост из канала" if src else ("альбом" if (task.get("items") and len(task["items"]) > 1) else ("медиа" if task.get("items") else "текст"))
                    await _notify_admins(
                        bot,
                        f"Предстоящий пост в {slot.strftime('%H:%M')} ({TZ}). Тип: {kind}\n\nПревью:\n{cap[:2000]}"
                    )
                _sent_preview_keys.add(key)

            # публикация
            if now >= slot and key not in _done_post_keys:
                ok = await _post_one(bot)
                if ok:
                    await _notify_admins(bot, f"Опубликовано (слот {slot.strftime('%H:%M')}). Осталось в очереди: {get_count()}")
                _done_post_keys.add(key)

        await asyncio.sleep(20)
