import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from aiogram import Bot
from aiogram.enums import ParseMode
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


def _build_channel_link(channel_id_or_username: str, msg_id: int) -> str:
    """
    Возвращает ссылку на сообщение в канале:
    - публичный канал:   https://t.me/<username>/<msg_id>
    - приватный канал:   https://t.me/c/<internal>/<msg_id>, где internal = abs(chat_id) без префикса -100
    """
    if isinstance(channel_id_or_username, str) and channel_id_or_username.startswith("@"):
        return f"https://t.me/{channel_id_or_username[1:]}/{msg_id}"
    # numeric chat_id
    try:
        cid = int(channel_id_or_username)
    except Exception:
        # вдруг передали '-100...' как строку
        try:
            cid = int(str(channel_id_or_username))
        except Exception:
            return str(channel_id_or_username)

    internal = str(abs(cid))
    if internal.startswith("100"):
        internal = internal[3:]
    return f"https://t.me/c/{internal}/{msg_id}"


async def _notify_admins(bot: Bot, text: str):
    for aid in ADMINS:
        try:
            await bot.send_message(aid, text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except Exception as e:
            logging.warning(f"Админ {aid} недоступен: {e}")


async def _send_preview(bot: Bot):
    text = "Превью: через ~45 минут будет публикация очередного поста."
    await _notify_admins(bot, text)


async def _publish(bot: Bot, task: dict):
    """
    task = { id, items:[{type,file_id},...], caption, src_chat_id, src_msg_id }
    """
    caption = build_caption(task.get("caption") or "")
    items = task["items"]

    # 1) публикация
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

    # 2) попытка удалить старый пост
    src_chat_id = task.get("src_chat_id")
    src_msg_id = task.get("src_msg_id")
    if src_chat_id and src_msg_id:
        try:
            await bot.delete_message(src_chat_id, src_msg_id)
        except TelegramBadRequest as e:
            # Частый кейс: бот НЕ автор старого поста в канале — Telegram не даёт удалить.
            link = _build_channel_link(str(CHANNEL_ID if str(src_chat_id).startswith("-100") else src_chat_id), src_msg_id)
            logging.warning(f"Не смог удалить старое сообщение {src_chat_id}/{src_msg_id}: {e}")
            await _notify_admins(
                bot,
                (
                    "Не удалось удалить старый пост (ограничение Telegram — бот может удалять только свои сообщения).\n"
                    f"Пожалуйста, удалите вручную: {link}"
                ),
            )
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

            # публикация в слот
            if now >= nxt:
                task = dequeue_oldest()
                if task:
                    await _publish(bot, task)
                await asyncio.sleep(2)

            await asyncio.sleep(5)
        except Exception as e:
            logging.exception(f"Scheduler loop error: {e}")
            await asyncio.sleep(5)
