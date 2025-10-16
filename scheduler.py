# scheduler.py
import os
import asyncio
import logging
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from typing import List
import json

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramUnauthorizedError, TelegramBadRequest

# конфиг
from config import TOKEN as BOT_TOKEN, CHANNEL_ID, TZ as TZ_NAME, ADMINS, ALBUM_URL, CONTACT_TEXT

# функции БД (очередь копий)
from storage.db import (
    init_db,
    queue_next_pending,   # взять самую старую запись в статусе pending/previewed
    queue_mark_status,    # проставить статус: previewed/posted/error
    queue_count_pending,  # количество pending
)

# ---------------- ЛОГИ ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("scheduler")

# ---------------- РАСПИСАНИЕ ----------------
TIMES_RAW = os.getenv("TIMES", "12:00,16:00,20:00")
PREVIEW_BEFORE_MIN = int(os.getenv("PREVIEW_BEFORE_MIN", "45"))

tz = ZoneInfo(TZ_NAME)

def _parse_times(s: str) -> List[dtime]:
    out: List[dtime] = []
    for token in s.split(","):
        token = token.strip()
        if not token:
            continue
        h, m = token.split(":")
        out.append(dtime(hour=int(h), minute=int(m)))
    return out or [dtime(12, 0), dtime(16, 0), dtime(20, 0)]

TIMES = _parse_times(TIMES_RAW)

def _now() -> datetime:
    return datetime.now(tz)

def _next_slot(now: datetime) -> datetime:
    today = [datetime.combine(now.date(), t, tzinfo=tz) for t in TIMES]
    future = [dt for dt in today if dt > now]
    if future:
        return future[0]
    tomorrow = now.date() + timedelta(days=1)
    return datetime.combine(tomorrow, TIMES[0], tzinfo=tz)

# ---------------- СТИЛЬ (тот же, без эмодзи) ----------------
def unify_caption(text: str | None) -> str:
    t = (text or "").strip()
    t = t.replace("Цена -", "Цена —")
    while "  " in t:
        t = t.replace("  ", " ")
    lines = [ln.strip() for ln in t.splitlines()]
    lines = [ln for ln in lines if ln]
    body = "\n".join(lines).strip()

    tail = []
    if ALBUM_URL and (ALBUM_URL not in body):
        tail.append(f"Общий альбом: {ALBUM_URL}")
    low = body.lower()
    if CONTACT_TEXT and (CONTACT_TEXT.lower() not in low):
        tail.append(f"Покупка / вопросы: {CONTACT_TEXT}")
    if tail:
        body = (body + "\n\n" + "\n".join(tail)).strip()
    return body

async def _notify_admins(bot: Bot, text: str):
    for aid in ADMINS:
        try:
            await bot.send_message(aid, text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except TelegramUnauthorizedError:
            log.warning("Админ %s недоступен (Unauthorized). Нажми /start боту в ЛС.", aid)
        except Exception as e:
            log.warning("Не удалось отправить админу %s: %s", aid, e)

async def copy_and_delete(
    bot: Bot,
    source_chat_id: int | str,
    message_ids: List[int],
    target: int | str,
    caption_override: str | None,
):
    """
    Копируем пост/альбом без автора (copy_message).
    Первому элементу ставим унифицированную подпись.
    После публикации пытаемся удалить оригиналы.
    """
    new_caption = unify_caption(caption_override)
    for idx, mid in enumerate(message_ids):
        if idx == 0 and new_caption:
            await bot.copy_message(
                chat_id=target,
                from_chat_id=source_chat_id,
                message_id=mid,
                caption=new_caption,
                parse_mode=ParseMode.HTML,
            )
        else:
            await bot.copy_message(
                chat_id=target,
                from_chat_id=source_chat_id,
                message_id=mid,
            )

    # удалить оригиналы (если есть права delete в источнике)
    for mid in message_ids:
        try:
            await bot.delete_message(chat_id=source_chat_id, message_id=mid)
        except TelegramBadRequest:
            pass
        except Exception as e:
            log.debug("Ошибка удаления исходного %s/%s: %s", source_chat_id, mid, e)

# ---------------- ОСНОВНОЙ ЦИКЛ ----------------
async def run_scheduler():
    props = DefaultBotProperties(parse_mode=ParseMode.HTML)
    bot = Bot(BOT_TOKEN, default=props)

    await _notify_admins(
        bot,
        f"Планировщик запущен.\nСлоты: <code>{TIMES_RAW}</code>\n"
        f"Превью: <b>{PREVIEW_BEFORE_MIN}</b> мин.\nВ очереди: <b>{queue_count_pending()}</b>",
    )

    while True:
        now = _now()
        slot = _next_slot(now)
        preview_at = slot - timedelta(minutes=PREVIEW_BEFORE_MIN)
        log.info(
            "Следующий слот постинга: %s (%s). Превью в: %s",
            slot.strftime("%Y-%m-%d %H:%M"), TZ_NAME, preview_at.strftime("%Y-%m-%d %H:%M")
        )

        # ---- дождаться времени превью ----
        delay_preview = max(0.0, (preview_at - _now()).total_seconds())
        await asyncio.sleep(delay_preview)

        row = queue_next_pending()
        if row:
            caption = row.get("caption_override") or ""
            await _notify_admins(
                bot,
                "Предпросмотр публикации\n"
                f"Время поста: <code>{slot.strftime('%Y-%m-%d %H:%M')}</code> ({TZ_NAME})\n"
                f"Источник: <code>{row['source_chat_id']}</code>\n"
                f"Messages: <code>{row['message_ids']}</code>\n\n"
                f"{caption}"
            )
            queue_mark_status(row["id"], "previewed")
        else:
            await _notify_admins(bot, "Очередь пуста — публиковать нечего.")

        # ---- дождаться самого слота ----
        delay_post = max(0.0, (slot - _now()).total_seconds())
        await asyncio.sleep(delay_post)

        row = queue_next_pending()
        if not row:
            log.info("Слот %s: очередь пуста.", slot.strftime("%H:%M"))
            continue

        # message_ids — JSON
        try:
            message_ids = [int(x) for x in json.loads(row["message_ids"])]
        except Exception:
            message_ids = [int(x) for x in eval(row["message_ids"])]

        try:
            await copy_and_delete(
                bot=bot,
                source_chat_id=row["source_chat_id"],
                message_ids=message_ids,
                target=CHANNEL_ID,
                caption_override=row.get("caption_override"),
            )
            queue_mark_status(row["id"], "posted")
            await _notify_admins(bot, f"Опубликовано из <code>{row['source_chat_id']}</code> — ids={message_ids}")
            log.info("Posted task #%s", row["id"])
        except Exception as e:
            queue_mark_status(row["id"], "error")
            await _notify_admins(bot, f"Ошибка публикации id={row['id']}: <code>{e}</code>")
            log.exception("Ошибка публикации task #%s: %s", row["id"], e)

# ---------------- ENTRY ----------------
async def main():
    init_db()
    log.info("Scheduler TZ=%s, times=%s, preview_before=%s min", TZ_NAME, TIMES_RAW, PREVIEW_BEFORE_MIN)
    await run_scheduler()

if __name__ == "__main__":
    asyncio.run(main())
