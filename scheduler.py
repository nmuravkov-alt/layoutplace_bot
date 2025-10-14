# scheduler.py
import asyncio
import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from html import escape as html_escape

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest

from storage.db import (
    init_db,
    get_oldest,
    find_similar_ids,
    bulk_delete,
)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
ADMINS_RAW = os.getenv("ADMINS", "").strip()
TZ = os.getenv("TZ", "Europe/Moscow")
POST_TIMES_RAW = os.getenv("POST_TIMES", "12,16,20")
PREVIEW_BEFORE_MIN = int(os.getenv("PREVIEW_BEFORE_MIN", "45"))
POST_REPORT_TO_CHANNEL = os.getenv("POST_REPORT_TO_CHANNEL", "0").strip() == "1"

tz = ZoneInfo(TZ)

ADMINS = []
for p in ADMINS_RAW.replace(";", ",").split(","):
    p = p.strip()
    if p and p.lstrip("-").isdigit():
        ADMINS.append(int(p))

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("scheduler")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

def _parse_times(raw: str):
    out = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" in part:
            hh, mm = part.split(":", 1)
            out.append((int(hh), int(mm)))
        else:
            out.append((int(part), 0))
    return out or [(12, 0), (16, 0), (20, 0)]

POST_TIMES = _parse_times(POST_TIMES_RAW)

def _today_at(hh: int, mm: int) -> datetime:
    now = datetime.now(tz)
    return now.replace(hour=hh, minute=mm, second=0, microsecond=0)

def _next_slot() -> datetime:
    now = datetime.now(tz)
    candidates = []
    for hh, mm in POST_TIMES:
        t = _today_at(hh, mm)
        if t <= now:
            t += timedelta(days=1)
        candidates.append(t)
    return min(candidates)

async def _notify_admins(text: str):
    for uid in ADMINS:
        try:
            await bot.send_message(uid, text, disable_web_page_preview=True)
        except Exception:
            pass

async def _send_preview(when_post: datetime, text: str):
    safe = html_escape(text or "")
    caption = f"🕒 Предпросмотр поста (публикация в {when_post.strftime('%H:%M %d.%m')}, {TZ})\n\n{safe}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Опубликовать сейчас", callback_data="postnow")]
    ])
    for uid in ADMINS:
        try:
            await bot.send_message(uid, caption, reply_markup=kb)
        except Exception:
            pass

async def _send_to_channel(text: str):
    try:
        await bot.send_message(CHANNEL_ID, text, disable_web_page_preview=False)
    except TelegramBadRequest:
        await bot.send_message(CHANNEL_ID, html_escape(text), parse_mode=None, disable_web_page_preview=False)

async def _post_once():
    row = get_oldest()
    if not row:
        await _notify_admins("⛔ Очередь пуста — пост отменён.")
        return
    ad_id, text = row
    await _send_to_channel(text)
    similar = find_similar_ids(ad_id, threshold=0.88)
    removed = bulk_delete([ad_id] + similar)

    # отчёт админам
    now_h = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    await _notify_admins(
        f"✅ Опубликовано ({now_h}). ID: <code>{ad_id}</code>. "
        f"Удалено похожих (включая исходный): <b>{removed}</b>."
    )
    # опционально — лог в канал
    if POST_REPORT_TO_CHANNEL:
        await _send_to_channel(f"ℹ️ Пост опубликован. ID: {ad_id}. Удалено похожих: {removed}.")

async def run_scheduler():
    init_db()
    times_str = ",".join(f"{hh:02d}:{mm:02d}" for hh, mm in POST_TIMES)
    log.info("Scheduler TZ=%s, times=%s, preview_before=%s min", TZ, times_str, PREVIEW_BEFORE_MIN)

    last_preview_for = None
    last_post_for = None

    while True:
        next_post = _next_slot()
        preview_at = next_post - timedelta(minutes=PREVIEW_BEFORE_MIN)
        now = datetime.now(tz)

        # Превью (однократно в каждом окне)
        if last_preview_for != next_post and preview_at <= now < next_post:
            row = get_oldest()
            text = row[1] if row else "⛔ Очередь пуста"
            await _send_preview(next_post, text)
            last_preview_for = next_post
            log.info("Превью отправлено. Пост в %s", next_post)

        # Публикация
        if last_post_for != next_post and now >= next_post:
            await _post_once()
            last_post_for = next_post

        # инфо-лог при запуске
        if last_preview_for is None and last_post_for is None:
            delta_h = max(0, (preview_at - now).total_seconds()) / 3600
            log.info("Следующий ПРЕВЬЮ через %.2f часов (%s)", delta_h, preview_at.strftime("%Y-%m-%d %H:%M:%S %Z"))

        await asyncio.sleep(10)

async def main():
    await run_scheduler()

if __name__ == "__main__":
    asyncio.run(main())
