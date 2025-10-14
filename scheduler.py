# scheduler.py
# 1) Регулярные слоты (напр. 12:00,16:00,20:00) с превью за N минут и кнопкой «Опубликовать сейчас»
# 2) Разовые job'ы из /post_at HH:MM (храним ad_id и время), с превью за N минут

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
    job_get_next,
    job_mark_preview_sent,
    job_delete,
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

def _next_regular_slot() -> datetime:
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

async def _send_preview(text: str, when_post: datetime):
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

async def _post_by_ad_id(ad_id: int, text: str):
    await _send_to_channel(text)
    similar = find_similar_ids(ad_id, threshold=0.88)
    removed = bulk_delete([ad_id] + similar)
    now_h = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    await _notify_admins(
        f"✅ Опубликовано ({now_h}). ID: <code>{ad_id}</code>. "
        f"Удалено похожих (включая исходный): <b>{removed}</b>."
    )
    if POST_REPORT_TO_CHANNEL:
        await _send_to_channel(f"ℹ️ Пост опубликован. ID: {ad_id}. Удалено похожих: {removed}.")

async def run_scheduler():
    init_db()
    times_str = ",".join(f"{hh:02d}:{mm:02d}" for hh, mm in POST_TIMES)
    log.info("Scheduler TZ=%s, times=%s, preview_before=%s min", TZ, times_str, PREVIEW_BEFORE_MIN)

    last_regular_preview_for = None
    last_regular_post_for = None

    while True:
        now = datetime.now(tz)

        # ---------- 1) Регулярные слоты ----------
        next_post = _next_regular_slot()
        preview_at = next_post - timedelta(minutes=PREVIEW_BEFORE_MIN)

        # превью для регулярного слота
        if last_regular_preview_for != next_post and preview_at <= now < next_post:
            row = get_oldest()
            text = row[1] if row else "⛔ Очередь пуста"
            await _send_preview(text, next_post)
            last_regular_preview_for = next_post
            log.info("Превью (регулярное) отправлено. Пост в %s", next_post)

        # публикация регулярного слота — берём текущее самое старое
        if last_regular_post_for != next_post and now >= next_post:
            row = get_oldest()
            if row:
                ad_id, text = row
                await _post_by_ad_id(ad_id, text)
            else:
                await _notify_admins("⛔ Очередь пуста — регулярный пост пропущен.")
            last_regular_post_for = next_post

        # ---------- 2) Разовые job'ы из /post_at ----------
        job = job_get_next(int(now.timestamp()))
        if job:
            run_at = datetime.fromtimestamp(job["run_at"], tz)
            j_preview_at = run_at - timedelta(minutes=PREVIEW_BEFORE_MIN)

            # превью job'а
            if job["preview_sent"] == 0 and j_preview_at <= now < run_at:
                # текст по ad_id на момент превью всё ещё лежит в очереди
                # (если админ опубликовал раньше — при публикации job удалим сам job)
                row = get_oldest()
                text = row[1] if row and row[0] == job["ad_id"] else "⛔ Пост для job не найден в очереди"
                await _send_preview(text, run_at)
                job_mark_preview_sent(job["id"])
                log.info("Превью (job #%s) отправлено. Пост в %s", job["id"], run_at)

            # публикация job'а ровно в run_at (если ad ещё в очереди)
            if now >= run_at:
                row = get_oldest()
                if row and row[0] == job["ad_id"]:
                    await _post_by_ad_id(row[0], row[1])
                else:
                    await _notify_admins(f"⚠️ Job #{job['id']}: объявление уже отсутствует в очереди — пропущено.")
                job_delete(job["id"])

        # инфо-лог при самом первом цикле
        if last_regular_preview_for is None and last_regular_post_for is None:
            delta_h = max(0, (preview_at - now).total_seconds()) / 3600
            log.info("Следующий ПРЕВЬЮ через %.2f часов (%s)", delta_h, preview_at.strftime("%Y-%m-%d %H:%M:%S %Z"))

        await asyncio.sleep(10)

async def main():
    await run_scheduler()

if __name__ == "__main__":
    asyncio.run(main())
