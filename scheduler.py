# scheduler.py
import os
import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from html import escape as html_escape

from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest

# DB helpers
from storage.db import (
    init_db,
    get_oldest,
    delete_by_id,      # импорт оставлен на будущее (не обязателен)
    find_similar_ids,
    bulk_delete,
)

# -------------------- ENV --------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()  # @channelusername или -100...
ADMINS_RAW = os.getenv("ADMINS", "").strip()      # список id через запятую
TZ = os.getenv("TZ", "Europe/Moscow").strip()

# Время постинга по умолчанию — 12:00, 16:00, 20:00
TIMES = os.getenv("SCHEDULE_TIMES", "12,16,20")
# За сколько минут прислать превью админу
PREVIEW_MINUTES = int(os.getenv("PREVIEW_MINUTES", "45"))

# -------------------- LOG --------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s : %(message)s")
log = logging.getLogger("scheduler")

# -------------------- UTILS --------------------
def _parse_times(times: str) -> list[int]:
    out = []
    for t in (times or "").split(","):
        t = t.strip()
        if not t:
            continue
        try:
            hour = int(t)
            if 0 <= hour <= 23:
                out.append(hour)
        except ValueError:
            pass
    return sorted(set(out)) or [12, 16, 20]

def _parse_admins(raw: str) -> list[int]:
    ids = []
    for part in (raw or "").replace(" ", "").split(","):
        if not part:
            continue
        if part.lstrip("-").isdigit():
            try:
                ids.append(int(part))
            except ValueError:
                pass
    return ids

def _now_tz(tz: str) -> datetime:
    return datetime.now(ZoneInfo(tz))

def _next_time(now: datetime, hours: list[int]) -> datetime:
    """Ближайшее время сегодня/завтра из списка часов (минуты=00)."""
    candidates = []
    for h in hours:
        cand = now.replace(hour=h, minute=0, second=0, microsecond=0)
        if cand <= now:
            cand += timedelta(days=1)
        candidates.append(cand)
    return min(candidates)

def _escape(text: str) -> str:
    return html_escape(text or "")

def _channel_id_value() -> int | str:
    cid = CHANNEL_ID
    if cid.lstrip("-").isdigit():
        return int(cid)
    return cid  # например "@layoutplace"

HOURS = _parse_times(TIMES)
ADMINS = _parse_admins(ADMINS_RAW)
CHANNEL = _channel_id_value()

# -------------------- CORE --------------------
async def send_preview_to_admins(bot: Bot, when_post: datetime, text: str):
    """Всегда шлём превью только в личку админам (экранировано)."""
    if not ADMINS:
        log.warning("ADMINS пуст — некому отправлять превью.")
        return
    caption = (
        f"🕒 ПРЕВЬЮ поста на {when_post.strftime('%Y-%m-%d %H:%M')} ({TZ})\n\n"
        f"{_escape(text)}"
    )
    for uid in ADMINS:
        try:
            await bot.send_message(uid, caption)
        except TelegramBadRequest:
            # на всякий случай ещё раз plain
            await bot.send_message(uid, caption)

async def send_to_channel(bot: Bot, text: str):
    """Постинг в канал: пытаемся как HTML, при ошибке — экранируем."""
    try:
        await bot.send_message(CHANNEL, text, parse_mode=ParseMode.HTML)
    except TelegramBadRequest:
        safe = _escape(text)
        await bot.send_message(CHANNEL, safe)

async def do_post(bot: Bot):
    """Берём самое старое объявление, постим и удаляем похожие."""
    row = get_oldest()
    if not row:
        log.info("Очередь пуста — постить нечего.")
        return

    ad_id = row["id"]
    text = row["text"]

    await send_to_channel(bot, text)

    # Удаляем сам пост и похожие
    ids = find_similar_ids(text)
    if ad_id not in ids:
        ids.append(ad_id)
    deleted = bulk_delete(ids)
    log.info("Опубликовано и удалено %s похожих (ids=%s)", deleted, ids)

async def run_scheduler():
    if not BOT_TOKEN or not CHANNEL_ID:
        raise RuntimeError("Не заданы BOT_TOKEN/CHANNEL_ID")

    # Инициализируем БД (создаст таблицы при первом запуске)
    init_db()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    log.info(
        "Scheduler  TZ=%s, times=%s, preview_before=%s min",
        TZ, ",".join(map(str, HOURS)), PREVIEW_MINUTES
    )

    last_preview_for: datetime | None = None
    last_post_for: datetime | None = None

    while True:
        now = _now_tz(TZ)
        post_at = _next_time(now, HOURS)
        preview_at = post_at - timedelta(minutes=PREVIEW_MINUTES)

        # превью (всегда только админам)
        if (last_preview_for != post_at) and (now >= preview_at) and (now < post_at):
            row = get_oldest()
            preview_text = row["text"] if row else "⛔ Очередь пуста"
            await send_preview_to_admins(bot, post_at, preview_text)
            last_preview_for = post_at
            log.info("Превью отправлено. Следующий пост в %s", post_at)

        # публикация
        if (last_post_for != post_at) and (now >= post_at):
            await do_post(bot)
            last_post_for = post_at
            # небольшая пауза, чтобы не дёргать цикл сразу после публикации
            await asyncio.sleep(2)

        # информативный лог один раз при старте
        if last_preview_for is None and last_post_for is None:
            delta_h = (preview_at - now).total_seconds() / 3600
            log.info(
                "Следующий ПРЕВЬЮ через %.2f часов (%s)",
                delta_h, preview_at.strftime("%Y-%m-%d %H:%M:%S %Z")
            )

        await asyncio.sleep(10)

def main():
    asyncio.run(run_scheduler())

if __name__ == "__main__":
    main()
