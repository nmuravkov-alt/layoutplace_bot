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
    delete_by_id,
    find_similar_ids,
    bulk_delete,
)

# -------------------- ENV --------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()  # @channelusername или -100...
ADMINS_RAW = os.getenv("ADMINS", "").strip()      # список id через запятую
TZ = os.getenv("TZ", "Europe/Moscow").strip()

# Время постинга: по умолчанию 12:00, 16:00, 20:00
TIMES = os.getenv("SCHEDULE_TIMES", "12,16,20")
# За сколько минут прислать превью (уведомление с текстом поста)
PREVIEW_MINUTES = int(os.getenv("PREVIEW_MINUTES", "45"))

# Куда отправлять превью: admins (личка всем админам) или channel (в канал как сервисное)
PREVIEW_TARGET = os.getenv("PREVIEW_TARGET", "admins").lower()  # admins | channel

# -------------------- LOG --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(name)s : %(message)s"
)
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

def _escape_for_preview(text: str) -> str:
    """Превью шлем безопасно (без парсинга HTML)."""
    return html_escape(text or "")

def _channel_id_value() -> int | str:
    # поддержка @username и числового id
    cid = CHANNEL_ID
    if cid.lstrip("-").isdigit():
        return int(cid)
    return cid  # например "@layoutplace"

HOURS = _parse_times(TIMES)
ADMINS = _parse_admins(ADMINS_RAW)
CHANNEL = _channel_id_value()

# -------------------- CORE --------------------
async def send_preview(bot: Bot, when_post: datetime, text: str):
    caption = (
        f"🕒 ПРЕВЬЮ поста на {when_post.strftime('%Y-%m-%d %H:%M')} "
        f"({TZ})\n\n{text}"
    )

    if PREVIEW_TARGET == "channel":
        try:
            await bot.send_message(CHANNEL, caption, parse_mode=ParseMode.HTML)
        except TelegramBadRequest:
            # если вдруг ломается парсер — отправим без разметки
            await bot.send_message(CHANNEL, html_escape(caption))
    else:
        # в личку всем админам — без риска парсинга
        safe = _escape_for_preview(caption)
        for uid in ADMINS:
            try:
                await bot.send_message(uid, safe)
            except TelegramBadRequest:
                # на всякий случай продублируем ещё раз plain (обычно не требуется)
                await bot.send_message(uid, safe)

async def send_to_channel(bot: Bot, text: str):
    """Постинг в канал с защитой от кривого HTML."""
    try:
        await bot.send_message(CHANNEL, text, parse_mode=ParseMode.HTML)
    except TelegramBadRequest:
        # если в тексте есть неразрешённые теги — отправим экранированный вариант
        safe = html_escape(text)
        await bot.send_message(CHANNEL, safe)

async def do_post(bot: Bot):
    """Берём самое старое объявление, постим и чистим похожие."""
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

    # инициализация БД
    init_db()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    tz = ZoneInfo(TZ)
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

        # отправка превью
        if (last_preview_for != post_at) and (now >= preview_at) and (now < post_at):
            row = get_oldest()
            if row:
                await send_preview(bot, post_at, _escape_for_preview(row["text"]))
            else:
                # информируем, что очередь пуста
                await send_preview(bot, post_at, _escape_for_preview("⛔ Очередь пуста"))
            last_preview_for = post_at
            log.info("Превью отправлено. Следующий пост в %s", post_at)

        # сам пост
        if (last_post_for != post_at) and (now >= post_at):
            await do_post(bot)
            last_post_for = post_at
            # после поста перевычислим следующее окно, чтобы не крутиться впустую
            await asyncio.sleep(2)

        # информативный лог один раз при старте цикла
        if last_preview_for is None and last_post_for is None:
            delta = (preview_at - now).total_seconds() / 3600
            log.info(
                "Следующий ПРЕВЬЮ через %.2f часов (%s)",
                delta, preview_at.strftime("%Y-%m-%d %H:%M:%S %Z")
            )

        await asyncio.sleep(10)

def main():
    asyncio.run(run_scheduler())

if __name__ == "__main__":
    main()
