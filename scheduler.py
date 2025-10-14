# scheduler.py
# Планировщик: превью админу за N минут до поста и сам пост в канал.

import asyncio
import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from storage.db import (
    init_db,
    get_oldest,
    delete_by_id,
    find_similar_ids,
    bulk_delete,
)

# -------------------- Конфиг --------------------

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
ADMINS_RAW = os.getenv("ADMINS", "").strip()
TZ = os.getenv("TZ", "Europe/Moscow")

# Время постинга по-умолчанию — 12:00, 16:00, 20:00
TIMES_RAW = os.getenv("POST_TIMES", "12,16,20").strip()
# За сколько минут делать превью админу
PREVIEW_BEFORE_MIN = int(os.getenv("PREVIEW_BEFORE_MIN", "45"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

def _parse_admins(raw: str) -> List[int]:
    res: List[int] = []
    for part in raw.replace(";", ",").split(","):
        p = part.strip()
        if not p:
            continue
        try:
            res.append(int(p))
        except ValueError:
            pass
    return res

ADMINS: List[int] = _parse_admins(ADMINS_RAW)

try:
    tz = ZoneInfo(TZ)
except Exception:
    tz = ZoneInfo("UTC")

def _parse_times(raw: str) -> List[datetime.time]:
    times: List[datetime.time] = []
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        try:
            hh = int(p)
            mm = 0
            if ":" in p:
                hh, mm = map(int, p.split(":", 1))
            times.append(datetime.now(tz).replace(hour=hh, minute=mm, second=0, microsecond=0).time())
        except Exception:
            continue
    return times

POST_TIMES = _parse_times(TIMES_RAW)

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("scheduler")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

# -------------------- Планирование --------------------

def _today_at(t: datetime.time) -> datetime:
    now = datetime.now(tz)
    return now.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)

def _next_occurrence(target: datetime) -> datetime:
    """Если целевое время уже прошло — переносим на завтра в то же время."""
    now = datetime.now(tz)
    if target <= now:
        target = target + timedelta(days=1)
    return target

async def _send_preview(ad_id: int, text: str, when_post: datetime):
    """Превью уходит ТОЛЬКО в личку админам."""
    if not ADMINS:
        log.info("Preview skipped: no ADMINS configured")
        return
    header = (
        f"🕒 Предпросмотр поста (публикация в {when_post.strftime('%H:%M %d.%m')}, {TZ})\n"
        f"ID в очереди: <code>{ad_id}</code>\n\n"
    )
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, header + text)
        except Exception as e:
            log.warning("Preview send failed to %s: %s", admin_id, e)

async def _do_post():
    """Постинг самого старого + чистка похожих."""
    oldest = get_oldest()
    if not oldest:
        log.info("Постинг пропущен: очередь пуста")
        return
    ad_id, text = oldest
    await bot.send_message(CHANNEL_ID, text)

    similar = find_similar_ids(ad_id, threshold=0.88)
    removed = bulk_delete([ad_id] + similar)
    log.info("Опубликован %s, удалено из очереди %s (включая похожие)", ad_id, removed)

async def run_scheduler():
    init_db()
    times_info = ",".join(t.strftime("%H:%M") for t in POST_TIMES)
    log.info("Scheduler  TZ=%s, times=%s, preview_before=%s min", TZ, times_info, PREVIEW_BEFORE_MIN)

    # Расчёт “окна” для превью
    preview_delta = timedelta(minutes=PREVIEW_BEFORE_MIN)

    # Бесконечный цикл планировщика
    while True:
        now = datetime.now(tz)

        # ближайшая цель среди всех времён сегодня/завтра
        next_posts: List[datetime] = [_next_occurrence(_today_at(t)) for t in POST_TIMES]
        next_post = min(next_posts)

        # Если пришло время превью
        preview_time = next_post - preview_delta
        if preview_time <= now < next_post:
            # отправим превью один раз в этом окне
            # берём самый старый (только для просмотра, без удаления)
            oldest = get_oldest()
            if oldest:
                ad_id, text = oldest
                await _send_preview(ad_id, text, next_post)
                # ждём до времени поста (чтобы не слать превью многократно)
                sleep_sec = max(5, int((next_post - datetime.now(tz)).total_seconds()))
                log.info("Следующий ПОСТ через %.2f минут (в %s)", sleep_sec / 60, next_post.strftime("%H:%M %d.%m"))
                await asyncio.sleep(sleep_sec)
                # выполнить пост
                await _do_post()
            else:
                # очереди нет — просто ждём до времени поста и перепланируем
                sleep_sec = max(5, int((next_post - now).total_seconds()))
                log.info("Очередь пуста. Жду %s сек до следующего слота постинга", sleep_sec)
                await asyncio.sleep(sleep_sec)
        else:
            # ни превью, ни пост — просто спим немного
            # чтобы не грузить CPU, проверяем раз в 30 сек
            await asyncio.sleep(30)

# -------------------- Точка входа --------------------

async def main():
    try:
        await run_scheduler()
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
