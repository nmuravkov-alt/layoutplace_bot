"""
Планировщик: 12:00, 16:00, 20:00 (TZ из config.py), превью за 45 минут.
Запускать отдельным процессом/сервисом.
"""
import asyncio
import logging
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from typing import Optional

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from config import TOKEN as BOT_TOKEN, TZ as _TZ, ADMINS, CHANNEL_ID
from storage.db import init_db, get_oldest, pop_oldest, get_count
from main import publish_entry, delete_source_by_ids, normalize_caption  # используем готовые функции

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | scheduler | %(message)s")
log = logging.getLogger("scheduler")

TZ = ZoneInfo(_TZ)
SLOTS = (time(12, 0), time(16, 0), time(20, 0))
PREVIEW_BEFORE = timedelta(minutes=45)

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

async def _notify_admins(text: str):
    for aid in ADMINS:
        try:
            await bot.send_message(aid, text, disable_web_page_preview=True)
        except Exception as e:
            log.warning(f"Админ {aid} недоступен: {e}")

def _next_dt(after: datetime, t: time) -> datetime:
    dt = datetime.combine(after.date(), t, tzinfo=TZ)
    if dt <= after:
        dt += timedelta(days=1)
    return dt

def compute_next_slot(now: datetime) -> tuple[datetime, datetime]:
    """Возвращает (время_превью, время_поста) для ближайшего слота."""
    candidates = [ _next_dt(now, s) for s in SLOTS ]
    post_dt = min(candidates)
    preview_dt = post_dt - PREVIEW_BEFORE
    return preview_dt, post_dt

async def run_scheduler():
    init_db()
    await _notify_admins("Планировщик запущен.")
    preview_sent_for: Optional[datetime] = None
    posted_for: Optional[datetime] = None

    while True:
        now = datetime.now(TZ)
        preview_dt, post_dt = compute_next_slot(now)

        # Превью
        if preview_dt <= now < post_dt:
            # чтобы не слать дубли — проверим «на этот слот уже отправляли?»
            if preview_sent_for != post_dt:
                cnt = get_count()
                text = f"Превью: ближайший пост в {post_dt.strftime('%Y-%m-%d %H:%M')} (TZ={_TZ}).\nВ очереди: {cnt}."
                await _notify_admins(text)
                preview_sent_for = post_dt

        # Постинг
        if now >= post_dt:
            if posted_for != post_dt:
                entry = get_oldest()
                if entry:
                    try:
                        await publish_entry(entry)
                        await delete_source_by_ids(entry.get("src_chat_id"), entry.get("src_msg_id"))
                        _ = pop_oldest()
                        await _notify_admins(f"Опубликовано по расписанию. Осталось в очереди: {get_count()}.")
                    except Exception as e:
                        logging.exception(f"Ошибка публикации по расписанию: {e}")
                        await _notify_admins("Ошибка публикации по расписанию (смотри логи).")
                else:
                    await _notify_admins("Публикация по расписанию пропущена — очередь пуста.")
                posted_for = post_dt

        # Спим мелкими интервалами
        await asyncio.sleep(20)

async def main():
    await run_scheduler()

if __name__ == "__main__":
    asyncio.run(main())
