# scheduler.py
import asyncio
import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

# --- БД-утилиты ---
from storage.db import (
    init_db,
    get_oldest,
    delete_by_id,
    find_similar_ids,
    bulk_delete,
)

# ----------------- Конфиг -----------------
BOT_TOKEN   = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID  = os.getenv("CHANNEL_ID", "").strip()      # @username или -100...
ADMINS_RAW  = os.getenv("ADMINS", "").strip()          # "123, 456 789"
TZ_NAME     = os.getenv("TZ", "Europe/Moscow").strip()
POST_TIMES  = os.getenv("POST_TIMES", "12,16,20").strip()  # часы через запятую

def _parse_admins(raw: str) -> list[int]:
    parts = [p.strip() for p in raw.replace(",", " ").split()]
    out: list[int] = []
    for p in parts:
        try:
            out.append(int(p))
        except ValueError:
            pass
    return out

ADMINS: list[int] = _parse_admins(ADMINS_RAW)
TZ = ZoneInfo(TZ_NAME)

def _parse_hours(raw: str) -> list[int]:
    hours: list[int] = []
    for p in raw.replace("/", ",").split(","):
        p = p.strip()
        if not p:
            continue
        try:
            h = int(p)
            if 0 <= h <= 23:
                hours.append(h)
        except ValueError:
            continue
    hours = sorted(set(hours))
    return hours or [12, 16, 20]

HOURS = _parse_hours(POST_TIMES)

# ----------------- Логирование -----------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("scheduler")

# ----------------- Хелперы -----------------
def now() -> datetime:
    return datetime.now(TZ)

def human_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")

def next_run_after(dt: datetime) -> datetime:
    """Следующее время запуска ≥ dt по списку часов HOURS."""
    base = dt.replace(minute=0, second=0, microsecond=0)
    today_candidates = [base.replace(hour=h) for h in HOURS]
    for cand in today_candidates:
        if cand >= dt:
            return cand
    # иначе — завтра в первый час из списка
    return (base + timedelta(days=1)).replace(hour=HOURS[0])

async def notify_admins(bot: Bot, text: str) -> None:
    if not ADMINS:
        return
    for uid in ADMINS:
        try:
            await bot.send_message(uid, text, disable_web_page_preview=True)
        except Exception as e:
            log.warning(f"notify_admins fail for {uid}: {e}")

async def send_to_channel(bot: Bot, text: str) -> None:
    await bot.send_message(chat_id=CHANNEL_ID, text=text, disable_web_page_preview=True)

# ----------------- Основная работа -----------------
async def do_one_post(bot: Bot) -> None:
    """Берём самое старое, показываем превью админам, постим, чистим похожие, шлём отчёт."""
    await init_db()

    row = await get_oldest()
    if not row:
        await notify_admins(bot, "⚠️ Очередь пустая — постить нечего.")
        log.info("Очередь пустая.")
        return

    ad_id = row["id"] if isinstance(row, dict) else row[0]
    ad_text = row["text"] if isinstance(row, dict) else row[1]

    # Превью админам
    preview = (
        "📝 <b>Предстоящий пост</b>\n\n"
        f"{ad_text}\n\n"
        f"ID в очереди: <code>{ad_id}</code>"
    )
    await notify_admins(bot, preview)

    # Пост в канал
    await send_to_channel(bot, ad_text)

    # Поиск и удаление похожих
    similar_ids = await find_similar_ids(ad_id)  # список int (может быть пуст)
    removed = 0
    if similar_ids:
        ids = set(similar_ids)
        ids.add(ad_id)
        removed = await bulk_delete(list(ids))
    else:
        await delete_by_id(ad_id)
        removed = 1

    # Итоговый отчёт
    report = (
        "✅ <b>Опубликовано</b>\n"
        f"Удалено похожих (включая исходный): <b>{removed}</b>"
    )
    await notify_admins(bot, report)
    log.info(f"Опубликовано и удалено {removed} записей.")

async def run_scheduler() -> None:
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    log.info(f"Scheduler TZ={TZ_NAME}, times={','.join(map(str, HOURS))}")

    # бесконечный цикл по расписанию
    while True:
        now_dt = now()
        run_dt = next_run_after(now_dt)
        wait_sec = max((run_dt - now_dt).total_seconds(), 0)
        log.info(f"Следующий пост через {wait_sec/3600:.2f} часов ({human_dt(run_dt)})")
        try:
            await asyncio.sleep(wait_sec)
            await do_one_post(bot)
        except Exception as e:
            log.exception(f"Ошибка при выполнении поста: {e}")
            # чтобы не улететь в быстрый цикл при постоянной ошибке
            await asyncio.sleep(10)

async def main():
    await run_scheduler()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped")
