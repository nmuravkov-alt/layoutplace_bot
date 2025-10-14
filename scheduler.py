# scheduler.py
import asyncio
import logging
import os
import time
from datetime import datetime, timedelta

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from storage.db import (
    init_db,
    get_oldest,
    delete_by_id,
    find_similar_ids,
    bulk_delete,
    plan_create,
    plan_get,
    plan_cancel,   # (может пригодиться в будущем)
    plan_clear_old,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("scheduler")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
ADMINS_RAW = os.getenv("ADMINS", "").strip()
TZ = os.getenv("TZ", "Europe/Moscow")

SCHEDULE_TIMES = os.getenv("SCHEDULE_TIMES", "12,16,20")  # ЧЧ через запятую
PREVIEW_BEFORE_MINUTES = int(os.getenv("PREVIEW_BEFORE_MINUTES", "15"))

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

def _parse_admins(raw: str):
    ids = []
    for p in (raw or "").replace(";", ",").split(","):
        p = p.strip()
        if not p:
            continue
        try:
            ids.append(int(p))
        except ValueError:
            pass
    return ids

ADMINS = _parse_admins(ADMINS_RAW)

def _tznow():
    try:
        import pytz
        tz = pytz.timezone(TZ)
        return datetime.now(tz)
    except Exception:
        return datetime.utcnow()

def _mk_token(run_at_ts: int, ad_id: int) -> str:
    return f"{run_at_ts}:{ad_id}"

async def _notify_admins(text: str, reply_markup=None):
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, text, reply_markup=reply_markup, disable_web_page_preview=True)
        except Exception as e:
            log.warning("notify admin %s failed: %s", admin_id, e)

def _next_run_after(now_dt: datetime):
    hours = []
    for p in SCHEDULE_TIMES.split(","):
        p = p.strip()
        if not p:
            continue
        try:
            h = int(p)
            if 0 <= h < 24:
                hours.append(h)
        except ValueError:
            pass
    if not hours:
        hours = [12, 16, 20]
    hours = sorted(hours)

    # собрать все слоты сегодня/завтра и выбрать ближайший > now
    candidates = []
    today = now_dt.replace(minute=0, second=0, microsecond=0)
    for add in [0, 1, 2]:
        base = today + timedelta(days=add)
        for h in hours:
            candidates.append(base.replace(hour=h))
    for dt in candidates:
        if dt > now_dt:
            return dt
    return candidates[-1]

async def _send_preview(run_at_ts: int, ad_id: int, text: str):
    token = _mk_token(run_at_ts, ad_id)
    plan_create(token, ad_id, run_at_ts, int(time.time()))
    when_str = datetime.fromtimestamp(run_at_ts).strftime("%Y-%m-%d %H:%M:%S")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить пост", callback_data=f"cancel:{token}")]
    ])
    await _notify_admins(
        f"🔔 Предстоящий пост в <b>{when_str}</b>\n"
        f"token: <code>{token}</code>\n\n"
        f"{text}",
        reply_markup=kb,
    )

async def _post_to_channel(ad_id: int, text: str):
    await bot.send_message(CHANNEL_ID, text, disable_web_page_preview=False)
    sims = find_similar_ids(ad_id)
    delete_by_id(ad_id)
    bulk_delete(sims)
    await _notify_admins(f"✅ Опубликовано объявление id={ad_id}. Удалено похожих: <b>{len(sims)}</b>")

async def run_scheduler():
    await init_db()
    log.info("Scheduler  TZ=%s, times=%s, preview_before=%s min", TZ, SCHEDULE_TIMES, PREVIEW_BEFORE_MINUTES)
    while True:
        now = _tznow()
        next_run = _next_run_after(now)
        run_ts = int(next_run.timestamp())
        preview_delta = timedelta(minutes=max(PREVIEW_BEFORE_MINUTES, 0))
        preview_moment = next_run - preview_delta

        # 1) если до превью есть время — спим
        if now < preview_moment:
            to_sleep = (preview_moment - now).total_seconds()
            log.info("Следующий ПРЕВЬЮ через %.2f часов (%s)", to_sleep / 3600, preview_moment)
            await asyncio.sleep(to_sleep)

        # 2) отправляем превью (если есть что постить)
        ad = get_oldest()
        if ad:
            ad_id, text = ad
            await _send_preview(run_ts, ad_id, text)
        else:
            await _notify_admins("ℹ️ Очередь пуста: публиковать нечего.")

        # 3) ждём до фактического времени поста
        now2 = _tznow()
        if now2 < next_run:
            await asyncio.sleep((next_run - now2).total_seconds())

        # 4) в момент X проверяем, не отменили ли
        if ad:
            ad_id, text = ad
            token = _mk_token(run_ts, ad_id)
            plan = plan_get(token)
            if plan and plan["status"] == "cancelled":
                await _notify_admins(
                    f"⏭️ Публикация пропущена по отмене (token <code>{token}</code>, ad_id={ad_id})."
                )
            else:
                await _post_to_channel(ad_id, text)

        # 5) чистим старые план-записи и идём искать следующий слот
        plan_clear_old(int(time.time()))

async def main():
    await run_scheduler()

if __name__ == "__main__":
    asyncio.run(main())
