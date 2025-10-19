import os
import asyncio
import logging
from datetime import datetime, timedelta

import pytz
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

# ---------- ЛОГИ ----------
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("layoutplace_bot")

# ---------- ENV ----------
TOKEN = os.getenv("TOKEN", "")
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("ENV TOKEN пуст или имеет неверный формат. Задайте корректный токен бота.")

TZ = os.getenv("TZ", "Europe/Moscow")
SLOTS = [s.strip() for s in os.getenv("SLOTS", "12:00,16:00,20:00").split(",") if s.strip()]
PREVIEW_MIN = int(os.getenv("PREVIEW_MIN", "45"))
ADMINS = [int(x) for x in os.getenv("ADMINS", "").replace(" ", "").split(",") if x]
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1000000000000"))

# ---------- AIOGRAM ----------
bot = Bot(TOKEN, parse_mode="HTML")
dp = Dispatcher()

_scheduler_task: asyncio.Task | None = None


# ---------- УТИЛИТЫ ВРЕМЕНИ ----------
def now_tz() -> datetime:
    return datetime.now(pytz.timezone(TZ))

def today_slots() -> list[datetime]:
    tz = pytz.timezone(TZ)
    n = now_tz()
    res = []
    for s in SLOTS:
        hh, mm = map(int, s.split(":"))
        res.append(tz.localize(datetime(n.year, n.month, n.day, hh, mm)))
    return res

def next_after(now: datetime, dts: list[datetime]) -> datetime:
    for dt in sorted(dts):
        if dt > now:
            return dt
    return sorted(dts)[0] + timedelta(days=1)


# ---------- ПЛАНИРОВЩИК ----------
async def scheduler_loop():
    log.info(f"Scheduler TZ={TZ}, times={','.join(SLOTS)}, preview_before={PREVIEW_MIN} min")
    while True:
        try:
            n = now_tz()
            slots = today_slots()
            post_dt = next_after(n, slots)
            preview_dt = post_dt - timedelta(minutes=PREVIEW_MIN)

            # PREVIEW
            n = now_tz()
            if preview_dt > n:
                await asyncio.sleep((preview_dt - n).total_seconds())
                for aid in ADMINS:
                    try:
                        await bot.send_message(
                            aid,
                            f"Превью: ближайший постинг в {post_dt.strftime('%H:%M')} ({TZ})"
                        )
                    except Exception as e:
                        log.warning(f"Админ {aid} недоступен: {e}")

            # POST
            n = now_tz()
            if post_dt > n:
                await asyncio.sleep((post_dt - n).total_seconds())

            # здесь вызови свою процедуру автопостинга очереди
            # пример: await post_oldest()
            log.info(f"Постинг слота {post_dt.strftime('%H:%M')} — вызовите здесь свою логику автопостинга.")

            await asyncio.sleep(1)

        except asyncio.CancelledError:
            log.info("Scheduler task cancelled")
            break
        except Exception as e:
            log.exception(f"Scheduler error: {e}")
            await asyncio.sleep(5)


# ---------- ХУКИ ----------
async def on_startup():
    global _scheduler_task
    # СНОСИМ webHook ещё раз тут для гарантии
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    if _scheduler_task is None or _scheduler_task.done():
        _scheduler_task = asyncio.create_task(scheduler_loop())
        log.info("Scheduler task created.")
    else:
        log.info("Scheduler уже запущен — пропускаем.")

async def on_shutdown():
    global _scheduler_task
    if _scheduler_task and not _scheduler_task.done():
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except asyncio.CancelledError:
            pass
        log.info("Scheduler stopped.")


# ---------- ХЭНДЛЕРЫ ----------
@dp.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer("Бот запущен. Превью за 45 мин и автопостинг по слотам включены.")

@dp.message(Command("ping"))
async def cmd_ping(m: Message):
    await m.answer("pong")


# ---------- ТОЧКА ВХОДА ----------
async def run_bot():
    # 1) Сносим webhook ДО старта polling — это снимает «Conflict»
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    # 2) Регистрируем async-функции, а НЕ lambda (это и было причиной warning)
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    log.info("Starting bot instance...")
    # 3) Один polling-на-весь-сервис:
    await dp.start_polling(bot, allowed_updates=None)
