import os
import asyncio
import logging
from datetime import datetime, timedelta, time as dtime

import pytz  # pip install pytz
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message

# --- логирование ---
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s"
)
log = logging.getLogger("layoutplace_bot")

# --- ENV ---
TOKEN = os.getenv("TOKEN", "")
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("ENV TOKEN пуст или имеет неверный формат. Задайте корректный токен бота.")

TZ = os.getenv("TZ", "Europe/Moscow")
SLOTS = [s.strip() for s in os.getenv("SLOTS", "12:00,16:00,20:00").split(",") if s.strip()]
PREVIEW_MIN = int(os.getenv("PREVIEW_MIN", "45"))

# Админы: "123,456"
ADMINS = [int(x) for x in os.getenv("ADMINS", "").replace(" ", "").split(",") if x]
# Канал числовым id (например -1001758490510)
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1000000000000"))

# --- aiogram core ---
bot = Bot(TOKEN, parse_mode="HTML")
dp = Dispatcher()

# флаг/ссылка на единственный планировщик
_scheduler_task: asyncio.Task | None = None


# ====== Хэндлеры (примеры) ======
@dp.message(Command("ping"))
async def cmd_ping(m: Message):
    await m.answer("pong")


@dp.message(Command("start"))
async def cmd_start(m: Message):
    await m.answer(
        "Бот запущен. Здесь только пример хэндлеров.\n"
        "Главное — см. on_startup: он удаляет webhook и запускает один планировщик."
    )


# ====== Планировщик (пример цикла) ======
def _now_tz():
    return datetime.now(pytz.timezone(TZ))

def _today_slots_dt():
    tz = pytz.timezone(TZ)
    now = _now_tz()
    res = []
    for s in SLOTS:
        hh, mm = (int(x) for x in s.split(":"))
        res.append(tz.localize(datetime(now.year, now.month, now.day, hh, mm)))
    return res

def _next_dt_after(now: datetime, dts: list[datetime]) -> datetime:
    # ближайший слот; если все прошли — завтра первый
    for dt in sorted(dts):
        if dt > now:
            return dt
    # завтра первый из списка
    first = dts[0]
    return first + timedelta(days=1)

async def _scheduler_loop():
    log.info(f"Scheduler TZ={TZ}, times={','.join(SLOTS)}, preview_before={PREVIEW_MIN} min")
    while True:
        try:
            now = _now_tz()
            slots = _today_slots_dt()

            # 1) ближайший «постинг»
            post_dt = _next_dt_after(now, slots)

            # 2) момент превью
            preview_dt = post_dt - timedelta(minutes=PREVIEW_MIN)

            # если превью уже прошло — просто ждём постинг
            if preview_dt > now:
                to_sleep = (preview_dt - now).total_seconds()
                await asyncio.sleep(to_sleep)
                # >>> ТУТ: отправка превью админу(ам) <<<
                for aid in ADMINS:
                    try:
                        await bot.send_message(aid, f"Превью: ближайший постинг в {post_dt.strftime('%H:%M')} ({TZ})")
                    except Exception as e:
                        log.warning(f"Админ {aid} недоступен: {e}")

            # ждём сам постинг
            now = _now_tz()
            if post_dt > now:
                await asyncio.sleep((post_dt - now).total_seconds())

            # >>> ТУТ: основной вызов автопостинга <<<
            # вызовите вашу функцию, которая берёт самый старый таск из БД и постит в канал.
            # пример: await autopost_once()
            log.info(f"Постинг слота {post_dt.strftime('%H:%M')} выполнен (здесь вызовите вашу логику).")

            # небольшой «дыхательный» таймаут
            await asyncio.sleep(1)

        except asyncio.CancelledError:
            log.info("Scheduler task cancelled, выходим из цикла.")
            break
        except Exception as e:
            log.exception(f"Scheduler error: {e}")
            # чтобы не зациклиться на ошибке — подождём и продолжим
            await asyncio.sleep(5)


# ====== хуки старта/остановки ======
async def _on_startup():
    global _scheduler_task
    # СНОСИМ возможный webhook (важно против конфликтов!)
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    # Запускаем РОВНО ОДИН планировщик
    if _scheduler_task is None or _scheduler_task.done():
        _scheduler_task = asyncio.create_task(_scheduler_loop())
        log.info("Scheduler task created.")
    else:
        log.info("Scheduler task уже запущен — пропускаем.")

async def _on_shutdown():
    global _scheduler_task
    if _scheduler_task and not _scheduler_task.done():
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except asyncio.CancelledError:
            pass
        log.info("Scheduler остановлен.")


# ====== точка входа ======
async def run_bot():
    # регистрируем хуки и запускаем polling ОДИН раз
    dp.startup.register(lambda *_: _on_startup())
    dp.shutdown.register(lambda *_: _on_shutdown())
    log.info("Starting bot instance...")
    await dp.start_polling(bot)
