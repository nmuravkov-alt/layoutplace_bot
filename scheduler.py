# scheduler.py
# Планировщик: 3 раза в день триггерит /post_oldest и уведомляет админа

import asyncio
import logging
import os
from datetime import datetime, time, timedelta
from typing import List

import pytz
from aiogram import Bot
from aiogram.enums import ParseMode

# опционально: храним «когда следующий пост» в мета
try:
    from storage.meta import set_meta  # type: ignore
except Exception:
    def set_meta(*_args, **_kwargs):
        pass


# ------------ Конфиг из ENV ------------

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMINS_RAW = os.getenv("ADMINS", "").strip()
TZ_NAME = os.getenv("TZ", "Europe/Moscow").strip()
SCHEDULE_TIMES = os.getenv("SCHEDULE_TIMES", "12:00,16:00,20:00").strip()

# берём первого админа как основной чат для уведомлений/триггера команды
ADMIN_ID = None
for chunk in ADMINS_RAW.split(","):
    s = chunk.strip()
    if s.isdigit():
        ADMIN_ID = int(s)
        break


# ------------ Вспомогалки ------------

def parse_times(spec: str) -> List[time]:
    """Парсим 'HH:MM,HH:MM,...' → [time, ...]"""
    out: List[time] = []
    for part in spec.split(","):
        p = part.strip()
        if not p:
            continue
        try:
            hh, mm = p.split(":")
            out.append(time(int(hh), int(mm)))
        except Exception:
            logging.warning(f"Skip bad time '{p}' in SCHEDULE_TIMES")
    if not out:
        out = [time(12, 0), time(16, 0), time(20, 0)]
    return sorted(out)


def next_run_dt(now: datetime, times: List[time], tz) -> datetime:
    """Находим ближайшее время запуска c учётом TZ."""
    today = now.date()
    # сперва сегодня
    for t in times:
        candidate = tz.localize(datetime.combine(today, t))
        if candidate > now:
            return candidate
    # иначе завтра первое время
    tomorrow = today + timedelta(days=1)
    return tz.localize(datetime.combine(tomorrow, times[0]))


async def notify_admin(bot: Bot, text: str):
    """Тихо уведомляем админа (если задан)."""
    if not (ADMIN_ID and isinstance(ADMIN_ID, int)):
        return
    try:
        await bot.send_message(ADMIN_ID, text, disable_web_page_preview=True, parse_mode=ParseMode.HTML)
    except Exception as e:
        logging.warning(f"notify_admin failed: {e}")


async def trigger_post(bot: Bot):
    """
    Триггерим публикацию через основной бот:
    посылаем админу команду /post_oldest — дальше обработчик в main.py всё сделает.
    """
    if not (ADMIN_ID and isinstance(ADMIN_ID, int)):
        logging.error("ADMIN_ID is not set; cannot trigger /post_oldest")
        return
    try:
        await bot.send_message(ADMIN_ID, "/post_oldest")
    except Exception as e:
        logging.exception(f"Failed to trigger /post_oldest: {e}")


# ------------ Основной цикл планировщика ------------

async def run_scheduler():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is empty")
    if ADMIN_ID is None:
        logging.warning("ADMINS is empty or invalid — уведомления и триггер /post_oldest не будут работать")

    tz = pytz.timezone(TZ_NAME or "Europe/Moscow")
    times = parse_times(SCHEDULE_TIMES)

    logging.info(f"Scheduler TZ={tz.zone}, times={','.join(t.strftime('%H:%M') for t in times)}")
    bot = Bot(BOT_TOKEN, parse_mode=ParseMode.HTML)

    try:
        while True:
            now = datetime.now(tz)
            nxt = next_run_dt(now, times, tz)

            # сохраним в мета и уведомим
            try:
                set_meta("next_post_at", nxt.isoformat())
            except Exception:
                pass

            hours_left = (nxt - now).total_seconds() / 3600.0
            logging.info(f"Следующий пост через {hours_left:.2f} часов ({nxt.strftime('%Y-%m-%d %H:%M:%S %Z')})")
            await notify_admin(
                bot,
                f"🗓 Следующий пост в <b>{nxt.strftime('%Y-%m-%d %H:%M:%S %Z')}</b>\n"
                f"(через ~{hours_left:.2f} ч)"
            )

            # спим до времени публикации
            await asyncio.sleep(max(1, int((nxt - datetime.now(tz)).total_seconds())))

            # попытка публикации
            try:
                await trigger_post(bot)
                await notify_admin(bot, "✅ Запрос на публикацию отправлен (/post_oldest).")
            except Exception as e:
                logging.exception(f"Post failed: {e}")
                await notify_admin(bot, f"❌ Ошибка публикации: <code>{e}</code>")

            # маленькая пауза, чтобы не схлопнуться при одинаковых временах
            await asyncio.sleep(2)
    finally:
        await bot.session.close()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    asyncio.run(run_scheduler())


if __name__ == "__main__":
    main()
