# scheduler.py
import asyncio
import logging
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

from aiogram import Bot
from aiogram.enums import ParseMode

from config import TOKEN as BOT_TOKEN, TZ as TZ_NAME, ADMINS, CHANNEL_ID

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("scheduler")

TZ = ZoneInfo(TZ_NAME or "Europe/Moscow")

# Расписание постинга: 12:00 / 16:00 / 20:00 (локально для TZ)
SLOTS = [dtime(12, 0), dtime(16, 0), dtime(20, 0)]
PREVIEW_BEFORE_MIN = 45


def _now() -> datetime:
    return datetime.now(tz=TZ)


def _next_slot(now: datetime) -> datetime:
    """Вернёт ближайшее время постинга в пределах сегодня/завтра."""
    today = now.date()
    candidates = [datetime.combine(today, t, tzinfo=TZ) for t in SLOTS]
    for dt in candidates:
        if dt > now:
            return dt
    # если все прошли — первый слот завтра
    tomorrow = today + timedelta(days=1)
    return datetime.combine(tomorrow, SLOTS[0], tzinfo=TZ)


async def _notify_admins(bot: Bot, text: str) -> None:
    """Отправка служебного сообщения админам в ЛС.
    Работает только если админ нажал /start боту (иначе Unauthorized)."""
    for aid in ADMINS:
        try:
            await bot.send_message(
                aid,
                text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except Exception as e:
            # Обычно это TelegramUnauthorizedError: админ не нажал /start
            log.warning("Админ %s недоступен (%s). Нажми /start боту в ЛС.", aid, type(e).__name__)


async def run_scheduler():
    bot = Bot(
        token=BOT_TOKEN,
        default=None,  # никаких устаревших аргументов здесь
    )

    log.info(
        "Scheduler  TZ=%s, times=%s, preview_before=%d min",
        TZ.key,
        ",".join(t.strftime("%H:%M") for t in SLOTS),
        PREVIEW_BEFORE_MIN,
    )

    while True:
        now = _now()
        slot = _next_slot(now)
        preview_at = slot - timedelta(minutes=PREVIEW_BEFORE_MIN)

        # Информируем, что запущены и когда следующий слот/превью
        await _notify_admins(
            bot,
            (
                f"⏰ <b>Планировщик на связи</b>\n"
                f"Канал: <code>{CHANNEL_ID}</code>\n"
                f"Часовой пояс: <code>{TZ.key}</code>\n\n"
                f"Следующий слот постинга: <b>{slot:%Y-%m-%d %H:%M}</b>\n"
                f"Превью в: <b>{preview_at:%Y-%m-%d %H:%M}</b>"
            ),
        )

        # Ждём времени превью
        now = _now()
        if preview_at > now:
            sleep_sec = (preview_at - now).total_seconds()
            log.info("Ждём превью %.2f секунд (до %s)", sleep_sec, preview_at)
            await asyncio.sleep(sleep_sec)

        # Превью админам (сам текст поста формирует бот при публикации —
        # тут лишь напоминание, что через 45 минут запостим)
        await _notify_admins(
            bot,
            (
                "🔔 <b>Превью</b>\n"
                f"До постинга осталось <b>{PREVIEW_BEFORE_MIN} минут</b>."
            ),
        )

        # Ждём до самого слота
        now = _now()
        if slot > now:
            sleep_sec = (slot - now).total_seconds()
            log.info("Ждём слот постинга %.2f секунд (до %s)", sleep_sec, slot)
            await asyncio.sleep(sleep_sec)

        # На самом слоте мы не публикуем сами — публикацию делает ваш основной бот
        # (он берёт самый старый пост/репост из очереди).
        # Здесь просто пингуем админов, что «пора».
        await _notify_admins(
            bot,
            "🚀 Время постинга. Бот опубликует следующий элемент очереди.",
        )

        # Переходим к поиску следующего слота (цикл while True)
        # Защита от плотной итерации:
        await asyncio.sleep(1)


async def main():
    try:
        await run_scheduler()
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
