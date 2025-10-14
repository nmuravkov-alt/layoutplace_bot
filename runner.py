# runner.py
import asyncio
import logging
import inspect

from aiogram.exceptions import TelegramConflictError

from main import bot, dp, ADMINS, CHANNEL_ID, TZ  # берём объекты и конфиг из main.py
from storage.db import init_db

# ------------ логирование ------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("runner")


async def _notify_admins(text: str) -> None:
    """Отправить сервисное сообщение всем администраторам в личку."""
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, text)
        except Exception as e:
            log.exception(f"Не удалось отправить администратору {admin_id}: {e}")


async def _run():
    # Сообщаем о старте
    await _notify_admins(
        f"🚀 Бот запускается (канал @{CHANNEL_ID.strip('@')}, TZ={TZ}). "
        f"Если таких уведомлений два — вероятен двойной запуск."
    )

    try:
        # --- Инициализация БД (поддержка sync и async вариантов) ---
        if inspect.iscoroutinefunction(init_db):
            await init_db()
        else:
            init_db()

        # --- Старт polling ---
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

    except TelegramConflictError as e:
        # Конфликт: параллельный getUpdates (второй инстанс с тем же токеном)
        msg = (
            "⚠️ Обнаружен двойной запуск бота.\n\n"
            "• Telegram завершил текущий процесс из-за параллельного getUpdates.\n"
            "• Проверь, что бот не запущен локально или на другом сервисе с тем же токеном.\n\n"
            f"Технически: {e.__class__.__name__}: {e}"
        )
        log.error(msg)
        await _notify_admins(msg)
        raise  # пусть платформа перезапустит контейнер

    finally:
        # Корректно закрываем HTTP-сессию, чтобы не было "Unclosed client session"
        try:
            await bot.session.close()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(_run())
