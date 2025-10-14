# runner.py
import asyncio
import logging

from aiogram.exceptions import TelegramConflictError

from main import bot, dp, ADMINS, CHANNEL_ID, TZ  # берём готовые объекты и конфиг
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
    # Пингуем о старте
    await _notify_admins(f"🚀 Бот запускается (канал @{CHANNEL_ID.strip('@')}, TZ={TZ}). "
                         f"Если таких уведомлений два — вероятен двойной запуск.")

    try:
        # Подготовим БД и стартуем polling
        await init_db()
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

    except TelegramConflictError as e:
        # Конфликт: второй инстанс бота с тем же токеном
        msg = (
            "⚠️ Обнаружен двойной запуск бота.\n\n"
            "• Telegram завершил текущий процесс из-за параллельного getUpdates.\n"
            "• Проверь, что бот не запущен локально или на другом сервисе с тем же токеном.\n\n"
            f"Технически: {e.__class__.__name__}: {e}"
        )
        log.error(msg)
        await _notify_admins(msg)
        # Пробрасываем исключение — Railway перезапустит контейнер (если включён autorestart)
        raise


if __name__ == "__main__":
    asyncio.run(_run())
