# runner.py
import asyncio
import logging
from aiogram.exceptions import TelegramConflictError

# Импортируем готовые объекты из твоего бота.
# ВАЖНО: main.py НЕ должен запускать polling при импортe (только внутри if __name__ == "__main__")
from main import bot, dp, ADMINS, TZ

log = logging.getLogger("conflict-runner")
logging.basicConfig(level=logging.INFO)

async def _notify_admins(text: str):
    """Отправка личного уведомления всем админам из переменной окружения ADMINS."""
    if not ADMINS:
        log.warning("ADMINS пуст — отправить уведомление некому")
        return
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, text)
        except Exception as e:
            log.exception(f"Не удалось отправить администратору {admin_id}: {e}")

async def _run():
    # Пингуем о старте
    await _notify_admins(f"✅ Бот запускается (TZ={TZ}). Если таких уведомлений два — вероятен двойной запуск.")

    try:
        # Основной цикл polling
        await dp.start_polling(bot)
    except TelegramConflictError as e:
        # Конфликт: второй инстанс того же токена
        msg = (
            "⚠️ Обнаружен двойной запуск бота.\n\n"
            "Telegram завершил текущий процесс из-за параллельного getUpdates.\n"
            "Проверь, что бот не запущен локально/на другом сервисе с тем же токеном.\n\n"
            f"Технически: {e.__class__.__name__}: {e}"
        )
        log.error(msg)
        await _notify_admins(msg)
        # Пробрасываем исключение, чтобы Railway перезапустил контейнер (если включён автоперезапуск)
        raise

if __name__ == "__main__":
    asyncio.run(_run())
