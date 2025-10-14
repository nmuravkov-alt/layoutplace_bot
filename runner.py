# runner.py
import asyncio
import logging
import inspect
import sys

from aiogram.exceptions import TelegramConflictError

# Важно: в main.py НЕ должно быть автозапуска polling при импорте
from main import bot, dp, ADMINS, CHANNEL_ID, TZ
from storage.db import init_db

# ---------- логирование ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("runner")


async def _notify_admins(text: str) -> None:
    """Отправка сервисного сообщения всем администраторам в личку."""
    if not ADMINS:
        log.warning("ADMINS пуст — отправить уведомление некому")
        return
    for admin_id in ADMINS:
        try:
            await bot.send_message(admin_id, text, disable_web_page_preview=True)
        except Exception as e:
            log.exception(f"Не удалось отправить администратору {admin_id}: {e}")


async def _safe_close_bot():
    try:
        await bot.session.close()
    except Exception:
        pass


async def _run():
    # Сообщение о старте
    await _notify_admins(
        f"🚀 Бот запускается (канал @{CHANNEL_ID.strip('@')}, TZ={TZ}). "
        f"Если таких уведомлений два — запущены два инстанса."
    )

    # Инициализация БД (поддержка sync/async вариантов)
    if inspect.iscoroutinefunction(init_db):
        await init_db()
    else:
        init_db()

    try:
        # Основной polling
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

    except TelegramConflictError as e:
        # Обнаружен параллельный getUpdates (второй инстанс с тем же токеном)
        msg = (
            "⚠️ Обнаружен двойной запуск бота.\n\n"
            "• Telegram завершил текущий процесс из-за параллельного getUpdates.\n"
            "• Проверь, что бот не запущен локально или на другом сервисе с тем же токеном.\n\n"
            f"Технически: {e.__class__.__name__}: {e}"
        )
        log.error(msg)
        await _notify_admins(msg)
        # Корректно закрываем HTTP-сессию и завершаем процесс БЕЗ ретраев
        await _safe_close_bot()
        sys.exit(0)

    except Exception as e:
        # Любая другая критическая ошибка — уведомим и дадим платформе перезапустить
        msg = f"❌ Критическая ошибка бота: {e}"
        log.exception(msg)
        await _notify_admins(msg)
        await _safe_close_bot()
        raise

    finally:
        # На всякий случай закроем сессию, если сюда попали по нормальному завершению
        await _safe_close_bot()


if __name__ == "__main__":
    asyncio.run(_run())
