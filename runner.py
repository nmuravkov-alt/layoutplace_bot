# runner.py
import asyncio
import inspect

from main import bot, dp
try:
    # если в main уже экспортируется init_db
    from main import init_db  # type: ignore
except ImportError:
    # иначе берём напрямую из хранилища
    from storage.db import init_db  # type: ignore


async def _startup():
    # Инициализация БД (создаст таблицы, включая queue)
    if inspect.iscoroutinefunction(init_db):
        await init_db()
    else:
        init_db()  # type: ignore

    # Запуск бота
    await dp.start_polling(bot)


if __name__ == "__main__":
    print("Starting bot instance...")
    asyncio.run(_startup())
