# runner.py
import asyncio
import inspect

from main import bot, dp
try:
    from main import init_db  # если экспортируешь из main
except ImportError:
    from storage.db import init_db  # иначе берём из storage.db

async def _startup():
    if inspect.iscoroutinefunction(init_db):
        await init_db()
    else:
        init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    print("Starting bot instance...")
    asyncio.run(_startup())
