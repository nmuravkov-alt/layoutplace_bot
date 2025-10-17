# runner.py
import asyncio
import logging

from main import _run as run_bot
from scheduler import run_scheduler

logging.basicConfig(level=logging.INFO)

async def _run():
    # Запускаем бот и планировщик параллельно
    await asyncio.gather(
        run_bot(),
        run_scheduler(),
    )

if __name__ == "__main__":
    asyncio.run(_run())
