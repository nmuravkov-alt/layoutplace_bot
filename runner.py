"""
Простой раннер, который стартует main.py как модуль.
Оставлен для совместимости с твоей инфраструктурой Railway.
"""
import asyncio
import logging
from main import _run

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | runner | %(message)s")

if __name__ == "__main__":
    logging.info("Starting bot instance...")
    asyncio.run(_run())
