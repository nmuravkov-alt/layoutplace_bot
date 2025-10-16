# runner.py
import asyncio
from main import bot, dp, CHANNEL_ID, TZ   # ADMINS больше не экспортируется из main.py
from config import ADMINS                  # теперь берём из config.py

if __name__ == "__main__":
    print("Starting bot instance...")
    asyncio.run(dp.start_polling(bot))
