# runner.py
import asyncio
from main import bot, dp, CHANNEL_ID, TZ  # убрали ADMINS — они теперь внутри config
from config import ADMINS

if __name__ == "__main__":
    print("Starting bot instance...")
    asyncio.run(dp.start_polling(bot))
