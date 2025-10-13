import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID", "@layoutplace")
ADMINS = {int(x) for x in os.getenv("ADMINS", "").replace(" ", "").split(",") if x}
TZ = os.getenv("TZ", "Europe/Moscow")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set (.env)")
if not CHANNEL_ID:
    raise RuntimeError("CHANNEL_ID is not set (.env)")
