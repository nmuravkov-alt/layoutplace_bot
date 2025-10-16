import os

# === Бот / Канал / Часовой пояс ===
TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()  # @username или -100...
TZ = os.getenv("TZ", "Europe/Moscow")

# === Админы (числовые ID через запятую) ===
ADMINS = [int(a.strip()) for a in os.getenv("ADMINS", "").split(",") if a.strip()]

# === Единый стиль: ссылка на общий альбом и контакт ===
ALBUM_URL = os.getenv("ALBUM_URL", "https://vk.com/market-222108341?screen=group&section=album_26").strip()
CONTACT_TEXT = os.getenv("CONTACT_TEXT", "@layoutplacebuy").strip()

# === База (если нужен путь) ===
DB_PATH = os.getenv("DB_PATH", "data.db")
