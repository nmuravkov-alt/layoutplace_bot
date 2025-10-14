import os

# ========== Основные настройки ==========
TOKEN = os.getenv("BOT_TOKEN", "").strip()              # Токен Telegram-бота
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()        # ID канала или username (@channel)
TZ = os.getenv("TZ", "Europe/Moscow")                   # Часовой пояс

# ========== Админы ==========
# В Railway переменная ADMINS должна быть: 123456789,987654321
ADMINS = [int(a.strip()) for a in os.getenv("ADMINS", "").split(",") if a.strip()]

# ========== База данных (если нужно позже) ==========
DB_PATH = os.getenv("DB_PATH", "data.db")
