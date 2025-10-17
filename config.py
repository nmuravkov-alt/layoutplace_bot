import os

# === Бот / Канал / Часовой пояс ===
BOT_TOKEN   = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID  = os.getenv("CHANNEL_ID", "").strip()     # @username или -100...
TZ          = os.getenv("TZ", "Europe/Moscow").strip()

# === Админы (числовые ID через запятую) ===
ADMINS = [int(a.strip()) for a in os.getenv("ADMINS", "").split(",") if a.strip()]

# === Единый стиль: ссылка на общий альбом и контакт ===
ALBUM_URL    = os.getenv("ALBUM_URL", "").strip()
CONTACT_TEXT = os.getenv("CONTACT_TEXT", "@layoutplacebuy").strip()

# === База (путь к файлу БД) — ПЕРЕЕМЕННАЯ ОБЯЗАТЕЛЬНО должна быть /data/data.db ===
DB_PATH = os.getenv("DB_PATH", "/data/data.db").strip()

# === Расписание ===
# Слоты постинга через запятую в формате HH:MM (по TZ)
TIMES = [t.strip() for t in os.getenv("TIMES", "12:00,16:00,20:00").split(",") if t.strip()]
# За сколько минут до слота отправлять превью админам
PREVIEW_BEFORE_MIN = int(os.getenv("PREVIEW_BEFORE_MIN", "45"))
