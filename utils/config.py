# config.py
import os

# === Токен и канал ===
TOKEN = os.getenv("BOT_TOKEN", "").strip()                 # токен бота
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()           # -100... или @username
TZ = os.getenv("TZ", "Europe/Moscow").strip()              # часовой пояс (IANA)

# === Админы (через запятую, только числовые ID) ===
ADMINS = [int(x) for x in os.getenv("ADMINS", "").split(",") if x.strip()]

# === Расписание ===
# строка вида "12:00,16:00,20:00"
POST_TIMES = [t.strip() for t in os.getenv("POST_TIMES", "12:00,16:00,20:00").split(",") if t.strip()]
# за сколько минут до слота высылать превью в ЛС админам
PREVIEW_BEFORE_MIN = int(os.getenv("PREVIEW_BEFORE_MIN", "45"))

# === Единый стиль (используется нормализатором текста) ===
ALBUM_URL = os.getenv("ALBUM_URL", "https://vk.com/market-222108341?screen=group&section=album_26").strip()
CONTACT_TEXT = os.getenv("CONTACT_TEXT", "@layoutplacebuy").strip()

# === База данных (файл sqlite) ===
# если подключён Railway Volume — лучше использовать /data/data.db
DB_PATH = os.getenv("DB_PATH", "/data/data.db").strip()
