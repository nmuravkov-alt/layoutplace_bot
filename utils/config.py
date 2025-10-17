import os

def _bool(x: str, default=False) -> bool:
    if x is None:
        return default
    return x.strip().lower() in {"1", "true", "yes", "y", "on"}

TOKEN        = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID   = int(os.getenv("CHANNEL_ID", "-1000000000000"))   # -100...
ADMINS       = [int(x) for x in os.getenv("ADMINS", "").split(",") if x.strip()]
TZ           = os.getenv("TZ", "Europe/Moscow")
SLOTS_CSV    = os.getenv("SLOTS", "12:00,16:00,20:00")
PREV_MIN     = int(os.getenv("PREVIEW_BEFORE_MIN", "45"))

ALBUM_URL    = os.getenv("ALBUM_URL", "").strip()
CONTACT_TEXT = os.getenv("CONTACT_TEXT", "@layoutplacebuy").strip()

DB_PATH      = os.getenv("DB_PATH", "/data/data.db")

# новенькое
AUTO_POST            = _bool(os.getenv("AUTO_POST", "1"))   # включён по умолчанию
CATCH_UP_MISSED      = _bool(os.getenv("CATCH_UP_MISSED", "0"))  # не «догоняем»
POST_WINDOW_SECONDS  = int(os.getenv("POST_WINDOW_SECONDS", "60"))  # окно +-60с на срабатывание

# безопасность/валидация
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("BOT_TOKEN пустой/некорректный")
if not ADMINS:
    raise RuntimeError("ADMINS не задан (comma-separated user ids)")
