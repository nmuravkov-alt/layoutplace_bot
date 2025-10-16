# utils/config.py (или просто config.py в корне — как у тебя подключено)
import os

def _as_int_or_str(v: str):
    """
    CHANNEL_ID может быть @username (строка) или -100... (int).
    Возвращаем int если это число, иначе оставляем строкой.
    """
    s = (v or "").strip()
    if s.startswith("-") and s[1:].isdigit():
        try:
            return int(s)
        except Exception:
            return s
    return s

def _parse_admins(v: str):
    """
    ADMINS — список числовых ID через запятую.
    """
    ids = []
    for a in (v or "").split(","):
        a = a.strip()
        if a.isdigit():
            ids.append(int(a))
    return ids

# === Бот / Канал / Часовой пояс ===
TOKEN       = os.getenv("BOT_TOKEN", "").strip()
CHANNEL_ID  = _as_int_or_str(os.getenv("CHANNEL_ID", ""))     # @username или -100...
TZ          = os.getenv("TZ", "Europe/Moscow").strip()

# === Админы (числовые ID через запятую) ===
ADMINS      = _parse_admins(os.getenv("ADMINS", ""))

# === Единый стиль: ссылка на общий альбом и контакт ===
ALBUM_URL    = os.getenv("ALBUM_URL", "https://vk.com/market-222108341?screen=group&section=album_26").strip()
CONTACT_TEXT = os.getenv("CONTACT_TEXT", "@layoutplacebuy").strip()

# === Путь к базе (необязательно менять) ===
DB_PATH      = os.getenv("DB_PATH", "data.db").strip()
