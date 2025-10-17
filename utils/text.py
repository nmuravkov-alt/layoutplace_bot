# utils/text.py
from config import ALBUM_URL, CONTACT_TEXT

def build_caption(user_caption: str) -> str:
    """
    Единый стиль без эмодзи.
    В конце:
      Общий альбом: <ALBUM_URL>
      Покупка/вопросы: <CONTACT_TEXT>
    """
    user_caption = (user_caption or "").strip()
    footer = []
    if ALBUM_URL:
        footer.append(f"Общий альбом: {ALBUM_URL}")
    if CONTACT_TEXT:
        footer.append(f"Покупка/вопросы: {CONTACT_TEXT}")
    tail = "\n".join(footer)
    if user_caption:
        return f"{user_caption}\n\n{tail}" if tail else user_caption
    else:
        return tail
