import re
from config import ALBUM_URL, CONTACT_TEXT

WHITES = re.compile(r"[ \t]+\n")       # лишние пробелы перед переносом
MULTI_NL = re.compile(r"\n{3,}")       # 3+ переносов подряд

def normalize_caption(raw: str) -> str:
    if not raw:
        raw = ""

    # убираем невидимые/мусорные символы
    txt = raw.replace("\u200b", "").replace("\ufeff", "")

    # немного подчистим лишние пробелы/переводы
    txt = WHITES.sub("\n", txt)
    txt = MULTI_NL.sub("\n\n", txt).strip()

    # Финальный блок (без эмодзи, как просил)
    block = (
        f"\n\n"
        f"Общий альбом: {ALBUM_URL}\n"
        f"Покупка/вопросы: {CONTACT_TEXT}"
    )

    # Если уже есть «Общий альбом»/«Покупка/вопросы» — второй раз не добавляем
    low = txt.lower()
    if "общий альбом" not in low and "покупка/вопросы" not in low:
        txt = f"{txt}{block}"

    return txt[:1024]  # ограничим caption по правилам Telegram MediaGroup
