import re
from typing import Optional


def normalize_text(raw: Optional[str]) -> str:
    """
    Приводим входной текст к единому виду:
    - вытягиваем название, размер, состояние, цену (если есть)
    - чистим лишние пробелы, двойные переносы
    NOTE: бот не «угадывает» поля, берёт как есть и аккуратно раскладывает.
    """
    raw = (raw or "").strip()
    # Меняем длинные тире, пробелы
    txt = raw.replace("—", "-").replace("–", "-")
    txt = re.sub(r"[ \t]+", " ", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)

    # Пытаемся найти цену вида "Цена - 4 250 ₽" или "Цена: 4250"
    price_line = None
    for line in txt.splitlines():
        if re.search(r"(цена|price)", line, re.IGNORECASE):
            price_line = line.strip()
            break

    # Выделяем хештеги (оставим в конце блока, если есть)
    hashtags = [h for h in re.findall(r"(#[\w\d_]+)", txt, flags=re.UNICODE)]
    hashtags_line = " ".join(sorted(set(hashtags), key=str.lower))

    # Убираем хештеги из основного текста (чтобы не дублировались)
    if hashtags:
        txt = re.sub(r"(#[\w\d_]+)", "", txt).strip()
        txt = re.sub(r"[ \t]+", " ", txt)
        txt = re.sub(r"\n{3,}", "\n\n", txt)

    # Собираем
    lines = [l.strip() for l in txt.splitlines() if l.strip()]
    up_block = "\n".join(lines)
    if price_line and price_line not in lines:
        up_block += ("\n" + price_line)

    if hashtags_line:
        up_block += ("\n\n" + hashtags_line)

    return up_block.strip()


def build_final_caption(user_block: str, album_url: str, contact: str) -> str:
    """
    Итоговая подпись строго в едином стиле. Внизу неизменяемые ссылки.
    """
    user_block = (user_block or "").strip()
    bottom = []
    if album_url:
        bottom.append(f"Общий альбом: {album_url}")
    if contact:
        bottom.append(f"Покупка/вопросы: {contact}")
    tail = "\n".join(bottom)
    if user_block and tail:
        return f"{user_block}\n\n{tail}"
    return user_block or tail or ""
