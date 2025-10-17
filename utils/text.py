import re

DASH = " — "

def _clean_lines(text: str) -> list[str]:
    text = (text or "").replace("\r", "")
    lines = [l.strip() for l in text.split("\n")]
    # выбрасываем пустые края
    return [l for l in lines if l]

def _rm_fixed(lines: list[str]) -> list[str]:
    cleaned = []
    for l in lines:
        low = l.lower()
        if "vk.com" in low:          # любые ссылки на vk/альбом
            continue
        if "общий альбом" in low:
            continue
        if "покупка/вопросы" in low:
            continue
        cleaned.append(l)
    return cleaned

def _norm_pair(line: str, key: str, label: str) -> str|None:
    # ключи: состояние/размер/цена
    if re.search(fr"^{key}\s*[:\-–—]", line, flags=re.I):
        val = re.sub(fr"^{key}\s*[:\-–—]\s*", "", line, flags=re.I).strip()
        if not val: return None
        if label == "Цена":
            # чистим ₽ и пробелы/дефисы
            val = re.sub(r"\s*р(уб|\.?)?\s*\.?$", " ₽", val, flags=re.I)
            val = val.replace("руб", "₽").replace("р.", "₽").replace("р ", "₽ ")
        return f"{label}{DASH if label=='Цена' else ' : '}{val}"
    return None

def normalize_caption(original: str, album_url: str, contact_text: str) -> str:
    lines = _clean_lines(original)
    lines = _rm_fixed(lines)

    title = None
    head = []
    tail = []

    # заголовок — первая строка, если она не ключ
    if lines:
        if not re.match(r"^(состояние|размер|цена)\b", lines[0], flags=re.I):
            title = lines[0]; lines = lines[1:]

    # пройдемся и нормализуем известные поля
    state = size = price = None
    hashtags = []
    for l in lines:
        l2 = _norm_pair(l, "состояние", "Состояние")
        if l2: state = l2; continue
        l2 = _norm_pair(l, "размер", "Размер")
        if l2: size = l2; continue
        l2 = _norm_pair(l, "цена", "Цена")
        if l2: price = l2; continue
        if l.startswith("#"): hashtags.append(l)
        else: tail.append(l)

    out = []
    if title: out.append(title)
    if state: out.append(state)
    if size:  out.append(size)
    if price: out.append(price)
    # сначала хэштеги, затем «хвост»
    out += hashtags + tail

    # фикс-блок (не меняется)
    if album_url:
        out.append(f"Общий альбом: {album_url}")
    if contact_text:
        out.append(f"Покупка/вопросы: {contact_text}")

    return "\n".join(out)
