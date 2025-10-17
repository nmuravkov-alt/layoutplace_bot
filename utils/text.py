# utils/text.py
import re
from typing import List, Dict, Tuple, Optional
from config import ALBUM_URL, CONTACT_TEXT

# ----------------- БАЗОВЫЕ УТИЛИТЫ -----------------

_EMOJI_RE = re.compile(
    r"[\u2600-\u27BF"         # разное (символы/значки)
    r"\U0001F300-\U0001FAFF"  # смайлы/пиктограммы
    r"\U0001F1E6-\U0001F1FF"  # флаги
    r"]+", flags=re.UNICODE
)

def _strip_emojis(text: str) -> str:
    return _EMOJI_RE.sub("", text or "")

def _clean_spaces(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\u00A0", " ")           # NBSP -> space
    s = re.sub(r"[ \t]+", " ", s)          # множественные пробелы
    s = re.sub(r"\s*\n\s*", "\n", s)       # пробелы вокруг переносов
    return s.strip()

def _norm_dash(s: str) -> str:
    if not s:
        return ""
    s = s.replace(" – ", " — ")
    s = s.replace(" - ", " — ")
    s = s.replace("–", "—")
    s = re.sub(r"\s*-\s*", " — ", s)
    s = re.sub(r"\s*—\s*", " — ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def _collapse_blank_lines(lines: List[str]) -> List[str]:
    out = []
    for ln in lines:
        if ln.strip() == "":
            if not out or out[-1] == "":
                continue
            out.append("")
        else:
            out.append(ln)
    if out and out[-1] == "":
        out.pop()
    return out

# ----------------- РАЗБОР ПОЛЕЙ -----------------

_FIELD_ALIASES: Dict[str, str] = {
    "размер": "Размер",
    "size": "Размер",
    "состояние": "Состояние",
    "condition": "Состояние",
    "цена": "Цена",
    "price": "Цена",
}
_FIELD_ORDER = ["Размер", "Состояние", "Цена"]

_FIELD_LINE_RE = re.compile(
    r"^\s*(?P<key>[A-Za-zА-Яа-яЁё]+)\s*[:\-—]?\s*(?P<val>.+?)\s*$"
)

def _parse_field_line(ln: str) -> Optional[Tuple[str, str]]:
    m = _FIELD_LINE_RE.match(ln.strip())
    if not m:
        return None
    key_raw = m.group("key").lower()
    key = _FIELD_ALIASES.get(key_raw)
    if not key:
        return None
    val = m.group("val").strip()
    if key == "Цена":
        val = _norm_dash(val)
        # уберём возможный дублирующий «Цена:» внутри значения
        val = re.sub(r"^(цена|price)\s*[:\-—]?\s*", "", val, flags=re.I).strip()
    return key, val

# ----------------- ХВОСТ -----------------

def _ensure_tail(lines: List[str]) -> List[str]:
    had_album = any("Общий альбом:" in ln for ln in lines)
    had_contact = any("Покупка/вопросы:" in ln for ln in lines)

    tail = []
    if not had_album and ALBUM_URL:
        tail.append(f"Общий альбом: {ALBUM_URL}")
    if not had_contact and CONTACT_TEXT:
        tail.append(f"Покупка/вопросы: {CONTACT_TEXT}")

    if tail:
        if lines and lines[-1].strip() != "":
            lines.append("")
        lines.extend(tail)
    return lines

# ----------------- ВЫДЕЛЕНИЕ «ДОСТАВКИ» -----------------

# Триггеры, по которым считаем строку «доставочной»
_SHIP_RE = re.compile(
    r"(?i)\b("
    r"доставка|отправка|самовывоз|ship|shipping|pickup|courier|курьер|"
    r"почта|почтой|cdek|сдэк|boxberry|боксберри|dpd|ems|dhl|fedex|ups|worldwide|по всему миру"
    r")\b"
)

def _split_shipping(lines: List[str]) -> Tuple[List[str], List[str]]:
    """
    Делит произвольные строки на:
      - обычные (non_ship)
      - доставочные (ship)
    """
    non_ship, ship = [], []
    for ln in lines:
        if _SHIP_RE.search(ln):
            ship.append(ln)
        else:
            non_ship.append(ln)
    return non_ship, ship

# ----------------- ОСНОВНОЙ КОНСТРУКТОР -----------------

def build_caption(user_caption: str) -> str:
    """
    Строгий единый стиль:
    1) Заголовок (первая содержательная строка, не являющаяся полем)
    2) Размер
    3) Состояние
    4) Цена
    5) Остальной текст (как есть, но очищенный)
    6) Блок «доставка» (всегда ПЕРЕД хвостом)
    7) Хвост: 'Общий альбом: ...' и 'Покупка/вопросы: ...'
    Без эмодзи, нормализованные тире/пробелы. Идемпотентно.
    """
    text = _clean_spaces(_strip_emojis(user_caption or ""))

    # если пусто — всё равно покажем хвост
    if not text:
        return "\n".join(_ensure_tail([])).strip()

    raw_lines = [ln.strip() for ln in text.split("\n")]
    raw_lines = [ln for ln in raw_lines if ln != ""]

    title: Optional[str] = None
    fields: Dict[str, str] = {}
    leftovers: List[str] = []

    # Разбор
    for ln in raw_lines:
        parsed = _parse_field_line(ln)
        if parsed:
            key, val = parsed
            fields[key] = val
            continue

        ln_clean = _norm_dash(ln)
        if title is None and ln_clean:
            title = ln_clean
        elif ln_clean:
            leftovers.append(ln_clean)

    # Делим «доп. текст» на обычный и доставочный
    non_ship, ship = _split_shipping(leftovers)

    # Сборка в фиксированном порядке
    blocks: List[str] = []
    if title:
        blocks.append(title)

    for key in _FIELD_ORDER:
        if key in fields and fields[key]:
            if key == "Цена":
                blocks.append(f"{key} — {fields[key]}")
            else:
                blocks.append(f"{key}: {fields[key]}")

    # Остальной (не доставочный) текст
    if non_ship:
        if blocks and blocks[-1] != "":
            blocks.append("")
        blocks.extend(non_ship)

    # Блок доставки — ВСЕГДА перед хвостом
    if ship:
        if blocks and blocks[-1] != "":
            blocks.append("")
        # можно схлопнуть в одну строку при желании; пока оставляем как есть
        blocks.extend(ship)

    # Схлопнем лишние пустые
    blocks = _collapse_blank_lines(blocks)

    # Хвост (не дублируем, если уже есть)
    blocks = _ensure_tail(blocks)

    result = "\n".join(blocks).strip()
    return result
