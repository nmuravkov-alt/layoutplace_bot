# utils/text.py
import re
from typing import List
from config import ALBUM_URL, CONTACT_TEXT

# --- утилиты ---------------------------------------------------------------

_EMOJI_RE = re.compile(
    r"[\u2600-\u27BF"         # разное (символы/значки)
    r"\U0001F300-\U0001FAFF"  # смайлы/пиктограммы
    r"\U0001F1E6-\U0001F1FF"  # флаги
    r"]+", flags=re.UNICODE
)

def _strip_emojis(text: str) -> str:
    return _EMOJI_RE.sub("", text)

def _clean_spaces(s: str) -> str:
    # единые пробелы и переносы
    s = s.replace("\u00A0", " ")           # NBSP -> space
    s = re.sub(r"[ \t]+", " ", s)          # множественные пробелы
    s = re.sub(r"\s*\n\s*", "\n", s)       # пробелы вокруг переносов
    s = s.strip()
    return s

def _norm_dash(s: str) -> str:
    # нормализуем тире/дефисы в "Цена — 3 200 ₽" и проч.
    s = s.replace(" - ", " — ").replace("–", "—")
    s = re.sub(r"\s*-\s*", " — ", s)  # любой дефис -> длинное тире с пробелами
    s = re.sub(r"\s*—\s*", " — ", s)  # выравниваем пробелы вокруг тире
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def _ensure_tail(lines: List[str]) -> List[str]:
    had_album = any("Общий альбом:" in ln for ln in lines)
    had_contact = any("Покупка/вопросы:" in ln for ln in lines)

    tail_lines = []
    if not had_album and ALBUM_URL:
        tail_lines.append(f"Общий альбом: {ALBUM_URL}")
    if not had_contact and CONTACT_TEXT:
        tail_lines.append(f"Покупка/вопросы: {CONTACT_TEXT}")

    if tail_lines:
        # добавляем пустую строку перед хвостом, если основная часть не пустая
        core_nonempty = any(ln.strip() for ln in lines)
        if core_nonempty and (len(lines) == 0 or lines[-1].strip() != ""):
            lines.append("")
        lines.extend(tail_lines)
    return lines

def _normalize_fields(lines: List[str]) -> List[str]:
    """
    Приводим часто встречающиеся поля к единому виду:
    - 'Размер: ...'
    - 'Состояние: ...'
    - 'Цена — ...'
    Остальное оставляем как есть.
    """
    out = []
    for raw in lines:
        ln = raw.strip()

        # пропускаем полностью пустые дубл. строки (но одну пустую оставим позже)
        if not ln:
            out.append("")
            continue

        # убираем «Размер - ...», «Размер–...», «Размер — ...»
        m = re.match(r"(?i)^(размер)\s*[:\-—]?\s*(.+)$", ln)
        if m:
            out.append(f"Размер: {m.group(2).strip()}")
            continue

        m = re.match(r"(?i)^(состояние)\s*[:\-—]?\s*(.+)$", ln)
        if m:
            out.append(f"Состояние: {m.group(2).strip()}")
            continue

        # Цена: допускаем варианты "Цена: 3200", "Цена - 3 200 ₽", "price ..."
        m = re.match(r"(?i)^(цена|price)\s*[:\-—]?\s*(.+)$", ln)
        if m:
            val = _norm_dash(m.group(2).strip())
            out.append(f"Цена — {val}")
            continue

        # общая нормализация тире
        out.append(_norm_dash(ln))

    # схлопнем лишние пустые строки (макс. одна подряд)
    cleaned = []
    for ln in out:
        if ln == "" and (not cleaned or cleaned[-1] == ""):
            continue
        cleaned.append(ln)
    return cleaned

# --- основной конструктор подписи ------------------------------------------

def build_caption(user_caption: str) -> str:
    """
    Единый стиль БЕЗ эмодзи и с обязательным хвостом.
    Идемпотентно: если хвост уже есть — второй раз не добавим.
    """
    text = (user_caption or "").strip()
    if not text:
        # даже если пусто — покажем хотя бы хвост (альбом/контакт)
        return "\n".join(_ensure_tail([])).strip()

    text = _strip_emojis(text)
    text = _clean_spaces(text)

    # разбираем на строки
    lines = text.split("\n")

    # нормализуем поля
    lines = _normalize_fields(lines)

    # добавляем хвост (если его ещё нет)
    lines = _ensure_tail(lines)

    # финальный трим и единые переносы
    result = "\n".join(lines).strip()
    return result
