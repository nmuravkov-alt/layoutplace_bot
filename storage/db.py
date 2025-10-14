# storage/db.py
import sqlite3
import re
import difflib
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path("storage/db.sqlite")

_word_re = re.compile(r"[A-Za-zА-Яа-я0-9]+")

def _normalize(text: str) -> str:
    """Нормализуем текст — убираем знаки, приводим к нижнему регистру"""
    tokens = _word_re.findall(text.lower())
    return " ".join(tokens)

def _similar(a: str, b: str) -> float:
    """Схожесть строк"""
    return difflib.SequenceMatcher(a=a, b=b).ratio()

@contextmanager
def _cx():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as cx:
        cx.row_factory = sqlite3.Row
        yield cx

def init_db():
    """Создаёт таблицу, если её нет"""
    with _cx() as cx:
        cx.execute(
            """
            CREATE TABLE IF NOT EXISTS ads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                text TEXT NOT NULL,
                norm TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )
            """
        )
        cx.execute("CREATE INDEX IF NOT EXISTS idx_ads_created ON ads(created_at)")
        cx.execute("CREATE INDEX IF NOT EXISTS idx_ads_norm ON ads(norm)")
    print("✅ DB initialized at", DB_PATH)

def enqueue(text: str):
    """Добавить объявление в очередь"""
    norm = _normalize(text)
    with _cx() as cx:
        cx.execute(
            "INSERT INTO ads (text, norm, created_at) VALUES (?, ?, strftime('%s','now'))",
            (text, norm),
        )

def get_oldest():
    """Получить самое старое объявление"""
    with _cx() as cx:
        row = cx.execute("SELECT id, text, norm FROM ads ORDER BY created_at ASC LIMIT 1").fetchone()
        return dict(row) if row else None

def delete_by_id(ad_id: int):
    """Удалить объявление по ID"""
    with _cx() as cx:
        cx.execute("DELETE FROM ads WHERE id=?", (ad_id,))

def find_similar_ids(text: str, threshold: float = 0.5):
    """Найти ID похожих объявлений"""
    norm = _normalize(text)
    with _cx() as cx:
        rows = cx.execute("SELECT id, norm FROM ads").fetchall()
    similar = []
    for r in rows:
        if _similar(norm, r["norm"]) >= threshold:
            similar.append(r["id"])
    return similar

def bulk_delete(ids: list[int]):
    """Удалить несколько объявлений"""
    if not ids:
        return
    with _cx() as cx:
        q = f"DELETE FROM ads WHERE id IN ({','.join(['?'] * len(ids))})"
        cx.execute(q, ids)
