# storage/db.py
# Простая обёртка вокруг SQLite для очереди объявлений:
# - init_db()             — создать таблицы/индексы (вызвать один раз при старте)
# - db_enqueue(text)      — положить объявление в очередь, вернуть id
# - get_oldest()          — забрать самое старое объявление (dict | None)
# - delete_by_id(id_)     — удалить объявление по id
# - find_similar_ids(txt) — найти похожие объявления, вернуть список id
# - bulk_delete(ids)      — массовое удаление по списку id

import os
import re
import time
import sqlite3
import difflib
from contextlib import contextmanager
from typing import Iterable, List, Optional, Dict, Any

DB_PATH = os.getenv("DB_PATH", "storage/db.sqlite")

# ------------------------ Вспомогательные ------------------------

@contextmanager
def _cx():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    cx = sqlite3.connect(DB_PATH)
    try:
        cx.row_factory = sqlite3.Row
        yield cx
        cx.commit()
    finally:
        cx.close()

_word_re = re.compile(r"[A-Za-zА-Яа-я0-9]+")

def _normalize(text: str) -> str:
    """Нормализуем текст: берём только «словесные» токены и приводим к нижнему регистру."""
    tokens = _word_re.findall(text.lower())
    return " ".join(tokens)

def _similar(a: str, b: str) -> float:
    """Оценка схожести двух строк от 0.0 до 1.0."""
    return difflib.SequenceMatcher(a=a, b=b).ratio()

# ------------------------ Схема БД ------------------------

def init_db() -> None:
    """Создаёт таблицу объявлений и индексы (идемпотентно)."""
    with _cx() as cx:
        cx.execute(
            """
            CREATE TABLE IF NOT EXISTS ads (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                text       TEXT    NOT NULL,
                norm       TEXT    NOT NULL,
                created_at INTEGER NOT NULL
            );
            """
        )
        cx.execute("CREATE INDEX IF NOT EXISTS idx_ads_created ON ads(created_at);")
        cx.execute("CREATE INDEX IF NOT EXISTS idx_ads_norm    ON ads(norm);")

# ------------------------ Операции ------------------------

def db_enqueue(text: str) -> int:
    """Кладёт объявление в очередь, возвращает его id."""
    norm = _normalize(text)
    created = int(time.time())
    with _cx() as cx:
        cur = cx.execute(
            "INSERT INTO ads (text, norm, created_at) VALUES (?, ?, ?)",
            (text, norm, created),
        )
        return int(cur.lastrowid)

def get_oldest() -> Optional[Dict[str, Any]]:
    """Возвращает самое старое объявление или None."""
    with _cx() as cx:
        row = cx.execute(
            "SELECT id, text, norm, created_at FROM ads ORDER BY created_at ASC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None

def delete_by_id(id_: int) -> None:
    """Удаляет объявление по id."""
    with _cx() as cx:
        cx.execute("DELETE FROM ads WHERE id = ?", (id_,))

def find_similar_ids(text: str, threshold: float = 0.80) -> List[int]:
    """
    Ищет в БД объявления, похожие на переданный текст.
    Возвращает список id, у которых similarity >= threshold.
    """
    norm = _normalize(text)
    result: List[int] = []
    with _cx() as cx:
        # Быстрый грубый отбор по пересечению токенов (через LIKE по первым словам),
        # затем точная проверка через SequenceMatcher.
        tokens = norm.split()
        if not tokens:
            return result

        # Берём несколько «ключевых» токенов для первичного отбора
        like_tokens = tokens[: min(3, len(tokens))]
        where = " OR ".join(["norm LIKE ?"] * len(like_tokens))
        args = [f"%{t}%" for t in like_tokens]

        rows = cx.execute(
            f"SELECT id, text, norm FROM ads WHERE {where} LIMIT 500",
            args,
        ).fetchall()

    for row in rows:
        score = _similar(norm, row["norm"])
        if score >= threshold:
            result.append(int(row["id"]))
    return result

def bulk_delete(ids: Iterable[int]) -> int:
    """Массовое удаление объявлений по списку идентификаторов. Возвращает кол-во удалённых."""
    ids = list({int(x) for x in ids})
    if not ids:
        return 0
    placeholders = ",".join("?" for _ in ids)
    with _cx() as cx:
        cur = cx.execute(f"DELETE FROM ads WHERE id IN ({placeholders})", ids)
        return cur.rowcount
