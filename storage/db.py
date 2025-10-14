# storage/db.py
# SQLite хранилище объявлений + утилиты поиска похожих

import os
import re
import sqlite3
import time
from contextlib import contextmanager
from difflib import SequenceMatcher
from typing import Iterable, List, Optional, Tuple, Union

DB_PATH = os.getenv("DB_PATH", "storage/db.sqlite")

# ----------------------- соединение с БД -----------------------

@contextmanager
def _cx():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    cx = sqlite3.connect(DB_PATH)
    try:
        cx.row_factory = sqlite3.Row
        yield cx
        cx.commit()
    finally:
        cx.close()

# ----------------------- нормализация текста -------------------

_word_re = re.compile(r"[A-Za-zА-Яа-я0-9]+")

def _normalize(text: str) -> str:
    """Простая нормализация для поиска похожих: слова в нижнем регистре, без знаков."""
    tokens = _word_re.findall(text.lower())
    return " ".join(tokens)

def _similar(a: str, b: str) -> float:
    """Похожесть двух нормализованных строк (0..1)."""
    return SequenceMatcher(a=a, b=b).ratio()

# ----------------------- инициализация БД ----------------------

def init_db() -> None:
    """Создать таблицы/индексы, если их ещё нет."""
    with _cx() as cx:
        cx.execute(
            """
            CREATE TABLE IF NOT EXISTS ads (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                text      TEXT    NOT NULL,
                norm      TEXT    NOT NULL,
                created_at INTEGER NOT NULL
            )
            """
        )
        cx.execute("CREATE INDEX IF NOT EXISTS idx_ads_created ON ads(created_at)")
        cx.execute("CREATE INDEX IF NOT EXISTS idx_ads_norm    ON ads(norm)")

# Полезно, если где-то вызывается через await
async def init_db_async() -> None:
    init_db()

# ----------------------- операции ------------------------------

def enqueue(text: str) -> int:
    """Положить объявление в очередь. Возвращает id."""
    norm = _normalize(text)
    ts = int(time.time())
    with _cx() as cx:
        cur = cx.execute(
            "INSERT INTO ads(text, norm, created_at) VALUES (?, ?, ?)",
            (text, norm, ts),
        )
        return int(cur.lastrowid)

def get_oldest(
    limit: int = 1,
    *,
    count_only: bool = False
) -> Union[int, Optional[Tuple[int, str]], List[Tuple[int, str]]]:
    """
    Получить самое старое объявление (или несколько).
    - count_only=True -> вернуть просто количество записей в очереди (int).
    - limit=1 -> вернуть (id, text) или None.
    - limit>1 -> вернуть список [(id, text), ...].
    """
    with _cx() as cx:
        if count_only:
            row = cx.execute("SELECT COUNT(*) AS c FROM ads").fetchone()
            return int(row["c"]) if row else 0

        rows = cx.execute(
            "SELECT id, text FROM ads ORDER BY created_at ASC LIMIT ?",
            (limit,),
        ).fetchall()

        pairs = [(int(r["id"]), str(r["text"])) for r in rows]
        if limit == 1:
            return pairs[0] if pairs else None
        return pairs

def delete_by_id(ad_id: int) -> int:
    """Удалить объявление по id. Возвращает число удалённых строк (0/1)."""
    with _cx() as cx:
        cur = cx.execute("DELETE FROM ads WHERE id = ?", (ad_id,))
        return int(cur.rowcount)

def find_similar_ids(ad_id: int, *, threshold: float = 0.88) -> List[int]:
    """
    Найти id похожих объявлений по нормализованному тексту.
    Текущий ad_id исключается из выдачи.
    """
    with _cx() as cx:
        row = cx.execute("SELECT norm FROM ads WHERE id = ?", (ad_id,)).fetchone()
        if not row:
            return []
        base = str(row["norm"])

        # Берём кандидатов по грубому фильтру: совпало хотя бы одно слово
        first_token = base.split(" ")[0] if base else ""
        if not first_token:
            candidates = cx.execute("SELECT id, norm FROM ads WHERE id != ?", (ad_id,)).fetchall()
        else:
            like = f"%{first_token}%"
            candidates = cx.execute(
                "SELECT id, norm FROM ads WHERE id != ? AND norm LIKE ?",
                (ad_id, like),
            ).fetchall()

        result: List[int] = []
        for r in candidates:
            nid = int(r["id"])
            sim = _similar(base, str(r["norm"]))
            if sim >= threshold:
                result.append(nid)

        return result

def bulk_delete(ids: Iterable[int]) -> int:
    """Удалить сразу много объявлений. Возвращает число удалённых."""
    ids = list(set(int(i) for i in ids if isinstance(i, (int, str))))
    if not ids:
        return 0
    placeholders = ",".join("?" for _ in ids)
    with _cx() as cx:
        cur = cx.execute(f"DELETE FROM ads WHERE id IN ({placeholders})", ids)
        return int(cur.rowcount)
