# storage/db.py
# SQLite-хранилище объявлений + утилиты

import os
import re
import sqlite3
import time
from contextlib import contextmanager
from difflib import SequenceMatcher
from typing import Iterable, List, Optional, Tuple, Union

DB_PATH = os.getenv("DB_PATH", "storage/db.sqlite")

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

_word_re = re.compile(r"[A-Za-zА-Яа-я0-9]+")

def _normalize(text: str) -> str:
    # убрать лишние пробелы, привести к нижнему, оставить буквы/цифры
    text = re.sub(r"\s+", " ", (text or "")).strip()
    tokens = _word_re.findall(text.lower())
    return " ".join(tokens)

def _similar(a: str, b: str) -> float:
    return SequenceMatcher(a=a, b=b).ratio()

def init_db() -> None:
    with _cx() as cx:
        cx.execute(
            """
            CREATE TABLE IF NOT EXISTS ads (
              id         INTEGER PRIMARY KEY AUTOINCREMENT,
              text       TEXT NOT NULL,
              norm       TEXT NOT NULL,
              created_at INTEGER NOT NULL
            )
            """
        )
        cx.execute("CREATE INDEX IF NOT EXISTS idx_ads_created ON ads(created_at)")
        cx.execute("CREATE INDEX IF NOT EXISTS idx_ads_norm ON ads(norm)")

# ---- базовые операции ----

def is_duplicate(text: str) -> Optional[int]:
    """Если в базе есть запись с таким же norm — вернуть её id, иначе None."""
    norm = _normalize(text)
    with _cx() as cx:
        row = cx.execute("SELECT id FROM ads WHERE norm = ? LIMIT 1", (norm,)).fetchone()
        return int(row["id"]) if row else None

def enqueue(text: str) -> int:
    """Добавить объявление (если дубль — вернуть существующий id)."""
    dup = is_duplicate(text)
    if dup:
        return dup
    norm = _normalize(text)
    ts = int(time.time())
    with _cx() as cx:
        cur = cx.execute(
            "INSERT INTO ads(text, norm, created_at) VALUES (?,?,?)",
            (text.strip(), norm, ts),
        )
        return int(cur.lastrowid)

def get_oldest(
    limit: int = 1,
    *,
    count_only: bool = False
) -> Union[int, Optional[Tuple[int, str]], List[Tuple[int, str]]]:
    """Старейшие объявления; count_only — вернуть количество."""
    with _cx() as cx:
        if count_only:
            row = cx.execute("SELECT COUNT(*) AS c FROM ads").fetchone()
            return int(row["c"]) if row else 0
        rows = cx.execute(
            "SELECT id, text FROM ads ORDER BY created_at ASC LIMIT ?",
            (limit,),
        ).fetchall()
        items = [(int(r["id"]), str(r["text"])) for r in rows]
        return items[0] if limit == 1 else items

def list_queue(limit: int = 10) -> List[Tuple[int, str, int]]:
    """Вернуть список (id, text, created_at) по старшинству."""
    with _cx() as cx:
        rows = cx.execute(
            "SELECT id, text, created_at FROM ads ORDER BY created_at ASC LIMIT ?",
            (int(limit),),
        ).fetchall()
        return [(int(r["id"]), str(r["text"]), int(r["created_at"])) for r in rows]

def delete_by_id(ad_id: int) -> int:
    with _cx() as cx:
        cur = cx.execute("DELETE FROM ads WHERE id = ?", (int(ad_id),))
        return int(cur.rowcount)

def bulk_delete(ids: Iterable[int]) -> int:
    ids = list({int(i) for i in ids})
    if not ids:
        return 0
    placeholders = ",".join("?" for _ in ids)
    with _cx() as cx:
        cur = cx.execute(f"DELETE FROM ads WHERE id IN ({placeholders})", ids)
        return int(cur.rowcount)

def find_similar_ids(ad_id: int, *, threshold: float = 0.88) -> List[int]:
    """Найти похожие к ad_id по norm/similarity (без самого ad_id)."""
    with _cx() as cx:
        base = cx.execute("SELECT norm FROM ads WHERE id = ?", (int(ad_id),)).fetchone()
        if not base:
            return []
        base_norm = str(base["norm"])
        # быстрый грубый фильтр: ищем записи, где есть хотя бы одно общее слово
        first_token = base_norm.split(" ")[0] if base_norm else ""
        if first_token:
            like = f"%{first_token}%"
            cand = cx.execute(
                "SELECT id, norm FROM ads WHERE id <> ? AND norm LIKE ?",
                (int(ad_id), like),
            ).fetchall()
        else:
            cand = cx.execute(
                "SELECT id, norm FROM ads WHERE id <> ?",
                (int(ad_id),),
            ).fetchall()
    out: List[int] = []
    for r in cand:
        nid, nn = int(r["id"]), str(r["norm"])
        if _similar(base_norm, nn) >= threshold:
            out.append(nid)
    return out
