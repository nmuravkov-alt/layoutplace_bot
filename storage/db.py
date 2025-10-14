# storage/db.py
import sqlite3
import time
import os
import re
import difflib
from contextlib import contextmanager
from typing import Iterable, Optional, Dict, Any, List

DB_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(DB_DIR, "queue.sqlite3")

@contextmanager
def _cx():
    os.makedirs(DB_DIR, exist_ok=True)
    cx = sqlite3.connect(DB_PATH)
    cx.row_factory = sqlite3.Row
    try:
        yield cx
        cx.commit()
    finally:
        cx.close()

def _now_ts() -> int:
    return int(time.time())

_word_re = re.compile(r"[A-Za-zА-Яа-я0-9]+")

def _normalize(text: str) -> str:
    tokens = _word_re.findall(text.lower())
    return " ".join(tokens)

def _similar(a: str, b: str) -> float:
    return difflib.SequenceMatcher(a=a, b=b).ratio()

async def init_db():... -> return None:
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

def enqueue(text: str) -> int:
    norm = _normalize(text or "")
    with _cx() as cx:
        cur = cx.execute(
            "INSERT INTO ads(text, norm, created_at) VALUES(?,?,?)",
            (text, norm, _now_ts()),
        )
        return int(cur.lastrowid)

def get_oldest() -> Optional[Dict[str, Any]]:
    with _cx() as cx:
        row = cx.execute(
            "SELECT id, text, norm, created_at FROM ads ORDER BY created_at ASC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None

def delete_by_id(ad_id: int) -> int:
    with _cx() as cx:
        cur = cx.execute("DELETE FROM ads WHERE id = ?", (int(ad_id),))
        return int(cur.rowcount)

def bulk_delete(ids: Iterable[int]) -> int:
    ids = [int(i) for i in ids]
    if not ids:
        return 0
    q = f"DELETE FROM ads WHERE id IN ({','.join('?' for _ in ids)})"
    with _cx() as cx:
        cur = cx.execute(q, ids)
        return int(cur.rowcount)

def find_similar_ids(text: str, threshold: float = 0.88, exclude_id: Optional[int] = None) -> List[int]:
    target_norm = _normalize(text or "")
    res: List[int] = []
    with _cx() as cx:
        prefix = target_norm[:16]
        rows = cx.execute(
            "SELECT id, norm FROM ads WHERE norm LIKE ? OR norm LIKE ? OR norm LIKE ?",
            (f"{prefix}%", f"% {prefix}%", f"%{prefix}%"),
        ).fetchall()
        if not rows:
            rows = cx.execute("SELECT id, norm FROM ads").fetchall()
        for r in rows:
            _id = int(r["id"])
            if exclude_id is not None and _id == exclude_id:
                continue
            score = _similar(target_norm, r["norm"])
            if score >= threshold:
                res.append(_id)
    return res
