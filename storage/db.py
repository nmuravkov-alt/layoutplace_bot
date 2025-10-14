# storage/db.py
import os
import re
import sqlite3
import difflib
from contextlib import contextmanager
from typing import List, Optional, Tuple, Dict, Any

DB_PATH = os.getenv("DB_PATH", "storage/db.sqlite")

@contextmanager
def _cx():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    cx = sqlite3.connect(DB_PATH)
    try:
        yield cx
        cx.commit()
    finally:
        cx.close()

_word_re = re.compile(r"[A-Za-zА-Яа-я0-9]+")

def _normalize(text: str) -> str:
    tokens = _word_re.findall(text.lower())
    return " ".join(tokens)

def _similar(a: str, b: str) -> float:
    return difflib.SequenceMatcher(a=a, b=b).ratio()

# ---------------------- миграции / инициализация ----------------------

async def init_db() -> None:
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

        # таблица планов — для превью/отмены конкретного запланированного поста
        cx.execute(
            """
            CREATE TABLE IF NOT EXISTS planned (
                token TEXT PRIMARY KEY,
                ad_id INTEGER NOT NULL,
                run_at INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'ok',
                created_at INTEGER NOT NULL
            )
            """
        )
        cx.execute("CREATE INDEX IF NOT EXISTS idx_planned_run_at ON planned(run_at)")
        cx.execute("CREATE INDEX IF NOT EXISTS idx_planned_ad ON planned(ad_id)")
        cx.execute("CREATE INDEX IF NOT EXISTS idx_planned_status ON planned(status)")

# -------------------------- очередь объявлений --------------------------

def enqueue(text: str, created_at: int) -> int:
    norm = _normalize(text)
    with _cx() as cx:
        cur = cx.execute(
            "INSERT INTO ads(text, norm, created_at) VALUES(?,?,?)",
            (text, norm, created_at),
        )
        return cur.lastrowid

def get_oldest() -> Optional[Tuple[int, str]]:
    with _cx() as cx:
        row = cx.execute(
            "SELECT id, text FROM ads ORDER BY created_at ASC LIMIT 1"
        ).fetchone()
        return (row[0], row[1]) if row else None

def delete_by_id(ad_id: int) -> None:
    with _cx() as cx:
        cx.execute("DELETE FROM ads WHERE id = ?", (ad_id,))

def find_similar_ids(ad_id: int, threshold: float = 0.8) -> List[int]:
    with _cx() as cx:
        row = cx.execute("SELECT text, norm FROM ads WHERE id = ?", (ad_id,)).fetchone()
        if not row:
            return []
        text, norm = row
        # пройдёмся по другим объявлениям
        rows = cx.execute(
            "SELECT id, text, norm FROM ads WHERE id <> ?", (ad_id,)
        ).fetchall()
    result: List[int] = []
    for rid, _t, _n in rows:
        # быстрый отсев по нормализованной строке
        if _n == norm:
            result.append(rid)
            continue
        if _similar(text, _t) >= threshold:
            result.append(rid)
    return result

def bulk_delete(ids: List[int]) -> None:
    if not ids:
        return
    with _cx() as cx:
        cx.executemany("DELETE FROM ads WHERE id = ?", [(i,) for i in ids])

# ------------------------------ planned API ------------------------------

def plan_create(token: str, ad_id: int, run_at: int, created_at: int) -> None:
    with _cx() as cx:
        cx.execute(
            "INSERT OR REPLACE INTO planned(token, ad_id, run_at, status, created_at) VALUES(?,?,?,?,?)",
            (token, ad_id, run_at, "ok", created_at),
        )

def plan_cancel(token: str) -> bool:
    with _cx() as cx:
        cur = cx.execute(
            "UPDATE planned SET status = 'cancelled' WHERE token = ? AND status <> 'cancelled'",
            (token,),
        )
        return cur.rowcount > 0

def plan_get(token: str) -> Optional[Dict[str, Any]]:
    with _cx() as cx:
        row = cx.execute(
            "SELECT token, ad_id, run_at, status, created_at FROM planned WHERE token = ?",
            (token,),
        ).fetchone()
    if not row:
        return None
    return {
        "token": row[0],
        "ad_id": row[1],
        "run_at": row[2],
        "status": row[3],
        "created_at": row[4],
    }

def plan_clear_old(now_ts: int) -> None:
    # чистим сильно старые записи
    with _cx() as cx:
        cx.execute("DELETE FROM planned WHERE run_at < ? - 86400*7", (now_ts,))
