# storage/db.py
# SQLite-хранилище объявлений + планировщик разовых постов

import os
import re
import sqlite3
import time
from contextlib import contextmanager
from difflib import SequenceMatcher
from typing import Iterable, List, Optional, Tuple, Union, Dict

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

        # Разовые задания (job на конкретный ad_id)
        cx.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
              id            INTEGER PRIMARY KEY AUTOINCREMENT,
              ad_id         INTEGER NOT NULL,
              run_at        INTEGER NOT NULL,
              preview_sent  INTEGER NOT NULL DEFAULT 0,
              created_at    INTEGER NOT NULL
            )
            """
        )
        cx.execute("CREATE INDEX IF NOT EXISTS idx_jobs_run_at ON jobs(run_at)")
        cx.execute("CREATE INDEX IF NOT EXISTS idx_jobs_ad_id ON jobs(ad_id)")

# ---------------- базовые операции по очереди ----------------

def is_duplicate(text: str) -> Optional[int]:
    norm = _normalize(text)
    with _cx() as cx:
        row = cx.execute("SELECT id FROM ads WHERE norm = ? LIMIT 1", (norm,)).fetchone()
        return int(row["id"]) if row else None

def enqueue(text: str) -> int:
    dup = is_duplicate(text)
    if dup:
        return dup
    norm = _normalize(text)
    ts = int(time.time())
    with _cx() as cx:
        cur = cx.execute("INSERT INTO ads(text, norm, created_at) VALUES (?,?,?)", (text.strip(), norm, ts))
        return int(cur.lastrowid)

def get_oldest(
    limit: int = 1, *, count_only: bool = False
) -> Union[int, Optional[Tuple[int, str]], List[Tuple[int, str]]]:
    with _cx() as cx:
        if count_only:
            row = cx.execute("SELECT COUNT(*) AS c FROM ads").fetchone()
            return int(row["c"]) if row else 0
        rows = cx.execute(
            "SELECT id, text FROM ads ORDER BY created_at ASC LIMIT ?",
            (int(limit),),
        ).fetchall()
    items = [(int(r["id"]), str(r["text"])) for r in rows]
    if limit == 1:
        return items[0] if items else None
    return items

def list_queue(limit: int = 10) -> List[Tuple[int, str, int]]:
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
    with _cx() as cx:
        base = cx.execute("SELECT norm FROM ads WHERE id = ?", (int(ad_id),)).fetchone()
        if not base:
            return []
        base_norm = str(base["norm"])
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

# ---------------- разовые задания (jobs) ----------------

def job_create(ad_id: int, run_at_ts: int) -> int:
    with _cx() as cx:
        cur = cx.execute(
            "INSERT INTO jobs(ad_id, run_at, created_at) VALUES (?,?,?)",
            (int(ad_id), int(run_at_ts), int(time.time())),
        )
        return int(cur.lastrowid)

def job_get_next(now_ts: int) -> Optional[Dict]:
    """Следующая job (по времени run_at), которая ещё не выполнена (run_at >= now - 2д)."""
    with _cx() as cx:
        row = cx.execute(
            "SELECT id, ad_id, run_at, preview_sent FROM jobs WHERE run_at >= ? ORDER BY run_at ASC LIMIT 1",
            (int(now_ts) - 172800,),  # -2 суток запас
        ).fetchone()
        if not row:
            return None
        return {
            "id": int(row["id"]),
            "ad_id": int(row["ad_id"]),
            "run_at": int(row["run_at"]),
            "preview_sent": int(row["preview_sent"]),
        }

def job_mark_preview_sent(job_id: int) -> None:
    with _cx() as cx:
        cx.execute("UPDATE jobs SET preview_sent = 1 WHERE id = ?", (int(job_id),))

def job_delete(job_id: int) -> None:
    with _cx() as cx:
        cx.execute("DELETE FROM jobs WHERE id = ?", (int(job_id),))
