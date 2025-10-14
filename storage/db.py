# storage/db.py
import os
import json
import sqlite3
import time
from contextlib import contextmanager
from difflib import SequenceMatcher
import re

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
    tokens = _word_re.findall((text or "").lower())
    return " ".join(tokens)

def _similar(a: str, b: str) -> float:
    return SequenceMatcher(a=a, b=b).ratio()

# ----------------- Инициализация БД -----------------
def init_db():
    with _cx() as cx:
        cx.execute("""
        CREATE TABLE IF NOT EXISTS ads(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text TEXT NOT NULL,
            norm TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )
        """)
        cx.execute("CREATE INDEX IF NOT EXISTS idx_ads_created ON ads(created_at)")
        cx.execute("CREATE INDEX IF NOT EXISTS idx_ads_norm ON ads(norm)")

        # Очередь копирования из канала
        cx.execute("""
        CREATE TABLE IF NOT EXISTS queue(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_chat_id INTEGER NOT NULL,
            message_ids TEXT NOT NULL,           -- JSON: [int,...]
            caption_override TEXT,               -- если нужно заменить подпись
            status TEXT NOT NULL DEFAULT 'pending',  -- pending|previewed|posted|error
            created_at INTEGER NOT NULL,
            posted_at INTEGER
        )
        """)
        cx.execute("CREATE INDEX IF NOT EXISTS idx_queue_status ON queue(status)")
        cx.execute("CREATE INDEX IF NOT EXISTS idx_queue_created ON queue(created_at)")

# --------------- Старые текстовые объявления (для команды /enqueue) ---------------
def db_enqueue(text: str) -> int:
    now = int(time.time())
    with _cx() as cx:
        cur = cx.execute(
            "INSERT INTO ads(text, norm, created_at) VALUES(?,?,?)",
            (text, _normalize(text), now)
        )
        return cur.lastrowid

def get_oldest(limit: int = 1):
    with _cx() as cx:
        cur = cx.execute("SELECT * FROM ads ORDER BY created_at ASC LIMIT ?", (limit,))
        items = [dict(r) for r in cur.fetchall()]
        if limit == 1:
            return items[0] if items else None
        return items

def delete_by_id(ad_id: int) -> int:
    with _cx() as cx:
        cur = cx.execute("DELETE FROM ads WHERE id = ?", (ad_id,))
        return cur.rowcount

def find_similar_ids(ad_id: int, threshold: float = 0.88):
    with _cx() as cx:
        row = cx.execute("SELECT id, norm FROM ads WHERE id = ?", (ad_id,)).fetchone()
        if not row:
            return []
        norm = row["norm"]
        cur = cx.execute("SELECT id, norm FROM ads WHERE id != ?", (ad_id,))
        result = []
        for r in cur.fetchall():
            if _similar(norm, r["norm"]) >= threshold:
                result.append(r["id"])
        return result

def bulk_delete(ids) -> int:
    if not ids:
        return 0
    ids = list(set(int(x) for x in ids))
    with _cx() as cx:
        q = f"DELETE FROM ads WHERE id IN ({','.join(['?']*len(ids))})"
        cur = cx.execute(q, ids)
        return cur.rowcount

# --------------------- Очередь перепостов из канала ---------------------
def queue_add(source_chat_id: int, message_ids: list[int], caption_override: str | None = None) -> int:
    with _cx() as cx:
        cur = cx.execute(
            "INSERT INTO queue(source_chat_id, message_ids, caption_override, status, created_at) VALUES(?,?,?,?,?)",
            (int(source_chat_id), json.dumps([int(x) for x in message_ids]), caption_override, "pending", int(time.time()))
        )
        return cur.lastrowid

def queue_next_pending():
    with _cx() as cx:
        r = cx.execute("SELECT * FROM queue WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1").fetchone()
        return dict(r) if r else None

def queue_mark_status(qid: int, status: str):
    with _cx() as cx:
        cx.execute("UPDATE queue SET status = ?, posted_at = CASE WHEN ?='posted' THEN ? ELSE posted_at END WHERE id = ?",
                   (status, status, int(time.time()), qid))

def queue_count_pending() -> int:
    with _cx() as cx:
        r = cx.execute("SELECT COUNT(*) c FROM queue WHERE status='pending'").fetchone()
        return int(r["c"]) if r else 0
