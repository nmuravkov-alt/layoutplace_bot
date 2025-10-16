import json
import sqlite3
import time
from typing import Any, Dict, Optional, Tuple, List
from config import DB_PATH

def _connect() -> sqlite3.Connection:
    cx = sqlite3.connect(DB_PATH)
    cx.row_factory = sqlite3.Row
    return cx

def init_db() -> None:
    cx = _connect()
    cur = cx.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        payload TEXT NOT NULL,
        created_at INTEGER NOT NULL
    )
    """)
    cx.commit()
    cx.close()

def db_enqueue(entry: Dict[str, Any]) -> int:
    cx = _connect()
    cur = cx.cursor()
    cur.execute(
        "INSERT INTO queue (payload, created_at) VALUES (?, ?)",
        (json.dumps(entry, ensure_ascii=False), int(time.time()))
    )
    cx.commit()
    post_id = cur.lastrowid
    cx.close()
    return post_id

def get_count() -> int:
    cx = _connect()
    cur = cx.cursor()
    cur.execute("SELECT COUNT(*) FROM queue")
    n = cur.fetchone()[0]
    cx.close()
    return int(n)

def get_oldest() -> Optional[Dict[str, Any]]:
    cx = _connect()
    cur = cx.cursor()
    cur.execute("SELECT id, payload FROM queue ORDER BY created_at ASC, id ASC LIMIT 1")
    row = cur.fetchone()
    cx.close()
    if not row:
        return None
    data = json.loads(row["payload"])
    data["_row_id"] = row["id"]
    return data

def pop_oldest() -> Optional[Dict[str, Any]]:
    entry = get_oldest()
    if not entry:
        return None
    row_id = entry.get("_row_id")
    cx = _connect()
    cur = cx.cursor()
    cur.execute("DELETE FROM queue WHERE id = ?", (row_id,))
    cx.commit()
    cx.close()
    return entry

def clear_queue() -> int:
    cx = _connect()
    cur = cx.cursor()
    cur.execute("DELETE FROM queue")
    n = cur.rowcount
    cx.commit()
    cx.close()
    return n

def get_all(limit: int = 50) -> List[Dict[str, Any]]:
    cx = _connect()
    cur = cx.cursor()
    cur.execute("SELECT id, payload, created_at FROM queue ORDER BY created_at ASC, id ASC LIMIT ?", (limit,))
    rows = cur.fetchall()
    cx.close()
    out = []
    for r in rows:
        d = json.loads(r["payload"])
        d["_row_id"] = r["id"]
        d["_created_at"] = r["created_at"]
        out.append(d)
    return out
