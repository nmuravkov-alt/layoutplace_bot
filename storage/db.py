import os
import json
import time
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

DB_PATH = os.getenv("DB_PATH", "/data/data.db")

# ---------- low-level ----------

def _connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    cx = sqlite3.connect(DB_PATH)
    cx.row_factory = sqlite3.Row
    return cx

def init_db() -> None:
    cx = _connect()
    with cx:
        cx.execute("""
        CREATE TABLE IF NOT EXISTS queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT NOT NULL,       -- JSON: [{"type":"photo","file_id":"..."}, ...]
            caption TEXT,                -- нормализованный текст
            src_chat_id INTEGER,
            src_msg_id INTEGER,
            created_at INTEGER NOT NULL
        )
        """)
        cx.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)

# ---------- meta helpers ----------

def meta_get(key: str) -> Optional[str]:
    cx = _connect()
    cur = cx.execute("SELECT value FROM meta WHERE key = ?", (key,))
    row = cur.fetchone()
    return row["value"] if row else None

def meta_set(key: str, value: str) -> None:
    cx = _connect()
    with cx:
        cx.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

# last posted message id in channel (for deletion)
def get_last_channel_msg_id() -> Optional[int]:
    v = meta_get("last_channel_msg_id")
    return int(v) if v and v.isdigit() else None

def set_last_channel_msg_id(msg_id: int) -> None:
    meta_set("last_channel_msg_id", str(msg_id))

# ---------- helpers for row shape ----------

def _row_to_task(row: sqlite3.Row) -> Dict[str, Any]:
    """Привести запись к формату, который ждёт main.py."""
    d = dict(row)
    # main.py ждёт ключ items_json
    d["items_json"] = d.get("payload", "[]")
    return d

# ---------- queue API ----------

def enqueue(items: List[Dict[str, Any]], caption: str,
            src: Tuple[Optional[int], Optional[int]]) -> int:
    """Добавить в очередь. items — список dict: {"type": "photo"|"video"|"document", "file_id": "..."}"""
    src_chat_id, src_msg_id = src
    cx = _connect()
    with cx:
        cur = cx.execute("""
            INSERT INTO queue(payload, caption, src_chat_id, src_msg_id, created_at)
            VALUES(?,?,?,?,?)
        """, (json.dumps(items, ensure_ascii=False), caption, src_chat_id, src_msg_id, int(time.time())))
        return cur.lastrowid

def dequeue_oldest() -> Optional[Dict[str, Any]]:
    """Достать и удалить самый старый элемент."""
    cx = _connect()
    cur = cx.execute("SELECT * FROM queue ORDER BY id LIMIT 1")
    row = cur.fetchone()
    if not row:
        return None
    with cx:
        cx.execute("DELETE FROM queue WHERE id = ?", (row["id"],))
    return _row_to_task(row)

# --- совместимость/удобные выборки ---

def peek_oldest() -> Optional[Dict[str, Any]]:
    """Вернуть самый старый элемент без удаления (для превью)."""
    cx = _connect()
    cur = cx.execute("SELECT * FROM queue ORDER BY id LIMIT 1")
    row = cur.fetchone()
    return _row_to_task(row) if row else None

def peek_all() -> List[Dict[str, Any]]:
    cx = _connect()
    cur = cx.execute("SELECT * FROM queue ORDER BY id")
    return [_row_to_task(r) for r in cur.fetchall()]

def get_queue() -> List[Dict[str, Any]]:
    """Алиас под разные версии main.py."""
    return peek_all()

def list_queue() -> List[Dict[str, Any]]:
    """Ещё один алиас — некоторые версии ищут list_queue()."""
    return peek_all()

def stats() -> Dict[str, int]:
    cx = _connect()
    cur = cx.execute("SELECT COUNT(*) AS c FROM queue")
    queued = cur.fetchone()["c"]
    return {"queued": queued}

def get_count() -> int:
    """Ровно то, что ожидает main.py."""
    cx = _connect()
    cur = cx.execute("SELECT COUNT(*) AS c FROM queue")
    return int(cur.fetchone()["c"])

def delete_by_id(qid: int) -> int:
    cx = _connect()
    with cx:
        cur = cx.execute("DELETE FROM queue WHERE id = ?", (qid,))
        return cur.rowcount

def remove_by_id(qid: int) -> int:
    """Алиас имени, которое зовёт main.py."""
    return delete_by_id(qid)

def delete_post(qid: int) -> int:
    """Алиас под старое название из предыдущих версий."""
    return delete_by_id(qid)

def last_id() -> Optional[int]:
    cx = _connect()
    cur = cx.execute("SELECT id FROM queue ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    return row["id"] if row else None

def clear_queue() -> int:
    cx = _connect()
    with cx:
        cur = cx.execute("DELETE FROM queue")
        return cur.rowcount
