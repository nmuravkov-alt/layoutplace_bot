# storage/db.py
import os
import sqlite3
import time
import json
from typing import Optional, Tuple, List, Dict, Any

_DB_PATH = None

def _connect() -> sqlite3.Connection:
    assert _DB_PATH, "DB not initialized. Call init_db(db_path) first."
    cx = sqlite3.connect(_DB_PATH, check_same_thread=False)
    cx.row_factory = sqlite3.Row
    return cx

def init_db(db_path: Optional[str] = None) -> None:
    """Инициализация БД + мягкие миграции."""
    global _DB_PATH
    _DB_PATH = db_path or os.getenv("DB_PATH", "/data/layoutplace.db")
    os.makedirs(os.path.dirname(_DB_PATH), exist_ok=True)
    cx = _connect()
    try:
        cur = cx.cursor()
        # Базовая таблица очереди
        cur.execute("""
        CREATE TABLE IF NOT EXISTS queue (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            items_json    TEXT    NOT NULL,   -- список медиа-элементов (photo/file_id/тип)
            caption       TEXT    NOT NULL,   -- текст финальной подписи
            src_chat_id   INTEGER,            -- откуда было переслано (если было)
            src_msg_id    INTEGER,            -- id исходного сообщения (если было)
            status        TEXT    NOT NULL DEFAULT 'queued', -- queued|posted|error
            scheduled_at  INTEGER,            -- Unix-время запланированной публикации (опц.)
            created_at    INTEGER  NOT NULL,  -- Unix-время добавления в очередь
            last_error    TEXT                -- последнее сообщение об ошибке (если было)
        );
        """)
        # Простейшая миграция (на случай старых схем)
        _safe_add_column(cur, "queue", "status",       "TEXT NOT NULL DEFAULT 'queued'")
        _safe_add_column(cur, "queue", "scheduled_at", "INTEGER")
        _safe_add_column(cur, "queue", "last_error",   "TEXT")
        cx.commit()
    finally:
        cx.close()

def _safe_add_column(cur: sqlite3.Cursor, table: str, column: str, ddl: str) -> None:
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r["name"] for r in cur.fetchall()]
    if column not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

# ---------------------------
# CRUD для очереди
# ---------------------------

def enqueue(items: List[Dict[str, Any]],
            caption: str,
            src: Optional[Tuple[int, int]] = None,
            scheduled_at: Optional[int] = None) -> int:
    """
    items: [{type:'photo'|'video'|'doc', file_id:'...', ...}, ...]
    caption: финальный текст
    src: (src_chat_id, src_msg_id) если переслано из канала
    scheduled_at: Unix-время, когда постить (опционально)
    """
    cx = _connect()
    try:
        cur = cx.cursor()
        src_chat_id, src_msg_id = (src or (None, None))
        cur.execute("""
            INSERT INTO queue(items_json, caption, src_chat_id, src_msg_id, scheduled_at, created_at)
            VALUES(?,?,?,?,?,?)
        """, (json.dumps(items, ensure_ascii=False), caption, src_chat_id, src_msg_id,
              scheduled_at, int(time.time())))
        cx.commit()
        return cur.lastrowid
    finally:
        cx.close()

def dequeue_oldest() -> Optional[sqlite3.Row]:
    """Достаёт самую старую запись и помечает как posted только вызывающим кодом после успешной публикации."""
    cx = _connect()
    try:
        cur = cx.cursor()
        cur.execute("""
            SELECT * FROM queue
            WHERE status='queued'
            ORDER BY id
            LIMIT 1
        """)
        row = cur.fetchone()
        return row
    finally:
        cx.close()

def mark_posted(qid: int) -> None:
    cx = _connect()
    try:
        cx.execute("UPDATE queue SET status='posted' WHERE id=?", (qid,))
        cx.commit()
    finally:
        cx.close()

def mark_error(qid: int, error_text: str) -> None:
    cx = _connect()
    try:
        cx.execute("UPDATE queue SET status='error', last_error=? WHERE id=?", (error_text, qid))
        cx.commit()
    finally:
        cx.close()

def remove(qid: int) -> None:
    cx = _connect()
    try:
        cx.execute("DELETE FROM queue WHERE id=?", (qid,))
        cx.commit()
    finally:
        cx.close()

def get_count(status: Optional[str] = None) -> int:
    cx = _connect()
    try:
        cur = cx.cursor()
        if status:
            cur.execute("SELECT COUNT(*) AS c FROM queue WHERE status=?", (status,))
        else:
            cur.execute("SELECT COUNT(*) AS c FROM queue")
        return int(cur.fetchone()["c"])
    finally:
        cx.close()

def list_queue(limit: int = 20) -> List[sqlite3.Row]:
    cx = _connect()
    try:
        cur = cx.cursor()
        cur.execute("""
            SELECT id, caption, status, created_at, scheduled_at
            FROM queue
            ORDER BY id
            LIMIT ?
        """, (limit,))
        return cur.fetchall()
    finally:
        cx.close()
