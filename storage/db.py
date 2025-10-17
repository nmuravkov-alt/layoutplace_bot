import json
import sqlite3
import time
from typing import List, Optional, Tuple, Dict, Any
from config import DB_PATH

def _connect():
    # один коннект на поток; планировщик и хэндлеры могут стучаться параллельно
    cx = sqlite3.connect(DB_PATH, check_same_thread=False)
    cx.row_factory = sqlite3.Row
    return cx

# --- служебка: узнать существующие поля таблицы ---
def _columns(cx: sqlite3.Connection, table: str) -> set[str]:
    cur = cx.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}  # name = col[1]

def _ensure_queue_schema(cx: sqlite3.Connection):
    cur = cx.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT
            -- остальные поля добавляем/проверяем миграциями ниже
        )
    """)
    existing = _columns(cx, "queue")

    # требуемые поля текущей версии
    needed = [
        ("items_json", "TEXT", None),
        ("caption", "TEXT", None),
        ("src_chat_id", "INTEGER", None),
        ("src_msg_id", "INTEGER", None),
        ("created_at", "INTEGER", 0),
    ]

    for name, typ, default in needed:
        if name not in existing:
            if default is None:
                cur.execute(f"ALTER TABLE queue ADD COLUMN {name} {typ}")
            else:
                cur.execute(f"ALTER TABLE queue ADD COLUMN {name} {typ} DEFAULT {default}")

    cx.commit()

def _ensure_albums_cache_schema(cx: sqlite3.Connection):
    cur = cx.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS albums_cache (
            media_group_id TEXT PRIMARY KEY,
            items_json     TEXT NOT NULL,
            updated_at     INTEGER NOT NULL
        )
    """)
    cx.commit()

def init_db():
    cx = _connect()
    try:
        _ensure_queue_schema(cx)
        _ensure_albums_cache_schema(cx)
    finally:
        cx.close()

# ---- API ----
# Формат items_json:
# json.dumps([{"type":"photo","file_id":"..."}, {"type":"video","file_id":"..."}])

def cache_album_upsert(media_group_id: str, items: List[Dict[str, Any]]):
    cx = _connect()
    cur = cx.cursor()
    cur.execute("""
        INSERT INTO albums_cache(media_group_id, items_json, updated_at)
        VALUES(?,?,?)
        ON CONFLICT(media_group_id) DO UPDATE SET
            items_json = excluded.items_json,
            updated_at = excluded.updated_at
    """, (media_group_id, json.dumps(items), int(time.time())))
    cx.commit()
    cx.close()

def cache_album_get(media_group_id: str) -> Optional[List[Dict[str, Any]]]:
    cx = _connect()
    cur = cx.cursor()
    cur.execute("SELECT items_json FROM albums_cache WHERE media_group_id = ?", (media_group_id,))
    row = cur.fetchone()
    cx.close()
    if not row:
        return None
    return json.loads(row[0])

def cache_album_clear():
    cx = _connect()
    cx.execute("DELETE FROM albums_cache")
    cx.commit()
    cx.close()

def enqueue(items: List[Dict[str, Any]], caption: str = "", src: Optional[Tuple[int, int]] = None) -> int:
    src_chat_id, src_msg_id = (src or (None, None))
    cx = _connect()
    cur = cx.cursor()
    cur.execute("""
        INSERT INTO queue(items_json, caption, src_chat_id, src_msg_id, created_at)
        VALUES(?,?,?,?,?)
    """, (json.dumps(items), caption, src_chat_id, src_msg_id, int(time.time())))
    qid = cur.lastrowid
    cx.commit()
    cx.close()
    return qid

def dequeue_oldest() -> Optional[Dict[str, Any]]:
    cx = _connect()
    cur = cx.cursor()
    cur.execute("SELECT id, items_json, caption, src_chat_id, src_msg_id FROM queue ORDER BY id LIMIT 1")
    row = cur.fetchone()
    if not row:
        cx.close()
        return None
    qid, items_json, caption, src_chat_id, src_msg_id = row
    cur.execute("DELETE FROM queue WHERE id = ?", (qid,))
    cx.commit()
    cx.close()
    return {
        "id": qid,
        "items": json.loads(items_json) if items_json else [],
        "caption": caption or "",
        "src_chat_id": src_chat_id,
        "src_msg_id": src_msg_id,
    }

def get_count() -> int:
    cx = _connect()
    cur = cx.cursor()
    cur.execute("SELECT COUNT(*) FROM queue")
    c = int(cur.fetchone()[0])
    cx.close()
    return c
