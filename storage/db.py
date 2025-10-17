import json
import sqlite3
import time
from typing import List, Optional, Tuple, Dict, Any
from config import DB_PATH

def _connect():
    # Важно: указывать check_same_thread=False, чтобы БД работала и из планировщика, и из хэндлеров
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    cx = _connect()
    cur = cx.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        items_json TEXT NOT NULL,         -- список медиа-элементов (dict), см. формат ниже
        caption TEXT,
        src_chat_id INTEGER,              -- исходный канал (для удаления старого поста)
        src_msg_id INTEGER,               -- исходный message_id
        created_at INTEGER NOT NULL
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS albums_cache (
        media_group_id TEXT PRIMARY KEY,
        items_json TEXT NOT NULL,         -- такой же формат, как в queue.items_json
        updated_at INTEGER NOT NULL
    )
    """)
    cx.commit()
    cx.close()

# ---- Формат items_json ----
# items_json = json.dumps([{"type":"photo","file_id":"..."},
#                          {"type":"video","file_id":"..."},
#                          {"type":"document","file_id":"..."}])

def cache_album_upsert(media_group_id: str, items: List[Dict[str, Any]]):
    cx = _connect()
    cur = cx.cursor()
    cur.execute("""
        INSERT INTO albums_cache(media_group_id, items_json, updated_at)
        VALUES(?,?,?)
        ON CONFLICT(media_group_id) DO UPDATE SET items_json = excluded.items_json,
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
        "items": json.loads(items_json),
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
