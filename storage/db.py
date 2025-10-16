# storage/db.py
import json
import sqlite3
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

from config import DB_PATH  # путь к sqlite задаётся через переменную окружения или в config.py

@contextmanager
def _cx():
    cx = sqlite3.connect(DB_PATH)
    try:
        yield cx
    finally:
        cx.commit()
        cx.close()

def init_db() -> None:
    with _cx() as cx:
        cx.execute("""
        CREATE TABLE IF NOT EXISTS queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text TEXT NOT NULL,
            media_json TEXT,                 -- JSON: список элементов альбома (type, file_id)
            src_chat_id INTEGER,             -- откуда форвардили (для удаления старого)
            src_msg_ids_json TEXT,           -- JSON: список message_id исходного поста/альбома
            created_at INTEGER NOT NULL
        )
        """)
        cx.execute("CREATE INDEX IF NOT EXISTS idx_queue_created ON queue(created_at)")

# --------- Запись в очередь ---------

def enqueue_text(text: str) -> int:
    with _cx() as cx:
        cur = cx.execute(
            "INSERT INTO queue(text, media_json, src_chat_id, src_msg_ids_json, created_at) VALUES (?, NULL, NULL, NULL, ?)",
            (text, int(time.time()))
        )
        return cur.lastrowid

def enqueue_media(
    text: str,
    media: List[Dict[str, str]],
    src_chat_id: Optional[int] = None,
    src_msg_ids: Optional[List[int]] = None,
) -> int:
    with _cx() as cx:
        cur = cx.execute(
            "INSERT INTO queue(text, media_json, src_chat_id, src_msg_ids_json, created_at) VALUES (?, ?, ?, ?, ?)",
            (
                text,
                json.dumps(media, ensure_ascii=False),
                src_chat_id,
                json.dumps(src_msg_ids or [], ensure_ascii=False),
                int(time.time()),
            ),
        )
        return cur.lastrowid

# --------- Чтение / удаление ---------

def get_oldest(limit: int = 1) -> List[Dict[str, Any]]:
    with _cx() as cx:
        cur = cx.execute(
            "SELECT id, text, media_json, src_chat_id, src_msg_ids_json, created_at "
            "FROM queue ORDER BY created_at ASC LIMIT ?",
            (limit,),
        )
        rows = cur.fetchall()
    items: List[Dict[str, Any]] = []
    for r in rows:
        items.append({
            "id": r[0],
            "text": r[1],
            "media": json.loads(r[2]) if r[2] else [],
            "src_chat_id": r[3],
            "src_msg_ids": json.loads(r[4]) if r[4] else [],
            "created_at": r[5],
        })
    return items

def get_count() -> int:
    with _cx() as cx:
        cur = cx.execute("SELECT COUNT(*) FROM queue")
        (n,) = cur.fetchone()
        return int(n)

def delete_by_id(item_id: int) -> None:
    with _cx() as cx:
        cx.execute("DELETE FROM queue WHERE id=?", (item_id,))

def clear_all() -> int:
    with _cx() as cx:
        cur = cx.execute("DELETE FROM queue")
        return cur.rowcount
