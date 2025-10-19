import os
import json
import time
import sqlite3
from contextlib import closing

DB_DIR = os.getenv("DB_DIR", "data")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "queue.sqlite3")


def _connect():
    cx = sqlite3.connect(DB_PATH, isolation_level=None)
    cx.execute("PRAGMA journal_mode=WAL;")
    cx.execute("PRAGMA synchronous=NORMAL;")
    return cx


def init_db():
    with closing(_connect()) as cx, cx:
        cx.execute("""
        CREATE TABLE IF NOT EXISTS queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            items_json TEXT NOT NULL,     -- список медиа/текста
            caption TEXT,                 -- исходная подпись/текст (сырое)
            src_chat_id INTEGER,          -- откуда переслано
            src_msg_id INTEGER,           -- id исходного сообщения
            created_at INTEGER NOT NULL
        )
        """)
    return True


def enqueue(*, items, caption=None, src=None):
    src_chat_id, src_msg_id = (src or (None, None))
    with closing(_connect()) as cx, cx:
        cur = cx.cursor()
        cur.execute(
            "INSERT INTO queue(items_json, caption, src_chat_id, src_msg_id, created_at) VALUES(?,?,?,?,?)",
            (json.dumps(items, ensure_ascii=False), caption, src_chat_id, src_msg_id, int(time.time()))
        )
        return cur.lastrowid


def dequeue_oldest():
    with closing(_connect()) as cx, cx:
        cur = cx.cursor()
        row = cur.execute(
            "SELECT id, items_json, caption, src_chat_id, src_msg_id FROM queue ORDER BY id LIMIT 1"
        ).fetchone()
        if not row:
            return None
        qid, items_json, caption, src_chat_id, src_msg_id = row
        cur.execute("DELETE FROM queue WHERE id=?", (qid,))
        return {
            "id": qid,
            "items": json.loads(items_json),
            "caption": caption,
            "src": (src_chat_id, src_msg_id) if src_chat_id and src_msg_id else None,
        }


def peek_oldest():
    with closing(_connect()) as cx:
        cur = cx.cursor()
        row = cur.execute(
            "SELECT id, items_json, caption, src_chat_id, src_msg_id FROM queue ORDER BY id LIMIT 1"
        ).fetchone()
        if not row:
            return None
        qid, items_json, caption, src_chat_id, src_msg_id = row
        return {
            "id": qid,
            "items": json.loads(items_json),
            "caption": caption,
            "src": (src_chat_id, src_msg_id) if src_chat_id and src_msg_id else None,
        }


def get_count():
    with closing(_connect()) as cx:
        (cnt,) = cx.execute("SELECT COUNT(*) FROM queue").fetchone()
    return int(cnt)


def get_all(limit=50):
    with closing(_connect()) as cx:
        cur = cx.cursor()
        rows = cur.execute(
            "SELECT id, items_json, caption, src_chat_id, src_msg_id, created_at FROM queue ORDER BY id LIMIT ?",
            (limit,)
        ).fetchall()
    out = []
    for r in rows:
        qid, items_json, caption, src_chat_id, src_msg_id, created_at = r
        out.append({
            "id": qid,
            "items": json.loads(items_json),
            "caption": caption,
            "src": (src_chat_id, src_msg_id) if src_chat_id and src_msg_id else None,
            "created_at": created_at,
        })
    return out
