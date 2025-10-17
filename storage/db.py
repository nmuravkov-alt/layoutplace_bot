import os
import json
import sqlite3
import time

DB_DIR = os.path.join(os.getcwd(), "data")
DB_PATH = os.path.join(DB_DIR, "bot.db")

def _connect():
    os.makedirs(DB_DIR, exist_ok=True)
    cx = sqlite3.connect(DB_PATH)
    cx.row_factory = sqlite3.Row
    return cx

def init_db():
    cx = _connect()
    cur = cx.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS queue(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            items_json TEXT NOT NULL,
            caption TEXT,
            src_chat_id INTEGER,
            src_msg_id INTEGER,
            created_at INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta(
            key TEXT PRIMARY KEY,
            val TEXT
        )
    """)
    cx.commit()
    cx.close()

def enqueue(items, caption, src):
    src_chat_id, src_msg_id = (src or (None, None))
    cx = _connect()
    cur = cx.cursor()
    cur.execute(
        "INSERT INTO queue(items_json, caption, src_chat_id, src_msg_id, created_at) VALUES(?,?,?,?,?)",
        (json.dumps(items), caption, src_chat_id, src_msg_id, int(time.time()))
    )
    qid = cur.lastrowid
    cx.commit()
    cx.close()
    return qid

def dequeue_oldest():
    cx = _connect()
    cur = cx.cursor()
    cur.execute("SELECT id, items_json, caption, src_chat_id, src_msg_id FROM queue ORDER BY id LIMIT 1")
    row = cur.fetchone()
    if not row:
        cx.close()
        return None
    cur.execute("DELETE FROM queue WHERE id=?", (row["id"],))
    cx.commit()
    cx.close()
    return {
        "id": row["id"],
        "items": json.loads(row["items_json"]),
        "caption": row["caption"] or "",
        "src": (row["src_chat_id"], row["src_msg_id"]) if row["src_chat_id"] and row["src_msg_id"] else None
    }

def peek_oldest():
    cx = _connect()
    cur = cx.cursor()
    cur.execute("SELECT id, items_json, caption, src_chat_id, src_msg_id FROM queue ORDER BY id LIMIT 1")
    row = cur.fetchone()
    cx.close()
    if not row:
        return None
    return {
        "id": row["id"],
        "items": json.loads(row["items_json"]),
        "caption": row["caption"] or "",
        "src": (row["src_chat_id"], row["src_msg_id"]) if row["src_chat_id"] and row["src_msg_id"] else None
    }

def get_count():
    cx = _connect()
    cur = cx.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM queue")
    c = cur.fetchone()["c"]
    cx.close()
    return int(c)

def set_meta(key, val):
    cx = _connect()
    cur = cx.cursor()
    cur.execute("INSERT INTO meta(key,val) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET val=excluded.val", (key, val))
    cx.commit()
    cx.close()

def get_meta(key, default=None):
    cx = _connect()
    cur = cx.cursor()
    cur.execute("SELECT val FROM meta WHERE key=?", (key,))
    row = cur.fetchone()
    cx.close()
    return row["val"] if row else default
