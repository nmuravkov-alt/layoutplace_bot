import sqlite3, json, time, logging
from typing import List, Dict, Optional, Tuple
from config import DB_PATH

log = logging.getLogger("db")

def _connect():
    cx = sqlite3.connect(DB_PATH)
    cx.row_factory = sqlite3.Row
    return cx

def _table_info_has(col_names, name):
    return any(c["name"] == name for c in col_names)

def init_db():
    cx = _connect()
    try:
        cur = cx.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='queue'")
        exists = cur.fetchone() is not None
        if not exists:
            cx.execute("""
                CREATE TABLE queue(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    items_json TEXT NOT NULL,
                    caption TEXT,
                    src_chat_id INTEGER,
                    src_msg_id INTEGER,
                    created_at INTEGER
                )
            """)
            cx.execute("""
                CREATE TABLE IF NOT EXISTS meta(
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            cx.commit()
            return

        # миграция со старой схемы (payload -> items_json)
        info = cx.execute("PRAGMA table_info(queue)").fetchall()
        names = [dict(r) for r in info]
        if _table_info_has(names, "payload") and not _table_info_has(names, "items_json"):
            log.warning("DB migrate: payload -> items_json")
            cx.execute("ALTER TABLE queue RENAME TO queue_old")
            cx.execute("""
                CREATE TABLE queue(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    items_json TEXT NOT NULL,
                    caption TEXT,
                    src_chat_id INTEGER,
                    src_msg_id INTEGER,
                    created_at INTEGER
                )
            """)
            # переливаем
            old = cx.execute("SELECT id, payload, caption, src_chat_id, src_msg_id, created_at FROM queue_old")
            for r in old:
                items = json.loads(r["payload"]) if r["payload"] else []
                cx.execute("""
                    INSERT INTO queue(id, items_json, caption, src_chat_id, src_msg_id, created_at)
                    VALUES(?,?,?,?,?,?)
                """, (r["id"], json.dumps(items), r["caption"], r["src_chat_id"], r["src_msg_id"], r["created_at"]))
            cx.execute("DROP TABLE queue_old")
            cx.execute("CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT)")
            cx.commit()
    finally:
        cx.close()

def get_count() -> int:
    cx = _connect()
    try:
        return cx.execute("SELECT COUNT(*) FROM queue").fetchone()[0]
    finally:
        cx.close()

def enqueue(items: List[Dict], caption: str, src: Optional[Tuple[int,int]]) -> int:
    src_chat_id, src_msg_id = (src or (None, None))
    cx = _connect()
    try:
        cur = cx.execute("""
            INSERT INTO queue(items_json, caption, src_chat_id, src_msg_id, created_at)
            VALUES(?,?,?,?,?)
        """, (json.dumps(items), caption, src_chat_id, src_msg_id, int(time.time())))
        cx.commit()
        return cur.lastrowid
    finally:
        cx.close()

def dequeue_oldest():
    cx = _connect()
    try:
        row = cx.execute("""
            SELECT id, items_json, caption, src_chat_id, src_msg_id
            FROM queue ORDER BY id LIMIT 1
        """).fetchone()
        if not row:
            return None
        cx.execute("DELETE FROM queue WHERE id=?", (row["id"],))
        cx.commit()
        return {
            "id": row["id"],
            "items": json.loads(row["items_json"]) if row["items_json"] else [],
            "caption": row["caption"] or "",
            "src": (row["src_chat_id"], row["src_msg_id"]) if row["src_chat_id"] else None,
        }
    finally:
        cx.close()

def peek_oldest():
    cx = _connect()
    try:
        row = cx.execute("""
            SELECT id, items_json, caption FROM queue ORDER BY id LIMIT 1
        """).fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "items": json.loads(row["items_json"]) if row["items_json"] else [],
            "caption": row["caption"] or "",
        }
    finally:
        cx.close()

def meta_get(key: str) -> Optional[str]:
    cx = _connect()
    try:
        r = cx.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return r["value"] if r else None
    finally:
        cx.close()

def meta_set(key: str, value: str):
    cx = _connect()
    try:
        cx.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                   (key, value))
        cx.commit()
    finally:
        cx.close()
