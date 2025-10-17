# storage/db.py
import os
import json
import time
import sqlite3
from typing import Optional, Tuple, List, Dict

DB_PATH = os.getenv("DB_PATH", "/data/bot.sqlite3")

def _ensure_dir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def _connect() -> sqlite3.Connection:
    _ensure_dir(DB_PATH)
    cx = sqlite3.connect(DB_PATH, check_same_thread=False)
    cx.row_factory = sqlite3.Row
    return cx

DESIRED_COLS = ["id","items_json","caption","src_chat_id","src_msg_id","created_at"]

def _columns(cursor) -> List[str]:
    cursor.execute("PRAGMA table_info(queue)")
    return [r["name"] for r in cursor.fetchall()]

def _create_table(cur: sqlite3.Cursor):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS queue(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            items_json TEXT NULL,
            caption TEXT NULL,
            src_chat_id INTEGER NULL,
            src_msg_id INTEGER NULL,
            created_at INTEGER NOT NULL
        )
    """)

def _need_migrate(cols: List[str]) -> bool:
    return sorted(cols) != sorted(DESIRED_COLS)

def _migrate(cur: sqlite3.Cursor, cols: List[str]):
    # Создаём новую таблицу правильной схемы и переносим, что сможем
    cur.execute("""
        CREATE TABLE IF NOT EXISTS queue_new(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            items_json TEXT NULL,
            caption TEXT NULL,
            src_chat_id INTEGER NULL,
            src_msg_id INTEGER NULL,
            created_at INTEGER NOT NULL
        )
    """)
    # Попробуем скопировать данные из старой таблицы, если какие-то поля есть
    src_fields = []
    for f in ["id","items_json","caption","src_chat_id","src_msg_id","created_at"]:
        if f in cols:
            src_fields.append(f)
    if src_fields:
        cur.execute(f"""
            INSERT INTO queue_new({",".join(src_fields)})
            SELECT {",".join(src_fields)} FROM queue
        """)
    cur.execute("DROP TABLE queue")
    cur.execute("ALTER TABLE queue_new RENAME TO queue")

def init_db():
    cx = _connect()
    with cx:
        cur = cx.cursor()
        _create_table(cur)
        cols = _columns(cur)
        if _need_migrate(cols):
            _migrate(cur, cols)
    cx.close()

def enqueue(*, items: Optional[List[Dict]] = None, caption: Optional[str] = None,
            src: Optional[Tuple[int,int]] = None) -> int:
    """
    Кладём в очередь:
      - src=(chat_id,msg_id) если хотим перепост из канала;
      - items=[{type: photo|video, file_id: str}, ...] для собранного медиа;
      - caption=текст подписи/поста (нормализуется в main/scheduler).
    """
    cx = _connect()
    with cx:
        cur = cx.cursor()
        items_json = json.dumps(items) if items else None
        src_chat_id = src[0] if src else None
        src_msg_id = src[1] if src else None
        cur.execute("""
            INSERT INTO queue(items_json, caption, src_chat_id, src_msg_id, created_at)
            VALUES(?,?,?,?,?)
        """, (items_json, caption, src_chat_id, src_msg_id, int(time.time())))
        qid = cur.lastrowid
    cx.close()
    return int(qid)

def dequeue_oldest() -> Optional[Dict]:
    cx = _connect()
    with cx:
        cur = cx.cursor()
        row = cur.execute("""
            SELECT id, items_json, caption, src_chat_id, src_msg_id
            FROM queue ORDER BY id LIMIT 1
        """).fetchone()
        if not row:
            return None
        qid = row["id"]
        cur.execute("DELETE FROM queue WHERE id=?", (qid,))
        items = json.loads(row["items_json"]) if row["items_json"] else []
        src = (row["src_chat_id"], row["src_msg_id"]) if (row["src_chat_id"] and row["src_msg_id"]) else None
        return {
            "id": qid,
            "items": items,
            "caption": row["caption"],
            "src": src
        }

def peek_oldest() -> Optional[Dict]:
    cx = _connect()
    with cx:
        cur = cx.cursor()
        row = cur.execute("""
            SELECT id, items_json, caption, src_chat_id, src_msg_id
            FROM queue ORDER BY id LIMIT 1
        """).fetchone()
    cx.close()
    if not row:
        return None
    items = json.loads(row["items_json"]) if row["items_json"] else []
    src = (row["src_chat_id"], row["src_msg_id"]) if (row["src_chat_id"] and row["src_msg_id"]) else None
    return {
        "id": row["id"],
        "items": items,
        "caption": row["caption"],
        "src": src
    }

def get_count() -> int:
    cx = _connect()
    with cx:
        cur = cx.cursor()
        n = cur.execute("SELECT COUNT(*) AS n FROM queue").fetchone()["n"]
    cx.close()
    return int(n or 0)

def list_queue(limit: int = 10) -> List[Dict]:
    cx = _connect()
    with cx:
        cur = cx.cursor()
        rows = cur.execute("""
            SELECT id, items_json, caption, src_chat_id, src_msg_id, created_at
            FROM queue ORDER BY id LIMIT ?
        """, (int(limit),)).fetchall()
    cx.close()
    out = []
    for r in rows:
        items = json.loads(r["items_json"]) if r["items_json"] else []
        src = (r["src_chat_id"], r["src_msg_id"]) if (r["src_chat_id"] and r["src_msg_id"]) else None
        out.append({
            "id": r["id"],
            "items": items,
            "caption": r["caption"],
            "src": src,
            "created_at": r["created_at"],
        })
    return out

def wipe_queue():
    cx = _connect()
    with cx:
        cur = cx.cursor()
        cur.execute("DELETE FROM queue")
    cx.close()
