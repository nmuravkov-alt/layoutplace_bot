import sqlite3, json, time, os
from typing import Optional, Tuple, List, Dict

DB_PATH = os.getenv("DB_PATH", "/data/data.db")

def _cx():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)

def init_db():
    cx = _cx()
    cur = cx.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS queue(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        src_chat_id INTEGER NOT NULL,
        src_msg_id  INTEGER NOT NULL,
        caption     TEXT,
        created_at  INTEGER NOT NULL
      )
    """)
    cur.execute("""
      CREATE TABLE IF NOT EXISTS meta(
        k TEXT PRIMARY KEY,
        v TEXT
      )
    """)
    cx.commit()
    cx.close()

def enqueue(src_chat_id:int, src_msg_id:int, caption:str|None)->int:
    cx = _cx()
    cur = cx.cursor()
    cur.execute(
      "INSERT INTO queue(src_chat_id,src_msg_id,caption,created_at) VALUES(?,?,?,?)",
      (src_chat_id, src_msg_id, caption, int(time.time()))
    )
    qid = cur.lastrowid
    cx.commit(); cx.close()
    return qid

def dequeue_oldest()->Optional[Tuple[int,int,int,Optional[str]]]:
    cx = _cx(); cur = cx.cursor()
    row = cur.execute(
        "SELECT id,src_chat_id,src_msg_id,caption FROM queue ORDER BY id LIMIT 1"
    ).fetchone()
    if not row:
        cx.close(); return None
    cur.execute("DELETE FROM queue WHERE id=?", (row[0],))
    cx.commit(); cx.close()
    # id, chat, msg, caption
    return (row[0], row[1], row[2], row[3])

def peek_oldest()->Optional[Tuple[int,int,int,Optional[str]]]:
    cx = _cx(); cur = cx.cursor()
    row = cur.execute(
        "SELECT id,src_chat_id,src_msg_id,caption FROM queue ORDER BY id LIMIT 1"
    ).fetchone()
    cx.close()
    if not row: return None
    return (row[0], row[1], row[2], row[3])

def count()->int:
    cx = _cx(); cur = cx.cursor()
    n = cur.execute("SELECT COUNT(*) FROM queue").fetchone()[0]
    cx.close(); return int(n)

def clear():
    cx = _cx(); cur = cx.cursor()
    cur.execute("DELETE FROM queue")
    cx.commit(); cx.close()

# last published id
def get_last_published_id()->Optional[int]:
    cx = _cx(); cur = cx.cursor()
    row = cur.execute("SELECT v FROM meta WHERE k='last_pub_id'").fetchone()
    cx.close()
    return int(row[0]) if row and row[0] else None

def set_last_published_id(msg_id:int):
    cx = _cx(); cur = cx.cursor()
    cur.execute("INSERT INTO meta(k,v) VALUES('last_pub_id',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (str(msg_id),))
    cx.commit(); cx.close()
