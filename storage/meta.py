# storage/meta.py
import os, sqlite3, pathlib

DB_PATH = os.getenv("DB_PATH", "storage/db.sqlite")
pathlib.Path(os.path.dirname(DB_PATH)).mkdir(parents=True, exist_ok=True)

def _conn():
    return sqlite3.connect(DB_PATH)

def _init():
    with _conn() as c:
        c.execute(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)"
        )
_init()

def set_meta(key: str, value: str) -> None:
    with _conn() as c:
        c.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )

def get_meta(key: str, default=None):
    with _conn() as c:
        cur = c.execute("SELECT value FROM meta WHERE key=?", (key,))
        row = cur.fetchone()
        return row[0] if row else default
