import os, sqlite3
from pathlib import Path

DATA_DIR = Path(os.getenv("RAILWAY_VOLUME_MOUNT_PATH", os.getenv("DATA_DIR", "storage")))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "db.sqlite3"

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_conn() as con:
        con.executescript('''
        CREATE TABLE IF NOT EXISTS schedules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text TEXT NOT NULL,
            cta_text TEXT,
            cta_url TEXT,
            run_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'scheduled',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        ''')
