import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "../data/goe.db")

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            url          TEXT UNIQUE,
            fingerprint  TEXT UNIQUE,
            title        TEXT,
            body         TEXT,
            source       TEXT,
            published_at TEXT,
            ingested_at  TEXT DEFAULT (datetime('now')),
            is_processed INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()
    print("Database ready.")

if __name__ == "__main__":
    init_db()