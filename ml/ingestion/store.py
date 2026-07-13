import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")

def get_connection():
    conn = psycopg2.connect(DATABASE_URL)
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    return conn

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id           SERIAL PRIMARY KEY,
            url          TEXT UNIQUE,
            fingerprint  TEXT UNIQUE,
            title        TEXT,
            body         TEXT,
            source       TEXT,
            domain       TEXT DEFAULT 'GEO',
            published_at TEXT,
            ingested_at  TIMESTAMP DEFAULT NOW(),
            is_processed INTEGER DEFAULT 0
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS entities (
            id         SERIAL PRIMARY KEY,
            article_id INTEGER REFERENCES articles(id),
            text       TEXT,
            label      TEXT
        )
    """)
    conn.commit()
    conn.close()
    print("Database ready.")

if __name__ == "__main__":
    init_db()