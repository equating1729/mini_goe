# ml/ingestion/migrate_to_supabase.py
import sqlite3
import psycopg2
import os
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

sqlite_conn = sqlite3.connect(str(BASE_DIR / "data/goe.db"))
sqlite_conn.row_factory = sqlite3.Row
sqlite_cursor = sqlite_conn.cursor()

pg_conn = psycopg2.connect(os.getenv("DATABASE_URL"))
pg_cursor = pg_conn.cursor()

# Init tables
pg_cursor.execute("""
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
pg_cursor.execute("""
    CREATE TABLE IF NOT EXISTS entities (
        id         SERIAL PRIMARY KEY,
        article_id INTEGER,
        text       TEXT,
        label      TEXT
    )
""")
pg_conn.commit()

# Migrate articles
sqlite_cursor.execute("SELECT * FROM articles")
articles = sqlite_cursor.fetchall()
print(f"Migrating {len(articles)} articles...")

for a in articles:
    try:
        pg_cursor.execute("""
            INSERT INTO articles (url, fingerprint, title, body, source, domain, published_at, is_processed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING
        """, (a["url"], a["fingerprint"], a["title"], a["body"],
              a["source"], a["domain"] or "GEO", a["published_at"], a["is_processed"]))
    except Exception as e:
        print(f"Skip: {e}")

pg_conn.commit()

# Migrate entities
sqlite_cursor.execute("SELECT * FROM entities")
entities = sqlite_cursor.fetchall()
print(f"Migrating {len(entities)} entities...")

for e in entities:
    try:
        pg_cursor.execute("""
            INSERT INTO entities (article_id, text, label)
            VALUES (%s, %s, %s)
        """, (e["article_id"], e["text"], e["label"]))
    except Exception as e:
        print(f"Skip: {e}")

pg_conn.commit()
pg_conn.close()
sqlite_conn.close()
print("Migration complete!")