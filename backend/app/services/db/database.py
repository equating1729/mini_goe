import os
import psycopg2
import psycopg2.extras
from app.core.config import settings

def get_connection():
    conn = psycopg2.connect(settings.DATABASE_URL)
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    return conn

DEFENSE_KEYWORDS = ["military", "army", "navy", "defence", "defense", "pentagon",
                     "nato", "missile", "troops", "soldier", "border", "terror"]

def get_latest_articles(limit: int = 20, domain: str = None):
    conn = get_connection()
    cursor = conn.cursor()

    if domain == "DEFENSE":
        cursor.execute("""
            SELECT id, title, source, domain, published_at, url
            FROM articles
            ORDER BY ingested_at DESC
            LIMIT 300
        """)
        rows = cursor.fetchall()
        conn.close()
        filtered = [
            dict(r) for r in rows
            if any(kw in (r["title"] or "").lower() for kw in DEFENSE_KEYWORDS)
        ]
        return filtered[:limit]

    elif domain and domain != "ALL":
        cursor.execute("""
            SELECT id, title, source, domain, published_at, url
            FROM articles
            WHERE domain = %s
            ORDER BY ingested_at DESC
            LIMIT %s
        """, (domain, limit))
    else:
        cursor.execute("""
            SELECT id, title, source, domain, published_at, url
            FROM articles
            ORDER BY ingested_at DESC
            LIMIT %s
        """, (limit,))

    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_article_stats():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as total FROM articles")
    total = cursor.fetchone()["total"]
    cursor.execute("""
        SELECT source, COUNT(*) as count
        FROM articles
        GROUP BY source
        ORDER BY count DESC
    """)
    by_source = [dict(r) for r in cursor.fetchall()]
    cursor.execute("""
        SELECT domain, COUNT(*) as count
        FROM articles
        GROUP BY domain
        ORDER BY count DESC
    """)
    by_domain = [dict(r) for r in cursor.fetchall()]
    cursor.execute("SELECT COUNT(*) as total FROM entities")
    entities = cursor.fetchone()["total"]
    conn.close()
    return {
        "total_articles": total,
        "total_entities": entities,
        "by_source": by_source,
        "by_domain": by_domain
    }