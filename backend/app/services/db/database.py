import sqlite3
from typing import Optional
from app.core.config import settings

def get_connection():
    conn = sqlite3.connect(settings.DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_latest_articles(limit: Optional[int] = None, domain: str = "ALL"):
    """
    Get articles with optional limit.
    - If limit is None: returns ALL articles
    - If limit is a number: returns only that many articles
    - domain filters by specific domain
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    if not domain or domain == "ALL":
        if limit and limit > 0:
            cursor.execute("""
                SELECT id, title, body, source, published_at, url, domain, ingested_at
                FROM articles
                ORDER BY ingested_at DESC
                LIMIT ?
            """, (limit,))
        else:
            # Fetch ALL articles (no limit)
            cursor.execute("""
                SELECT id, title, body, source, published_at, url, domain, ingested_at
                FROM articles
                ORDER BY ingested_at DESC
            """)
    else:
        if limit and limit > 0:
            cursor.execute("""
                SELECT id, title, body, source, published_at, url, domain, ingested_at
                FROM articles
                WHERE domain = ?
                ORDER BY ingested_at DESC
                LIMIT ?
            """, (domain, limit))
        else:
            # Fetch ALL articles for this domain (no limit)
            cursor.execute("""
                SELECT id, title, body, source, published_at, url, domain, ingested_at
                FROM articles
                WHERE domain = ?
                ORDER BY ingested_at DESC
            """, (domain,))
    
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_article_stats(domain: str = "ALL"):
    conn = get_connection()
    cursor = conn.cursor()
    
    # Domain breakdown (always overall)
    cursor.execute("SELECT domain, COUNT(*) as count FROM articles GROUP BY domain")
    domain_breakdown = {row["domain"]: row["count"] for row in cursor.fetchall()}
    for d in ["GEO", "DEFENSE", "TECH", "CLIMATE"]:
        if d not in domain_breakdown:
            domain_breakdown[d] = 0

    if not domain or domain == "ALL":
        cursor.execute("SELECT COUNT(*) as total FROM articles")
        total = cursor.fetchone()["total"]
        cursor.execute("""
            SELECT source, COUNT(*) as count
            FROM articles
            GROUP BY source
            ORDER BY count DESC
        """)
        by_source = [dict(row) for row in cursor.fetchall()]
        
        cursor.execute("""
            SELECT id, title, domain, published_at
            FROM articles
            ORDER BY ingested_at DESC
            LIMIT 5
        """)
        recent = [dict(row) for row in cursor.fetchall()]
        
        try:
            cursor.execute("SELECT COUNT(*) as total FROM entities")
            entities = cursor.fetchone()["total"]
        except sqlite3.OperationalError:
            entities = 0
    else:
        cursor.execute("SELECT COUNT(*) as total FROM articles WHERE domain = ?", (domain,))
        total = cursor.fetchone()["total"]
        cursor.execute("""
            SELECT source, COUNT(*) as count
            FROM articles
            WHERE domain = ?
            GROUP BY source
            ORDER BY count DESC
        """, (domain,))
        by_source = [dict(row) for row in cursor.fetchall()]
        
        cursor.execute("""
            SELECT id, title, domain, published_at
            FROM articles
            WHERE domain = ?
            ORDER BY ingested_at DESC
            LIMIT 5
        """, (domain,))
        recent = [dict(row) for row in cursor.fetchall()]
        
        try:
            cursor.execute("""
                SELECT COUNT(*) as total 
                FROM entities e
                JOIN articles a ON e.article_id = a.id
                WHERE a.domain = ?
            """, (domain,))
            entities = cursor.fetchone()["total"]
        except sqlite3.OperationalError:
            entities = 0
            
    conn.close()
    return {
        "total_articles": total,
        "total_entities": entities,
        "by_source": by_source,
        "domain_breakdown": domain_breakdown,
        "recent": recent,
        "current_domain": domain
    }

def get_all_articles(domain: str = "ALL"):
    """Convenience function to get ALL articles without limit."""
    return get_latest_articles(limit=None, domain=domain)