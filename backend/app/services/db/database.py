import os
import re
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

def get_article_stats(domain="ALL"):
    conn = get_connection()
    cursor = conn.cursor()

    if domain == "ALL":
        cursor.execute("""
            SELECT COUNT(*) as total
            FROM articles
        """)
    else:
        cursor.execute("""
            SELECT COUNT(*) as total
            FROM articles
            WHERE domain = %s
        """, (domain,))

    total = cursor.fetchone()["total"]

    if domain == "ALL":
        cursor.execute("""
            SELECT source, COUNT(*) as count
            FROM articles
            GROUP BY source
            ORDER BY count DESC
        """)
    else:
        cursor.execute("""
            SELECT source, COUNT(*) as count
            FROM articles
            WHERE domain = %s
            GROUP BY source
            ORDER BY count DESC
        """, (domain,))

    by_source = [dict(r) for r in cursor.fetchall()]


    if domain == "ALL":
        cursor.execute("""
            SELECT domain, COUNT(*) as count
            FROM articles
            GROUP BY domain
            ORDER BY count DESC
        """)
    else:
        cursor.execute("""
            SELECT domain, COUNT(*) as count
            FROM articles
            WHERE domain = %s
            GROUP BY domain
            ORDER BY count DESC
        """, (domain,))

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

STOP_WORDS = {
    "the", "and", "for", "are", "was", "what", "why", "how", "who", "about",
    "between", "relation", "relations", "with", "this", "that", "from", "is",
    "not", "does", "did", "has", "have", "can", "will", "into", "over", "tell",
    "latest", "news",
}


def search_articles(question: str, domain: str = None, limit: int = 6):
    """Keyword-scored retrieval from Postgres (not semantic search).

    Each question term that appears in title/body adds 1 to the score.
    Only the best-matching tier is kept, so a term like 'india' that
    matches almost everything does not flood the context.
    """
    terms = [t for t in re.findall(r"[a-zA-Z]{3,}", (question or "").lower())
             if t not in STOP_WORDS][:6]
    if not terms:
        return get_latest_articles(limit, domain)

    score_sql = " + ".join(
        ["(CASE WHEN lower(title || ' ' || coalesce(body, '')) LIKE %s "
         "THEN 1 ELSE 0 END)"] * len(terms)
    )
    params = [f"%{t}%" for t in terms]

    sql = (
        "SELECT id, title, body, source, domain, published_at, url, "
        f"({score_sql}) AS score FROM articles"
    )
    # DEFENSE has no dedicated source, so it is not filtered by tag here
    if domain and domain not in ("ALL", "DEFENSE"):
        sql += " WHERE domain = %s"
        params.append(domain)
    sql += " ORDER BY score DESC, ingested_at DESC LIMIT %s"
    params.append(limit * 3)

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql, params)
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()

    rows = [r for r in rows if r["score"] > 0]
    if not rows:
        return get_latest_articles(limit, domain)

    best = rows[0]["score"]
    rows = [r for r in rows if r["score"] >= min(best, 2)]
    return rows[:limit]