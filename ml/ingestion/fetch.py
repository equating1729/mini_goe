import feedparser
import hashlib
import sqlite3
from datetime import datetime
import sys
import os
import ssl
import certifi

ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())
sys.path.append(os.path.dirname(__file__))
from store import get_connection, init_db

SOURCES = [
    # GEOPOLITICS
    {
        "url": "https://www.thehindu.com/news/international/?service=rss",
        "name": "the_hindu_international",
        "domain": "GEO"
    },
    {
        "url": "https://www.thehindu.com/news/national/?service=rss",
        "name": "the_hindu_national",
        "domain": "GEO"
    },
    {
        "url": "https://feeds.reuters.com/reuters/INtopNews",
        "name": "reuters_india",
        "domain": "GEO"
    },
    {
        "url": "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=3",
        "name": "pib_india",
        "domain": "GEO"
    },

    # DEFENSE
    {
        "url": "https://feeds.feedburner.com/NDTV-LatestNews",
        "name": "ndtv_defence",
        "domain": "DEFENSE"
    },
    {
        "url": "https://timesofindia.indiatimes.com/rssfeeds/2647163.cms",
        "name": "times_defence",
        "domain": "DEFENSE"
    },

    # TECH
    {
        "url": "https://www.thehindu.com/sci-tech/technology/feeder/default.rss",
        "name": "the_hindu_tech",
        "domain": "TECH"
    },
    {
        "url": "https://inc42.com/feed/",
        "name": "inc42_tech",
        "domain": "TECH"
    },

    # CLIMATE
    {
        "url": "https://www.thehindu.com/sci-tech/energy-and-environment/feeder/default.rss",
        "name": "the_hindu_climate",
        "domain": "CLIMATE"
    },

    # ECONOMICS
    {
        "url": "https://www.thehindu.com/business/feeder/default.rss",
        "name": "the_hindu_business",
        "domain": "ECONOMICS"
    },
    {
        "url": "https://economictimes.indiatimes.com/rssfeedstopstories.cms",
        "name": "economic_times",
        "domain": "ECONOMICS"
    },
    {
    "url": "https://www.hindustantimes.com/feeds/rss/business/rssfeed.xml",
    "name": "hindustan_times_business",
    "domain": "ECONOMICS"
},

    # SOCIETY
    {
        "url": "https://www.thehindu.com/society/feeder/default.rss",
        "name": "the_hindu_society",
        "domain": "SOCIETY"
    },
    {
    "url": "https://www.hindustantimes.com/feeds/rss/lifestyle/art-culture/rssfeed.xml",
    "name": "hindustan_times_art_culture",
    "domain": "SOCIETY"
},
]

def make_fingerprint(url):
    return hashlib.sha256(url.encode()).hexdigest()

def clean_text(text):
    if not text:
        return ""
    return " ".join(text.split())

def fetch_feed(source):
    print(f"Fetching: {source['name']}...")
    try:
        feed = feedparser.parse(source["url"])
    except Exception as e:
        print(f"  Failed: {e}")
        return []

    articles = []
    for entry in feed.entries:
        url = entry.get("link", "")
        if not url:
            continue
        articles.append({
            "url": url,
            "fingerprint": make_fingerprint(url),
            "title": clean_text(entry.get("title", "")),
            "body": clean_text(entry.get("summary", "")),
            "source": source["name"],
            "domain": source.get("domain", "GEO"),
            "published_at": entry.get("published", str(datetime.now())),
        })

    print(f"  Found {len(articles)} articles.")
    return articles

def save_articles(articles):
    conn = get_connection()
    cursor = conn.cursor()
    new_count = 0
    for article in articles:
        try:
            cursor.execute("""
                           INSERT INTO articles
                           (url, fingerprint, title, body, source, domain, published_at)
                           VALUES
                           (:url, :fingerprint, :title, :body, :source, :domain, :published_at)
                           """, article)
            new_count += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    conn.close()
    return new_count

def run_ingestion():
    print(f"\n=== Ingestion run at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
    total_new = 0
    for source in SOURCES:
        articles = fetch_feed(source)
        new = save_articles(articles)
        print(f"  Saved {new} new from {source['name']}")
        total_new += new
    print(f"\nTotal new articles: {total_new}")

if __name__ == "__main__":
    init_db()
    run_ingestion()

for source in SOURCES:
    print(f"\nTesting: {source['name']}")

    feed = feedparser.parse(source["url"])

    print("bozo:", feed.bozo)
    print("entries:", len(feed.entries))

    if feed.bozo:
        print("Error:", feed.bozo_exception)