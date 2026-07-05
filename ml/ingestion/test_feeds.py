import feedparser

CANDIDATE_SOURCES = {
    # DEFENSE
    "idrw_defense": "https://idrw.org/feed/",
    "swarajya_defense": "https://swarajyamag.com/feed",
    "the_hindu_defense": "https://www.thehindu.com/news/national/defence/feeder/default.rss",
    "the_hindu_defence_alt": "https://www.thehindu.com/news/national/feeder/default.rss",
    "ndtv_defence": "https://feeds.feedburner.com/NDTV-LatestNews",
    "times_defence": "https://timesofindia.indiatimes.com/rssfeeds/2647163.cms",
    
    # TECH
    "the_hindu_tech": "https://www.thehindu.com/sci-tech/technology/feeder/default.rss",
    "inc42_tech": "https://inc42.com/feed/",
    "analytics_india": "https://analyticsindiamag.com/feed/",

    # CLIMATE
    "downtoearth": "https://www.downtoearth.org.in/rss/rss.xml",
    "the_hindu_climate": "https://www.thehindu.com/sci-tech/energy-and-environment/feeder/default.rss",

    # ECONOMICS
    "the_hindu_business": "https://www.thehindu.com/business/feeder/default.rss",
    "economic_times": "https://economictimes.indiatimes.com/rssfeedstopstories.cms",
    "hindustan_times_business": "https://www.hindustantimes.com/feeds/rss/business/rssfeed.xml",

    # SOCIETY
    "the_hindu_society": "https://www.thehindu.com/society/feeder/default.rss",
    "hindustan_times_art_culture": "https://www.hindustantimes.com/feeds/rss/lifestyle/art-culture/rssfeed.xml",
}

print("Testing all candidate RSS sources...\n")

working = []
broken = []

for name, url in CANDIDATE_SOURCES.items():
    try:
        feed = feedparser.parse(url)
        count = len(feed.entries)
        if count > 0 and not feed.bozo:
            print(f"✅ {name}: {count} entries")
            working.append((name, url, count))
        elif count > 0 and feed.bozo:
            print(f"⚠️  {name}: {count} entries but bozo=True ({feed.bozo_exception})")
            working.append((name, url, count))
        else:
            print(f"❌ {name}: 0 entries")
            broken.append((name, url))
    except Exception as e:
        print(f"❌ {name}: ERROR - {e}")
        broken.append((name, url))

print(f"\n=== SUMMARY ===")
print(f"Working: {len(working)}")
print(f"Broken: {len(broken)}")
print("\nCopy these into fetch.py SOURCES list:\n")
for name, url, count in working:
    print(f'    {{"url": "{url}", "name": "{name}"}},')