import { useState, useEffect } from "react";

const API = "http://localhost:8000/api";

export default function Sidebar({ activeDomain = "ALL" }) {
  const [blinking, setBlinking] = useState(true);
  const [articles, setArticles] = useState([]);
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);

  // Blinking dot
  useEffect(() => {
    const t = setInterval(() => setBlinking((b) => !b), 900);
    return () => clearInterval(t);
  }, []);

  // Fetch real articles + stats from backend, filtered by domain
  useEffect(() => {
    async function fetchData() {
      setLoading(true);
      try {
        const domainParam = activeDomain !== "ALL" ? `&domain=${activeDomain}` : "";
        const [articlesRes, statsRes] = await Promise.all([
          fetch(`${API}/articles?limit=10${domainParam}`),
          fetch(`${API}/stats`),
        ]);
        const articlesData = await articlesRes.json();
        const statsData = await statsRes.json();
        setArticles(articlesData);
        setStats(statsData);
      } catch (err) {
        console.error("Failed to fetch feed:", err);
      } finally {
        setLoading(false);
      }
    }

    fetchData();

    // Refresh every 5 minutes
    const interval = setInterval(fetchData, 5 * 60 * 1000);
    return () => clearInterval(interval);
  }, [activeDomain]);

  function formatTime(dateStr) {
    if (!dateStr) return "—";
    try {
      const date = new Date(dateStr);
      return (
        date.toLocaleTimeString("en-IN", {
          hour: "2-digit",
          minute: "2-digit",
          hour12: false,
          timeZone: "Asia/Kolkata",
        }) + " IST"
      );
    } catch {
      return "—";
    }
  }

  function getStatus(index) {
    if (index === 0) return "active";
    if (index % 4 === 2) return "warn";
    return "info";
  }

  function getAccentColor(status) {
    if (status === "warn") return "#e07a3a";
    if (status === "active") return "#3ddc84";
    return "#2a3038";
  }

  function getSourceLabel(source) {
    const map = {
      the_hindu_international: "THE HINDU",
      the_hindu_national: "THE HINDU",
      the_hindu_tech: "THE HINDU",
      the_hindu_climate: "THE HINDU",
      the_hindu_business: "THE HINDU",
      the_hindu_society: "THE HINDU",
      NDTV_India: "NDTV",
      ndtv_defence: "NDTV",
      BBCnews_India: "BBC",
      reuters_india: "REUTERS",
      pib_india: "PIB",
      inc42_tech: "INC42",
      economic_times: "ECON TIMES",
      times_defence: "TOI",
    };
    return map[source] || source?.toUpperCase() || "FEED";
  }

  return (
    <div style={styles.sidebar}>
      {/* Header */}
      <div style={styles.header}>
        <div>
          <h2 style={styles.title}>
            Intelligence
            <br />
            Feed
          </h2>
          <p style={styles.subtitle}>
            {activeDomain === "ALL" ? "Current Live Updates" : `${activeDomain} Updates`}
          </p>
        </div>
        {/* <div style={styles.hubId}>
          <span style={styles.hubLabel}>HUB-ID:</span>
          <span style={styles.hubValue}>IND-001</span>
        </div> */}
      </div>

      {/* Stats bar */}
      {stats && (
        <div style={styles.statsBar}>
          <div style={styles.statItem}>
            <span style={styles.statNumber}>{stats.total_articles}</span>
            <span style={styles.statLabel}>ARTICLES</span>
          </div>
          <div style={styles.statDivider} />
          <div style={styles.statItem}>
            <span style={styles.statNumber}>{stats.total_entities}</span>
            <span style={styles.statLabel}>ENTITIES</span>
          </div>
          <div style={styles.statDivider} />
          <div style={styles.statItem}>
            <span style={styles.statNumber}>
              {articles.length}
            </span>
            <span style={styles.statLabel}>SHOWING</span>
          </div>
        </div>
      )}

      {/* Feed Items */}
      <div style={styles.feedList}>
        {loading ? (
          <div style={styles.loadingText}>LOADING FEED...</div>
        ) : articles.length === 0 ? (
          <div style={styles.loadingText}>
            NO {activeDomain !== "ALL" ? activeDomain : ""} ARTICLES FOUND
          </div>
        ) : (
          articles.map((article, index) => {
            const status = getStatus(index);
            return (
              <div
                key={article.id}
                style={styles.feedItem}
                onClick={() =>
                  article.url && window.open(article.url, "_blank")
                }
              >
                <div
                  style={{
                    ...styles.feedAccent,
                    background: getAccentColor(status),
                  }}
                />
                <div style={styles.feedContent}>
                  <div style={styles.feedMeta}>
                    <span style={styles.feedTime}>
                      {formatTime(article.published_at)}
                    </span>
                    <span style={styles.sourceTag}>
                      {getSourceLabel(article.source)}
                    </span>
                    {status === "active" && (
                      <span
                        style={{
                          ...styles.activeDot,
                          opacity: blinking ? 1 : 0.3,
                        }}
                      >
                        ●
                      </span>
                    )}
                    {status === "warn" && (
                      <span style={styles.warnIcon}>△</span>
                    )}
                  </div>
                  <p style={styles.feedText}>{article.title}</p>
                </div>
              </div>
            );
          })
        )}
      </div>

      {/* Footer */}
      <div style={styles.footer}>
        <div style={styles.footerStats}>
          <span style={styles.liveIndicator}>
            <span style={{ color: "#3ddc84", marginRight: "4px" }}>●</span>
            LIVE SYNC
          </span>
          <span style={styles.footerCount}>
            {stats ? `${stats.total_articles} RECORDS` : "—"}
          </span>
        </div>
        {/* <button style={styles.protocolBtn}>INITIATE PROTOCOL</button> */}
      </div>
    </div>
  );
}

const styles = {
  sidebar: {
    width: "100%",
    height: "100%",
    minWidth: 0,
    background: "#0f1214",
    border: "1px solid #1a1e22",
    borderRadius: "8px",
    display: "flex",
    flexDirection: "column",
    overflow: "hidden",
  },

  header: {
    padding: "20px 20px 16px",
    borderBottom: "1px solid #1a1e22",
    display: "flex",
    justifyContent: "space-between",
    alignItems: "flex-start",
  },

  title: {
    fontFamily: "'Orbitron', monospace",
    fontSize: "20px",
    fontWeight: 700,
    color: "#c8922a",
    lineHeight: 1.2,
    marginBottom: "6px",
  },

  subtitle: {
    fontFamily: "'Rajdhani', sans-serif",
    fontSize: "12px",
    color: "#6a6865",
    letterSpacing: "0.5px",
    textTransform: "uppercase",
  },

  hubId: {
    background: "#1a1612",
    border: "1px solid #2a2218",
    borderRadius: "4px",
    padding: "6px 10px",
    textAlign: "center",
  },

  hubLabel: {
    display: "block",
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "10px",
    color: "#6a6865",
    letterSpacing: "1px",
  },

  hubValue: {
    display: "block",
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "12px",
    color: "#c8922a",
    letterSpacing: "1px",
  },
  statsBar: {
    display: "flex",
    justifyContent: "space-around",
    padding: "12px 16px",
    borderBottom: "1px solid #1a1e22",
    background: "#0d1013",
  },
  statItem: {
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    gap: "2px",
  },
  statNumber: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "16px",
    color: "#c8922a",
    fontWeight: 700,
  },
  statLabel: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "9px",
    color: "#6a6865",
    letterSpacing: "1px",
  },
  statDivider: {
    width: "1px",
    background: "#1a1e22",
    alignSelf: "stretch",
  },
  feedList: {
    flex: 1,
    padding: "12px 0",
    overflowY: "auto",
  },
  loadingText: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "11px",
    color: "#6a6865",
    textAlign: "center",
    padding: "20px",
    letterSpacing: "1px",
  },
  feedItem: {
    display: "flex",
    gap: "12px",
    padding: "12px 20px",
    borderBottom: "1px solid #13161a",
    cursor: "pointer",
    transition: "background 0.2s",
    alignItems: "flex-start",
  },

  feedAccent: {
    width: "2px",
    minWidth: "2px",
    borderRadius: "2px",
    alignSelf: "stretch",
  },

  feedContent: {
    flex: 1,
    minWidth: 0,
  },

  feedMeta: {
    display: "flex",
    alignItems: "center",
    gap: "6px",
    marginBottom: "4px",
    flexWrap: "wrap",
  },

  feedTime: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "11px",
    color: "#6a6865",
    letterSpacing: "0.5px",
    whiteSpace: "nowrap",
  },
  sourceTag: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "9px",
    color: "#c8922a",
    background: "#1a1612",
    border: "1px solid #2a2218",
    borderRadius: "2px",
    padding: "1px 4px",
    letterSpacing: "0.5px",
  },
  activeDot: {
    color: "#3ddc84",
    fontSize: "10px",
    transition: "opacity 0.3s",
  },

  warnIcon: {
    color: "#e07a3a",
    fontSize: "11px",
  },

  feedText: {
    fontFamily: "'Rajdhani', sans-serif",
    fontSize: "13px",
    color: "#c0bbb4",
    lineHeight: 1.4,
    fontWeight: 500,
    whiteSpace: "normal",
    overflowWrap: "break-word",
    wordBreak: "normal",
  },

  footer: {
    padding: "12px 20px 16px",
    borderTop: "1px solid #1a1e22",
    display: "flex",
    flexDirection: "column",
    gap: "10px",
  },
  footerStats: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
  },
  liveIndicator: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "10px",
    color: "#6a6865",
    letterSpacing: "1px",
  },
  footerCount: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "10px",
    color: "#c8922a",
    letterSpacing: "1px",
  },

  protocolBtn: {
    width: "100%",
    padding: "12px",
    background: "transparent",
    border: "1px solid #3a2e18",
    borderRadius: "4px",
    color: "#c8922a",
    fontFamily: "'Rajdhani', sans-serif",
    fontWeight: 700,
    fontSize: "13px",
    letterSpacing: "2px",
    cursor: "pointer",
    transition: "all 0.2s",
  },
};