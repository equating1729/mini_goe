import { useState, useRef, useEffect } from "react";

const SYSTEM_PROMPT = `You are a Strategic Geopolitical Intelligence AI called "GOE Strategic Hub v4.0" for a dashboard called "GOE – India's POV". 
You analyze global events strictly from India's geopolitical, economic, security, and strategic perspective.
Keep answers concise (2-4 sentences), use a professional intelligence analyst tone, and always tie the answer back to India's interests.
Avoid markdown. Use plain text only.`;

export default function AIConsole({ domain }) {
  const [messages, setMessages] = useState([
    {
      role: "assistant",
      text: "Ask questions and get answers from India's perspective. Analyzing 10,000+ data points for regional context...",
    },
  ]);
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const messagesEndRef = useRef(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  async function handleSubmit() {
    const q = query.trim();
    if (!q || loading) return;
    setQuery("");

    const userMsg = { role: "user", text: q };
    setMessages((prev) => [...prev, userMsg]);
    setLoading(true);

    try {
      const response = await fetch("http://localhost:8000/api/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: q, domain: domain }),
      });

      const data = await response.json();
      const text = data.answer || "No response received.";
      setMessages((prev) => [...prev, { role: "assistant", text }]);
    } catch (err) {
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          text: "Connection to GOE backend failed. Please retry.",
        },
      ]);
    } finally {
      setLoading(false);
    }
  }

  function handleKey(e) {
    if (e.key === "Enter") handleSubmit();
  }

  return (
    <div style={styles.wrapper}>
      {/* Header */}
      <div style={styles.header}>
        <div style={styles.headerIcon}>✦</div>
        <div>
          <div style={styles.title}>STRATEGIC AI CONSOLE</div>
          <div style={styles.subtitle}>
            Geopolitical Intelligence Processor v4.0 — {domain}
          </div>
        </div>
      </div>

      {/* Messages */}
      <div style={styles.messages}>
        {messages.map((msg, i) => (
          <div
            key={i}
            style={msg.role === "user" ? styles.userMsg : styles.aiMsg}
          >
            {msg.role === "assistant" && (
              <div style={styles.aiAvatar}>
                <svg
                  width="14"
                  height="14"
                  viewBox="0 0 24 24"
                  fill="currentColor"
                  style={{ color: "#c8922a" }}
                >
                  <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 14.5v-9l6 4.5-6 4.5z" />
                </svg>
              </div>
            )}
            <div
              style={msg.role === "user" ? styles.userBubble : styles.aiBubble}
            >
              {msg.text}
            </div>
          </div>
        ))}

        {loading && (
          <div style={styles.aiMsg}>
            <div style={styles.aiAvatar}>
              <svg width="14" height="14" viewBox="0 0 24 24" fill="#c8922a">
                <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 14.5v-9l6 4.5-6 4.5z" />
              </svg>
            </div>
            <div style={styles.loadingDots}>
              <span style={{ ...styles.dot, animationDelay: "0ms" }} />
              <span style={{ ...styles.dot, animationDelay: "150ms" }} />
              <span style={{ ...styles.dot, animationDelay: "300ms" }} />
            </div>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div style={styles.inputRow}>
        <input
          style={styles.input}
          placeholder="QUERY STRATEGIC HUB..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={handleKey}
        />
        <button
          style={styles.sendBtn}
          onClick={handleSubmit}
          disabled={loading}
        >
          <svg
            width="16"
            height="16"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
          >
            <line x1="22" y1="2" x2="11" y2="13" />
            <polygon points="22 2 15 22 11 13 2 9 22 2" />
          </svg>
        </button>
      </div>

      <style>{`
        @keyframes pulse-dot {
          0%, 80%, 100% { opacity: 0.2; transform: scale(0.8); }
          40% { opacity: 1; transform: scale(1); }
        }
      `}</style>
    </div>
  );
}

const styles = {
  wrapper: {
    flex: 1,
    background: "#0f1214",
    border: "1px solid #1a1e22",
    borderRadius: "8px",
    display: "flex",
    flexDirection: "column",
    minWidth: 0,
  },
  header: {
    display: "flex",
    gap: "12px",
    alignItems: "center",
    padding: "14px 16px",
    borderBottom: "1px solid #1a1e22",
  },
  headerIcon: {
    width: "32px",
    height: "32px",
    background: "#1a1612",
    border: "1px solid #3a2e18",
    borderRadius: "6px",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    color: "#c8922a",
    fontSize: "16px",
  },
  title: {
    fontFamily: "'Orbitron', monospace",
    fontSize: "13px",
    fontWeight: 700,
    color: "#d4cfc8",
    letterSpacing: "1px",
  },
  subtitle: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "10px",
    color: "#6a6865",
    marginTop: "2px",
    letterSpacing: "0.5px",
  },
  messages: {
    flex: 1,
    overflowY: "auto",
    padding: "12px",
    display: "flex",
    flexDirection: "column",
    gap: "10px",
  },
  aiMsg: {
    display: "flex",
    gap: "8px",
    alignItems: "flex-start",
  },
  userMsg: {
    display: "flex",
    justifyContent: "flex-end",
  },
  aiAvatar: {
    width: "28px",
    height: "28px",
    minWidth: "28px",
    background: "#1a1612",
    border: "1px solid #2a2218",
    borderRadius: "50%",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
  },
  aiBubble: {
    background: "#161a1d",
    border: "1px solid #1e2428",
    borderRadius: "4px 12px 12px 12px",
    padding: "10px 12px",
    fontFamily: "'Rajdhani', sans-serif",
    fontSize: "13px",
    color: "#c0bbb4",
    lineHeight: 1.5,
    maxWidth: "85%",
  },
  userBubble: {
    background: "#1a1a12",
    border: "1px solid #2a2818",
    borderRadius: "12px 4px 12px 12px",
    padding: "10px 12px",
    fontFamily: "'Rajdhani', sans-serif",
    fontSize: "13px",
    color: "#c8b070",
    lineHeight: 1.5,
    maxWidth: "85%",
  },
  loadingDots: {
    display: "flex",
    gap: "4px",
    alignItems: "center",
    padding: "12px",
    background: "#161a1d",
    border: "1px solid #1e2428",
    borderRadius: "4px 12px 12px 12px",
  },
  dot: {
    display: "inline-block",
    width: "6px",
    height: "6px",
    borderRadius: "50%",
    background: "#c8922a",
    animation: "pulse-dot 1.2s infinite ease-in-out",
  },
  inputRow: {
    display: "flex",
    gap: "8px",
    padding: "12px",
    borderTop: "1px solid #1a1e22",
    alignItems: "center",
  },
  input: {
    flex: 1,
    background: "transparent",
    border: "none",
    outline: "none",
    color: "#8a8880",
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "12px",
    letterSpacing: "0.5px",
  },
  sendBtn: {
    width: "32px",
    height: "32px",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    background: "transparent",
    border: "none",
    color: "#6a6865",
    cursor: "pointer",
    transition: "color 0.2s",
  },
};

