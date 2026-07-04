const tabs = ["ALL", "GEO", "DEFENSE", "TECH", "CLIMATE"];

export default function Navbar({ activeDomain, setActiveDomain }) {
  return (
    <nav style={styles.nav}>
      <div style={styles.logo}>
        <span style={styles.logoText}>GOE</span>
        <span style={styles.logoDivider}> – </span>
        <span style={styles.logoSub}>India's POV</span>
      </div>

      <div style={styles.tabs}>
        {tabs.map((tab) => (
          <button
            key={tab}
            style={{
              ...styles.tab,
              ...(activeDomain === tab ? styles.tabActive : {}),
            }}
            onClick={() => setActiveDomain(tab)}
          >
            {tab}
          </button>
        ))}
      </div>
    </nav>
  );
}

const styles = {
  nav: {
  display: "flex",
  alignItems: "center",
  padding: "0 24px",
  height: "56px",
  background: "#0a0c0e",
  borderBottom: "1px solid #1a1e22",
  position: "sticky",
  top: 0,
  zIndex: 100,
  position: "relative",
},
  logo: {
    display: "flex",
    alignItems: "baseline",
    gap: "2px",
    minWidth: "200px",
  },
  logoText: {
    fontFamily: "'Orbitron', monospace",
    fontSize: "18px",
    fontWeight: 700,
    color: "#c8922a",
    letterSpacing: "2px",
  },
  logoDivider: {
    color: "#4a4845",
    fontFamily: "'Orbitron', monospace",
    fontSize: "16px",
  },
  logoSub: {
    fontFamily: "'Orbitron', monospace",
    fontSize: "14px",
    fontWeight: 400,
    color: "#d4cfc8",
    letterSpacing: "1px",
  },
 tabs: {
  display: "flex",
  gap: "4px",
  background: "#0f1214",
  border: "1px solid #1e2428",
  borderRadius: "6px",
  padding: "4px",
  position: "absolute",
  left: "50%",
  transform: "translateX(-50%)",
},
  tab: {
    padding: "6px 20px",
    background: "transparent",
    border: "none",
    color: "#8a8880",
    fontFamily: "'Rajdhani', sans-serif",
    fontWeight: 600,
    fontSize: "13px",
    letterSpacing: "1.5px",
    borderRadius: "4px",
    transition: "all 0.2s",
    cursor: "pointer",
  },
  tabActive: {
    background: "#1e1a12",
    color: "#c8922a",
    border: "1px solid #3a2e18",
  },
};
