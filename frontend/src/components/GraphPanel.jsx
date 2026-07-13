import { useState, useEffect } from "react";

const API = import.meta.env.VITE_API;

export default function GraphPanel() {
  const [entities, setEntities] = useState([]);
  const [selected, setSelected] = useState(null);
  const [connections, setConnections] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${API}/graph/entities?limit=15`)
      .then((r) => r.json())
      .then((data) => {
        setEntities(data);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, []);

  async function handleEntityClick(entity) {
    if (selected?.name === entity.name) {
      setSelected(null);
      setConnections([]);
      return;
    }
    setSelected(entity);
    try {
      const res = await fetch(
        `${API}/graph/entity/${encodeURIComponent(entity.name)}`,
      );
      const data = await res.json();
      setConnections(data);
    } catch {
      setConnections([]);
    }
  }

  function getLabelColor(label) {
    const map = {
      GPE: "#3ddc84",
      PERSON: "#c8922a",
      ORG: "#4a9eff",
      NORP: "#e07a3a",
      EVENT: "#a855f7",
    };
    return map[label] || "#6a6865";
  }

  function getMaxConnections() {
    return Math.max(...entities.map((e) => e.connections || 1), 1);
  }

  return (
    <div style={styles.wrapper}>
      {/* Header */}
      <div style={styles.header}>
        <div style={styles.headerLeft}>
          <div style={styles.headerIcon}>◈</div>
          <div>
            <div style={styles.title}>ENTITY GRAPH</div>
            <div style={styles.subtitle}>Knowledge Network — Top 15 Nodes</div>
          </div>
        </div>
        <div style={styles.legend}>
          {["GPE", "PERSON", "ORG", "NORP"].map((label) => (
            <div key={label} style={styles.legendItem}>
              <div
                style={{
                  ...styles.legendDot,
                  background: getLabelColor(label),
                }}
              />
              <span style={styles.legendText}>{label}</span>
            </div>
          ))}
        </div>
      </div>

      {loading ? (
        <div style={styles.loadingText}>LOADING GRAPH...</div>
      ) : (
        <div style={styles.body}>
          {/* Left — Entity bars */}
          <div style={styles.entityList}>
            {entities.map((entity) => {
              const pct = (entity.connections / getMaxConnections()) * 100;
              const isSelected = selected?.name === entity.name;
              return (
                <div
                  key={entity.name}
                  style={{
                    ...styles.entityRow,
                    background: isSelected ? "#1a1612" : "transparent",
                    borderColor: isSelected ? "#3a2e18" : "transparent",
                  }}
                  onClick={() => handleEntityClick(entity)}
                >
                  <div style={styles.entityMeta}>
                    <div style={styles.entityNameRow}>
                      <div
                        style={{
                          ...styles.entityDot,
                          background: getLabelColor(entity.label),
                        }}
                      />
                      <span style={styles.entityName}>{entity.name}</span>
                      <span
                        style={{
                          ...styles.entityLabel,
                          color: getLabelColor(entity.label),
                          borderColor: getLabelColor(entity.label) + "44",
                        }}
                      >
                        {entity.label}
                      </span>
                    </div>
                    <span style={styles.entityCount}>{entity.connections}</span>
                  </div>
                  <div style={styles.barBg}>
                    <div
                      style={{
                        ...styles.barFill,
                        width: `${pct}%`,
                        background: getLabelColor(entity.label),
                      }}
                    />
                  </div>
                </div>
              );
            })}
          </div>

          {/* Right — Connections panel */}
          <div style={styles.connectionsPanel}>
            {!selected ? (
              <div style={styles.selectHint}>
                ← Click an entity to see its connections
              </div>
            ) : (
              <>
                <div style={styles.selectedHeader}>
                  <div
                    style={{
                      ...styles.selectedDot,
                      background: getLabelColor(selected.label),
                    }}
                  />
                  <span style={styles.selectedName}>{selected.name}</span>
                  <span style={styles.selectedCount}>
                    {selected.connections} connections
                  </span>
                </div>
                <div style={styles.connectionList}>
                  {connections.length === 0 ? (
                    <div style={styles.noConnections}>No connections found</div>
                  ) : (
                    connections.map((conn, i) => (
                      <div key={i} style={styles.connRow}>
                        <div
                          style={{
                            ...styles.connDot,
                            background: getLabelColor(conn.label),
                          }}
                        />
                        <span style={styles.connName}>{conn.connected_to}</span>
                        <div style={styles.connStrengthBar}>
                          <div
                            style={{
                              ...styles.connStrengthFill,
                              width: `${Math.min(
                                (conn.strength /
                                  (connections[0]?.strength || 1)) *
                                  100,
                                100,
                              )}%`,
                              background: getLabelColor(conn.label),
                            }}
                          />
                        </div>
                        <span style={styles.connStrength}>{conn.strength}</span>
                      </div>
                    ))
                  )}
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

const styles = {
  wrapper: {
    background: "#0f1214",
    border: "1px solid #1a1e22",
    borderRadius: "8px",
    display: "flex",
    flexDirection: "column",
    overflow: "hidden",
    marginTop: "12px",
  },
  header: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    padding: "14px 16px",
    borderBottom: "1px solid #1a1e22",
  },
  headerLeft: {
    display: "flex",
    gap: "12px",
    alignItems: "center",
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
  },
  legend: {
    display: "flex",
    gap: "12px",
    alignItems: "center",
  },
  legendItem: {
    display: "flex",
    alignItems: "center",
    gap: "4px",
  },
  legendDot: {
    width: "6px",
    height: "6px",
    borderRadius: "50%",
  },
  legendText: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "9px",
    color: "#6a6865",
    letterSpacing: "0.5px",
  },
  loadingText: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "11px",
    color: "#6a6865",
    textAlign: "center",
    padding: "30px",
    letterSpacing: "1px",
  },
  body: {
    display: "flex",
    gap: "0px",
    minHeight: "320px",
  },
  entityList: {
    flex: 1,
    borderRight: "1px solid #1a1e22",
    overflowY: "auto",
    padding: "8px 0",
  },
  entityRow: {
    padding: "8px 16px",
    cursor: "pointer",
    border: "1px solid transparent",
    borderRadius: "4px",
    margin: "2px 8px",
    transition: "all 0.2s",
  },
  entityMeta: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: "4px",
  },
  entityNameRow: {
    display: "flex",
    alignItems: "center",
    gap: "6px",
  },
  entityDot: {
    width: "6px",
    height: "6px",
    borderRadius: "50%",
    minWidth: "6px",
  },
  entityName: {
    fontFamily: "'Rajdhani', sans-serif",
    fontSize: "13px",
    color: "#c0bbb4",
    fontWeight: 600,
  },
  entityLabel: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "9px",
    border: "1px solid",
    borderRadius: "2px",
    padding: "1px 4px",
    letterSpacing: "0.5px",
  },
  entityCount: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "11px",
    color: "#6a6865",
  },
  barBg: {
    height: "2px",
    background: "#1a1e22",
    borderRadius: "2px",
    overflow: "hidden",
  },
  barFill: {
    height: "100%",
    borderRadius: "2px",
    transition: "width 0.5s ease",
  },
  connectionsPanel: {
    width: "260px",
    minWidth: "260px",
    padding: "12px",
    overflowY: "auto",
  },
  selectHint: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "10px",
    color: "#6a6865",
    textAlign: "center",
    padding: "40px 20px",
    lineHeight: 1.6,
    letterSpacing: "0.5px",
  },
  selectedHeader: {
    display: "flex",
    alignItems: "center",
    gap: "8px",
    marginBottom: "12px",
    paddingBottom: "10px",
    borderBottom: "1px solid #1a1e22",
  },
  selectedDot: {
    width: "8px",
    height: "8px",
    borderRadius: "50%",
    minWidth: "8px",
  },
  selectedName: {
    fontFamily: "'Orbitron', monospace",
    fontSize: "12px",
    color: "#c8922a",
    fontWeight: 700,
    flex: 1,
  },
  selectedCount: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "10px",
    color: "#6a6865",
  },
  connectionList: {
    display: "flex",
    flexDirection: "column",
    gap: "6px",
  },
  noConnections: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "10px",
    color: "#6a6865",
    textAlign: "center",
    padding: "20px",
  },
  connRow: {
    display: "flex",
    alignItems: "center",
    gap: "6px",
  },
  connDot: {
    width: "5px",
    height: "5px",
    borderRadius: "50%",
    minWidth: "5px",
  },
  connName: {
    fontFamily: "'Rajdhani', sans-serif",
    fontSize: "12px",
    color: "#c0bbb4",
    width: "90px",
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap",
  },
  connStrengthBar: {
    flex: 1,
    height: "2px",
    background: "#1a1e22",
    borderRadius: "2px",
    overflow: "hidden",
  },
  connStrengthFill: {
    height: "100%",
    borderRadius: "2px",
  },
  connStrength: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "10px",
    color: "#6a6865",
    minWidth: "24px",
    textAlign: "right",
  },
};
