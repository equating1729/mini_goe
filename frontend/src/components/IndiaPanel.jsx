import { useEffect, useRef, useState } from "react";

// India-specific coordinates for major locations
const INDIA_COORDS = {
  "New Delhi": [28.6139, 77.2090],
  "Delhi": [28.6139, 77.2090],
  "Mumbai": [19.0760, 72.8777],
  "Bangalore": [12.9716, 77.5946],
  "Bengaluru": [12.9716, 77.5946],
  "Chennai": [13.0827, 80.2707],
  "Kolkata": [22.5726, 88.3639],
  "Hyderabad": [17.3850, 78.4867],
  "Ahmedabad": [23.0225, 72.5714],
  "Pune": [18.5204, 73.8567],
  "Jaipur": [26.9124, 75.7873],
  "Lucknow": [26.8467, 80.9462],
  "Kanpur": [26.4499, 80.3319],
  "Nagpur": [21.1458, 79.0882],
  "Indore": [22.7196, 75.8577],
  "Bhopal": [23.2599, 77.4126],
  "Visakhapatnam": [17.6868, 83.2185],
  "Patna": [25.5941, 85.1376],
  "Vadodara": [22.3072, 73.1812],
  "Guwahati": [26.1445, 91.7362],
  "Chandigarh": [30.7333, 76.7794],
  "Srinagar": [34.0837, 74.7973],
  "Amritsar": [31.6340, 74.8723],
  "Jodhpur": [26.2389, 73.0243],
  "Coimbatore": [11.0168, 76.9558],
  "Kochi": [9.9312, 76.2673],
  "Thiruvananthapuram": [8.5241, 76.9366],
  "Bhubaneswar": [20.2961, 85.8245],
  "Ranchi": [23.3441, 85.3096],
  "Dehradun": [30.3165, 78.0322],
  "Shimla": [31.1048, 77.1734],
  "Gangtok": [27.3389, 88.6065],
  "Itanagar": [27.0844, 93.6053],
  "Imphal": [24.8170, 93.9368],
  "Aizawl": [23.7271, 92.7176],
  "Shillong": [25.5788, 91.8933],
  "Agartala": [23.8315, 91.2868],
  "Dimapur": [25.9110, 93.7273],
  "Port Blair": [11.6234, 92.7265],
  "Silvassa": [20.2737, 72.9974],
  "Daman": [20.4283, 72.8397],
  "Puducherry": [11.9416, 79.8083],
  "Kavaratti": [10.5622, 72.6369],
};

// Layer definitions for India - SAME COLORS AS WORLDMAP
const INDIA_LAYERS = [
  {
    id: "geo_entities",
    icon: "◈",
    label: "GEOPOLITICAL ENTITIES",
    color: "#3ddc84",
    defaultOn: true,
  },
  {
    id: "defense_entities",
    icon: "✈",
    label: "DEFENSE ENTITIES",
    color: "#4a8ae8",
    defaultOn: true,
  },
  {
    id: "tech_entities",
    icon: "⬜",
    label: "TECH ENTITIES",
    color: "#4ab8e8",
    defaultOn: true,
  },
  {
    id: "climate_entities",
    icon: "⊙",
    label: "CLIMATE ENTITIES",
    color: "#3ddc84",
    defaultOn: true,
  },
  {
    id: "person_entities",
    icon: "●",
    label: "KEY PERSONS",
    color: "#c8922a",
    defaultOn: true,
  },
];

// Domain to layer mapping - SAME AS WORLDMAP
const DOMAIN_LAYER_MAP = {
  ALL: ["geo_entities", "defense_entities", "tech_entities", "climate_entities", "person_entities"],
  GEO: ["geo_entities", "person_entities"],
  DEFENSE: ["defense_entities", "person_entities"],
  TECH: ["tech_entities"],
  CLIMATE: ["climate_entities"],
};

// Entity label to layer mapping
const ENTITY_LABEL_TO_LAYER = {
  "GPE": "geo_entities",
  "LOC": "geo_entities",
  "NORP": "geo_entities",
  "ORG": "defense_entities",
  "EVENT": "defense_entities",
  "PERSON": "person_entities",
  "DATE": "climate_entities",
  "TIME": "climate_entities",
  "MONEY": "tech_entities",
  "PERCENT": "tech_entities",
  "FAC": "tech_entities",
  "PRODUCT": "tech_entities",
  "WORK_OF_ART": "tech_entities",
  "LAW": "defense_entities",
};

// Classify entity to layer - SAME AS WORLDMAP
function classifyEntity(entity, activeDomain) {
  if (entity.label === "PERSON") return "person_entities";
  if (activeDomain === "DEFENSE") return "defense_entities";
  if (activeDomain === "TECH") return "tech_entities";
  if (activeDomain === "CLIMATE") return "climate_entities";
  return "geo_entities";
}

// Get color for entity type
function getEntityColor(label, activeDomain) {
  if (activeDomain === "GEO") return "#3ddc84";
  if (activeDomain === "DEFENSE") return "#4a8ae8";
  if (activeDomain === "TECH") return "#4ab8e8";
  if (activeDomain === "CLIMATE") return "#3ddc84";
  
  const layer = ENTITY_LABEL_TO_LAYER[label];
  const found = INDIA_LAYERS.find(l => l.id === layer);
  if (found) return found.color;
  
  return "#6a6865";
}

function markerHtml(color, connections, pulse) {
  const size = Math.max(8, Math.min(20, 8 + (connections || 0) * 0.5));
  return `<div style="position:relative;display:flex;align-items:center;justify-content:center;width:${size + 12}px;height:${size + 12}px;">
    ${pulse ? `<div style="position:absolute;width:${size + 12}px;height:${size + 12}px;border-radius:50%;background:${color};opacity:0.25;animation:indiaPulse 1.8s ease-out infinite;"></div>` : ''}
    <div style="width:${size}px;height:${size}px;border-radius:50%;background:${color};box-shadow:0 0 ${size/2}px ${color};z-index:2;border:1px solid rgba(255,255,255,0.3);display:flex;align-items:center;justify-content:center;font-family:'Share Tech Mono',monospace;font-size:${Math.max(5, size/3)}px;color:#000;font-weight:700;">
      ${connections || 1}
    </div>
  </div>`;
}

export default function IndiaPanel({ domain = "ALL", onViewChange, viewMode }) {
  const mapRef = useRef(null);
  const mapObj = useRef(null);
  const markersGroup = useRef({});
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(true);
  const [entities, setEntities] = useState([]);
  const [selectedEntity, setSelectedEntity] = useState(null);
  const [states, setStates] = useState(() => {
    const s = {};
    INDIA_LAYERS.forEach((l) => (s[l.id] = !!l.defaultOn));
    return s;
  });

  useEffect(() => {
    const activeLayerIds = DOMAIN_LAYER_MAP[domain] || DOMAIN_LAYER_MAP.ALL;
    setStates((prev) => {
      const next = {};
      INDIA_LAYERS.forEach((l) => {
        next[l.id] = activeLayerIds.includes(l.id);
      });
      return next;
    });
  }, [domain]);

  useEffect(() => {
    const domainParam = domain !== "ALL" ? `&domain=${domain}` : "";
    fetch(`http://localhost:8000/api/graph/entities?limit=50${domainParam}`)
      .then((r) => r.json())
      .then((data) => {
        const indiaEntities = data.filter(e => {
          const match = Object.keys(INDIA_COORDS).find(city => 
            e.name.toLowerCase().includes(city.toLowerCase()) || 
            city.toLowerCase().includes(e.name.toLowerCase())
          );
          if (match) {
            e.coords = INDIA_COORDS[match];
            e.location = match;
            return true;
          }
          return false;
        });
        setEntities(indiaEntities);
        setLoading(false);
      })
      .catch(() => {
        setLoading(false);
        setEntities([]);
      });
  }, [domain]);

  useEffect(() => {
    if (mapObj.current || !mapRef.current) return;
    const L = window.L;
    if (!L) return;

    const map = L.map(mapRef.current, {
      center: [22.5, 82.5],
      zoom: 4.5,
      zoomControl: false,
      minZoom: 4,
      maxZoom: 8,
      attributionControl: false,
    });

    L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
      maxZoom: 19,
    }).addTo(map);

    INDIA_LAYERS.forEach((layer) => {
      const group = L.layerGroup();
      markersGroup.current[layer.id] = group;
      if (layer.defaultOn) group.addTo(map);
    });

    mapObj.current = map;

    setTimeout(() => {
      map.invalidateSize();
    }, 200);

    const resizeObserver = new ResizeObserver(() => {
      setTimeout(() => {
        map.invalidateSize();
      }, 50);
    });

    resizeObserver.observe(mapRef.current);

    return () => {
      resizeObserver.disconnect();
      map.remove();
      mapObj.current = null;
    };
  }, []);

  useEffect(() => {
    const map = mapObj.current;
    if (!map) return;
    INDIA_LAYERS.forEach((l) => {
      const g = markersGroup.current[l.id];
      if (!g) return;
      states[l.id]
        ? !map.hasLayer(g) && g.addTo(map)
        : map.hasLayer(g) && map.removeLayer(g);
    });
  }, [states]);

  useEffect(() => {
    const map = mapObj.current;
    const L = window.L;
    if (!map || !L) return;

    INDIA_LAYERS.forEach((l) => {
      markersGroup.current[l.id]?.clearLayers();
    });

    if (entities.length === 0) return;

    const maxConn = Math.max(...entities.map(e => e.connections || 1), 1);

    entities.forEach((entity) => {
      if (!entity.coords) return;

      const bucket = classifyEntity(entity, domain);
      const group = markersGroup.current[bucket];
      if (!group) return;

      const color = getEntityColor(entity.label, domain);
      const connections = entity.connections || 1;
      const isPulse = connections > maxConn * 0.7;

      const icon = L.divIcon({
        html: markerHtml(color, connections, isPulse),
        className: "",
        iconSize: [30, 30],
        iconAnchor: [15, 15],
      });

      const marker = L.marker(entity.coords, { icon })
        .bindPopup(
          `<div style="font-family:'Share Tech Mono',monospace;font-size:11px;color:#d4cfc8;background:#0f1214;border:1px solid #2a3038;padding:8px 12px;border-radius:3px;min-width:160px;">
            <div style="color:${color};font-weight:700;margin-bottom:4px;">${entity.name}</div>
            <div style="color:#6a6865;">${entity.label} · ${connections} connections</div>
            ${entity.location ? `<div style="color:#4a4845;font-size:9px;">📍 ${entity.location}</div>` : ''}
          </div>`,
          { className: "india-popup", closeButton: false }
        )
        .on('click', () => {
          setSelectedEntity(entity);
        });

      marker.addTo(group);
    });
  }, [entities, domain]);

  const toggleLayer = (id) => setStates((prev) => ({ ...prev, [id]: !prev[id] }));

  const activeLayerIds = DOMAIN_LAYER_MAP[domain] || DOMAIN_LAYER_MAP.ALL;
  const filteredLayers = INDIA_LAYERS.filter((l) =>
    l.label.toUpperCase().includes(search.toUpperCase())
  );

  function getEntityTypeColor(label) {
    if (label === "PERSON") return "#c8922a";
    const layer = ENTITY_LABEL_TO_LAYER[label];
    const found = INDIA_LAYERS.find(l => l.id === layer);
    return found ? found.color : "#6a6865";
  }

  return (
    <div style={s.root}>
      <style>{`
        @keyframes indiaPulse { 0%{transform:scale(1);opacity:.4} 100%{transform:scale(3.5);opacity:0} }
        @keyframes slideIn { from { opacity:0; transform:translateY(10px); } to { opacity:1; transform:translateY(0); } }
        .leaflet-container { background:#080c10 !important; }
        .india-popup .leaflet-popup-content-wrapper { background:transparent!important;border:none!important;box-shadow:none!important;padding:0!important; }
        .india-popup .leaflet-popup-tip-container { display:none!important; }
        .india-popup .leaflet-popup-content { margin:0!important; }
        #india-search::placeholder { color:#3a3835; }
      `}</style>

      <div style={s.topBar}>
        <div style={s.topLeft}>
          <span style={s.panelLabel}>REGIONAL IMPACT ANALYTICS</span>
          <span style={s.subLabel}>India Intelligence Map · {domain}</span>
        </div>

        {onViewChange && (
          <div style={s.viewToggle}>
            <button 
              style={{ ...s.toggleBtn, ...(viewMode === "MAP" ? s.toggleBtnActive : {}) }} 
              onClick={() => onViewChange("MAP")}
            >
              MAP
            </button>
            <button 
              style={{ ...s.toggleBtn, ...(viewMode === "GRAPH" ? s.toggleBtnActive : {}) }} 
              onClick={() => onViewChange("GRAPH")}
            >
              GRAPH
            </button>
          </div>
        )}

        <div style={s.topRight}>
          <span style={s.syncDot} />
          <span style={s.syncText}>ENTITIES: {entities.length}</span>
        </div>
      </div>

      <div style={s.body}>
        <div style={s.panel}>
          <div style={s.pHead}>
            <span style={s.pTitle}>LAYERS</span>
            {/* <div style={s.pIcons}>
              <span style={s.iBtn}>?</span>
              <span style={s.iBtn}>▼</span>
            </div> */}
          </div>

          <div style={s.searchBox}>
            <input
              id="india-search"
              style={s.search}
              placeholder="Search layers..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
          </div>

          {/* ✅ FIXED: Only checkbox changes, labels always visible */}
          <div style={s.list}>
            {filteredLayers.map((l) => (
              <div
                key={l.id}
                style={s.row}
                onClick={() => toggleLayer(l.id)}
              >
                <div
                  style={{
                    ...s.cb,
                    background: states[l.id] ? "#1a3a5c" : "transparent",
                    borderColor: states[l.id] ? "#2a6aac" : "#2a3038",
                  }}
                >
                  {states[l.id] && (
                    <svg width="9" height="7" viewBox="0 0 9 7" fill="none">
                      <path
                        d="M1 3.5L3.5 6L8 1"
                        stroke="#4ab8e8"
                        strokeWidth="1.5"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                      />
                    </svg>
                  )}
                </div>
                {/* ✅ Icon - Always full color */}
                <span style={{ ...s.lIcon, color: l.color }}>
                  {l.icon}
                </span>
                {/* ✅ Label - Always full brightness */}
                <span style={s.lLabel}>
                  {l.label}
                </span>
              </div>
            ))}
          </div>

          <div style={s.stats}>
            <div style={s.stat}>
              <span style={s.statVal({ color: "#c8922a" })}>
                {entities.length}
              </span>
              <span style={s.statKey}>NODES</span>
            </div>
            <div style={s.statDiv} />
            <div style={s.stat}>
              <span style={s.statVal({ color: "#3ddc84" })}>
                {entities.reduce((sum, e) => sum + (e.connections || 0), 0)}
              </span>
              <span style={s.statKey}>CONNECTIONS</span>
            </div>
          </div>
        </div>

        <div style={s.mapWrap}>
          <div ref={mapRef} style={s.map} />

          <div style={s.zoom}>
            <button style={s.zBtn} onClick={() => mapObj.current?.zoomIn()}>+</button>
            <button style={s.zBtn} onClick={() => mapObj.current?.zoomOut()}>−</button>
            <button style={{ ...s.zBtn, marginTop: 4 }} onClick={() => mapObj.current?.setView([22.5, 82.5], 4.5)}>⌂</button>
          </div>

          <div style={s.hubBadge}>
            <span style={s.hubKey}>DOMAIN</span>
            <span style={s.hubVal}>{domain}</span>
          </div>

          <div style={s.webgl}>INDIA</div>
        </div>
      </div>

      {selectedEntity && (
        <div style={s.detailPanel}>
          <div style={s.detailHeader}>
            <div
              style={{
                ...s.detailDot,
                background: getEntityTypeColor(selectedEntity.label),
              }}
            />
            <span style={s.detailName}>{selectedEntity.name}</span>
            <span
              style={{
                ...s.detailLabel,
                color: getEntityTypeColor(selectedEntity.label),
                borderColor: getEntityTypeColor(selectedEntity.label) + "44",
              }}
            >
              {selectedEntity.label}
            </span>
            <button style={s.detailClose} onClick={() => setSelectedEntity(null)}>✕</button>
          </div>
          <div style={s.detailBody}>
            <span style={s.detailConnections}>Connections: {selectedEntity.connections}</span>
            {selectedEntity.location && (
              <span style={s.detailLocation}>📍 {selectedEntity.location}</span>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// Styles - SAME AS WORLDMAP
const s = {
  root: {
    flex: 1,
    background: "#0f1214",
    border: "1px solid #1a1e22",
    borderRadius: "8px",
    display: "flex",
    flexDirection: "column",
    overflow: "hidden",
    minWidth: 0,
    position: "relative",
  },
  topBar: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    padding: "10px 14px",
    borderBottom: "1px solid #1a1e22",
    flexShrink: 0,
  },
  topLeft: { display: "flex", flexDirection: "column", gap: "2px" },
  panelLabel: {
    fontFamily: "'Rajdhani', sans-serif",
    fontSize: "11px",
    fontWeight: 700,
    color: "#3ddc84",
    letterSpacing: "2px",
    textTransform: "uppercase",
  },
  subLabel: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "10px",
    color: "#4a4845",
    letterSpacing: "0.5px",
  },
  topRight: { display: "flex", alignItems: "center", gap: "6px" },
  syncDot: {
    width: "6px",
    height: "6px",
    borderRadius: "50%",
    background: "#3ddc84",
    boxShadow: "0 0 4px #3ddc84",
    display: "inline-block",
  },
  syncText: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "10px",
    color: "#6a6865",
    letterSpacing: "0.5px",
  },
  body: {
    flex: 1,
    display: "flex",
    overflow: "hidden",
    minHeight: 0,
  },
  panel: {
    width: "220px",
    minWidth: "220px",
    background: "rgba(10,12,14,0.95)",
    borderRight: "1px solid #1e2428",
    display: "flex",
    flexDirection: "column",
    zIndex: 10,
  },
  pHead: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "8px 10px",
    borderBottom: "1px solid #1e2428",
  },
  pTitle: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "10px",
    color: "#8a8880",
    letterSpacing: "2px",
  },
  pIcons: { display: "flex", gap: "4px" },
  iBtn: {
    width: "18px",
    height: "18px",
    background: "#1a1e22",
    border: "1px solid #252b30",
    borderRadius: "2px",
    color: "#6a6865",
    fontSize: "9px",
    cursor: "pointer",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    fontFamily: "'Share Tech Mono', monospace",
    userSelect: "none",
  },
  searchBox: { padding: "6px 8px", borderBottom: "1px solid #1e2428" },
  search: {
    width: "100%",
    background: "#0f1214",
    border: "1px solid #252b30",
    borderRadius: "3px",
    color: "#8a8880",
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "9px",
    padding: "4px 6px",
    outline: "none",
    letterSpacing: "0.5px",
  },
  list: {
    flex: 1,
    overflowY: "auto",
    padding: "6px 0",
  },
  row: {
    display: "flex",
    alignItems: "center",
    gap: "6px",
    padding: "6px 10px",
    cursor: "pointer",
    userSelect: "none",
    transition: "background 0.15s",
    border: "1px solid transparent",
    borderRadius: "4px",
    margin: "2px 6px",
  },
  cb: {
    width: "13px",
    height: "13px",
    border: "1px solid #2a3038",
    borderRadius: "2px",
    flexShrink: 0,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    transition: "all .15s",
  },
  lIcon: { fontSize: "11px", width: "14px", textAlign: "center", flexShrink: 0 },
  lLabel: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "9px",
    color: "#8a8880",
    letterSpacing: "0.5px",
    whiteSpace: "nowrap",
    overflow: "hidden",
    textOverflow: "ellipsis",
  },
  stats: {
    borderTop: "1px solid #1e2428",
    padding: "8px 10px",
    display: "flex",
    justifyContent: "space-around",
    alignItems: "center",
  },
  stat: { display: "flex", flexDirection: "column", alignItems: "center", gap: "2px" },
  statVal: ({ color }) => ({
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "14px",
    color,
    letterSpacing: "0.5px",
    fontWeight: 700,
  }),
  statKey: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "8px",
    color: "#4a4845",
    letterSpacing: "1px",
  },
  statDiv: {
    width: "1px",
    height: "24px",
    background: "#1e2428",
  },
  mapWrap: {
    flex: 1,
    position: "relative",
    overflow: "hidden",
  },
  map: {
    width: "100%",
    height: "100%",
    position: "absolute",
    top: 0,
    left: 0,
  },
  zoom: {
    position: "absolute",
    top: "8px",
    right: "8px",
    zIndex: 1001,
    display: "flex",
    flexDirection: "column",
    gap: "2px",
  },
  zBtn: {
    width: "24px",
    height: "24px",
    background: "rgba(10,12,14,0.9)",
    border: "1px solid #2a3038",
    borderRadius: "3px",
    color: "#8a8880",
    fontSize: "13px",
    cursor: "pointer",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    fontFamily: "monospace",
    transition: "all 0.15s",
  },
  hubBadge: {
    position: "absolute",
    bottom: "8px",
    left: "8px",
    zIndex: 1001,
    background: "rgba(10,12,14,0.88)",
    border: "1px solid #2a2218",
    borderRadius: "3px",
    padding: "4px 8px",
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
  },
  hubKey: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "8px",
    color: "#4a4845",
    letterSpacing: "1px",
  },
  hubVal: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "11px",
    color: "#c8922a",
    letterSpacing: "1px",
  },
  webgl: {
    position: "absolute",
    bottom: "8px",
    right: "8px",
    zIndex: 1001,
    background: "rgba(0,200,100,0.1)",
    border: "1px solid rgba(61,220,132,0.3)",
    color: "#3ddc84",
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "9px",
    padding: "3px 6px",
    borderRadius: "2px",
    letterSpacing: "1px",
  },
  viewToggle: {
    display: "flex",
    gap: "2px",
    background: "#0f1214",
    border: "1px solid #1e2428",
    borderRadius: "4px",
    padding: "2px",
  },
  toggleBtn: {
    padding: "3px 10px",
    background: "transparent",
    border: "none",
    color: "#6a6865",
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "9px",
    borderRadius: "2px",
    cursor: "pointer",
    transition: "all 0.15s",
  },
  toggleBtnActive: {
    background: "#1e1a12",
    color: "#c8922a",
    border: "1px solid #3a2e18",
  },
  detailPanel: {
    position: "absolute",
    bottom: "0",
    left: "220px",
    right: "0",
    background: "rgba(10,12,14,0.95)",
    borderTop: "1px solid #1a1e22",
    padding: "10px 16px",
    zIndex: 100,
    backdropFilter: "blur(8px)",
    animation: "slideIn 0.3s ease-out",
  },
  detailHeader: {
    display: "flex",
    alignItems: "center",
    gap: "8px",
  },
  detailDot: {
    width: "8px",
    height: "8px",
    borderRadius: "50%",
    flexShrink: 0,
  },
  detailName: {
    fontFamily: "'Orbitron', monospace",
    fontSize: "12px",
    color: "#c8922a",
    fontWeight: 700,
  },
  detailLabel: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "9px",
    border: "1px solid",
    borderRadius: "2px",
    padding: "1px 6px",
    letterSpacing: "0.5px",
  },
  detailClose: {
    marginLeft: "auto",
    background: "transparent",
    border: "none",
    color: "#6a6865",
    fontSize: "14px",
    cursor: "pointer",
    padding: "0 4px",
  },
  detailBody: {
    display: "flex",
    gap: "16px",
    marginTop: "4px",
  },
  detailConnections: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "10px",
    color: "#6a6865",
  },
  detailLocation: {
    fontFamily: "'Share Tech Mono', monospace",
    fontSize: "10px",
    color: "#4a4845",
  },
};