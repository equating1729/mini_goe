import { useEffect, useRef, useState } from "react";
import { Group, Panel, Separator } from "react-resizable-panels";
import { API } from "../config";

const LAYERS_DATA = [
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
  },
  { id: "tech_entities", icon: "⬜", label: "TECH ENTITIES", color: "#ffb303" },
  {
    id: "climate_entities",
    icon: "⊙",
    label: "CLIMATE ENTITIES",
    color: "#4b00a0",
  },
  { id: "person_entities", icon: "●", label: "KEY PERSONS", color: "#e6f700" },
];
const DOMAIN_LAYER_MAP = {
  ALL: [
    "geo_entities",
    "defense_entities",
    "tech_entities",
    "climate_entities",
    "person_entities",
  ],
  GEO: ["geo_entities", "person_entities"],
  DEFENSE: ["defense_entities", "person_entities"],
  TECH: ["tech_entities"],
  CLIMATE: ["climate_entities"],
};

const COUNTRY_COORDS = {
  Iran: [32.4, 53.7],
  India: [20.6, 78.9],
  Israel: [31.0, 35.0],
  "U.S.": [38.9, -77.0],
  Pakistan: [30.4, 69.3],
  China: [35.9, 104.2],
  Russia: [61.5, 105.3],
  Turkey: [38.9, 35.2],
  "Saudi Arabia": [23.9, 45.1],
  Ukraine: [48.4, 31.2],
  France: [46.2, 2.2],
  Germany: [51.2, 10.4],
  UK: [55.4, -3.4],
  Congress: [38.9, -77.0],
  UN: [40.7, -74.0],
  Hamas: [31.5, 34.5],
  NATO: [50.9, 4.3],
  Gaza: [31.4, 34.3],
};

function markerHtml(color, pulse) {
  if (pulse) {
    return `<div style="width:22px;height:22px;position:relative;display:flex;align-items:center;justify-content:center;">
      <div style="position:absolute;width:20px;height:20px;border-radius:50%;background:${color};opacity:0.25;animation:mapPulse 1.8s ease-out infinite;"></div>
      <div style="width:8px;height:8px;border-radius:50%;background:${color};box-shadow:0 0 8px ${color};z-index:2;border:1px solid rgba(255,255,255,0.3);"></div>
    </div>`;
  }
  return `<div style="width:9px;height:9px;border-radius:50%;background:${color};box-shadow:0 0 6px ${color};border:1px solid rgba(255,255,255,0.25);"></div>`;
}
const DEFENSE_KEYWORDS = [
  "military",
  "army",
  "navy",
  "defence",
  "defense",
  "pentagon",
  "nato",
  "missile",
];
const TECH_KEYWORDS = [
  "google",
  "microsoft",
  "openai",
  "isro",
  "nasa",
  "meta",
  "apple",
  "inc42",
];

function classifyEntity(entity, activeDomain) {
  if (entity.label === "PERSON") return "person_entities";
  if (activeDomain === "DEFENSE") return "defense_entities";
  if (activeDomain === "TECH") return "tech_entities";
  if (activeDomain === "CLIMATE") return "climate_entities";
  return "geo_entities";
}

export default function WorldMap({ activeDomain = "ALL" }) {
  const mapRef = useRef(null);
  const mapObj = useRef(null);
  const groups = useRef({});
  const [search, setSearch] = useState("");
  const [intelEntities, setIntelEntities] = useState([]);
  const [states, setStates] = useState(() => {
    const s = {};
    LAYERS_DATA.forEach((l) => (s[l.id] = !!l.defaultOn));
    return s;
  });
  useEffect(() => {
    const activeLayerIds =
      DOMAIN_LAYER_MAP[activeDomain] || DOMAIN_LAYER_MAP.ALL;
    setStates((prev) => {
      const next = {};
      LAYERS_DATA.forEach((l) => {
        next[l.id] = activeLayerIds.includes(l.id);
      });
      return next;
    });
  }, [activeDomain]);
  useEffect(() => {
    if (mapObj.current || !mapRef.current) return;
    const L = window.L;
    if (!L) return;

    const map = L.map(mapRef.current, {
      center: [20, 20],
      zoom: 2,
      zoomControl: false,
      minZoom: 2,
      maxZoom: 10,
      attributionControl: false,
    });
    setTimeout(() => {
      map.invalidateSize();
    }, 300);

    L.tileLayer(
      "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
      {
        maxZoom: 19,
      },
    ).addTo(map);

    LAYERS_DATA.forEach((layer) => {
      const group = L.layerGroup();
      groups.current[layer.id] = group;
      if (layer.defaultOn) group.addTo(map);
    });
    mapObj.current = map;

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
    const domainParam = activeDomain !== "ALL" ? `&domain=${activeDomain}` : "";
    fetch(`${API}/graph/entities?limit=30${domainParam}`)
      .then((r) => r.json())
      .then((data) => setIntelEntities(data))
      .catch(() => {});
  }, [activeDomain]);

  useEffect(() => {
    const map = mapObj.current;
    if (!map) return;
    LAYERS_DATA.forEach((l) => {
      const g = groups.current[l.id];
      if (!g) return;
      states[l.id]
        ? !map.hasLayer(g) && g.addTo(map)
        : map.hasLayer(g) && map.removeLayer(g);
    });
  }, [states]);

  const toggle = (id) => setStates((prev) => ({ ...prev, [id]: !prev[id] }));
  const filtered = LAYERS_DATA.filter((l) =>
    l.label.includes(search.toUpperCase()),
  );
  useEffect(() => {
    const map = mapObj.current;
    const L = window.L;
    if (!map || !L || intelEntities.length === 0) return;

    LAYERS_DATA.forEach((l) => {
      groups.current[l.id]?.clearLayers();
    });

    const maxConn = Math.max(...intelEntities.map((e) => e.connections || 1));

    intelEntities.forEach((entity) => {
      const coords = COUNTRY_COORDS[entity.name];
      if (!coords) return;

      const bucket = classifyEntity(entity, activeDomain);
      const group = groups.current[bucket];
      if (!group) return;

      const layerColor =
        LAYERS_DATA.find((l) => l.id === bucket)?.color || "#c8922a";
      const size = Math.max(
        8,
        Math.min(24, (entity.connections / maxConn) * 24),
      );

      const html = `
      <div style="position:relative;display:flex;align-items:center;justify-content:center;width:${size + 8}px;height:${size + 8}px;">
        <div style="position:absolute;width:${size + 8}px;height:${size + 8}px;border-radius:50%;background:${layerColor};opacity:0.15;animation:mapPulse 2s ease-out infinite;"></div>
        <div style="width:${size}px;height:${size}px;border-radius:50%;background:${layerColor};opacity:0.85;box-shadow:0 0 ${size}px ${layerColor};border:1px solid rgba(255,255,255,0.3);display:flex;align-items:center;justify-content:center;">
          <span style="font-family:'Share Tech Mono',monospace;font-size:${Math.max(6, size / 3)}px;color:#000;font-weight:700;">${entity.connections}</span>
        </div>
      </div>`;

      const icon = L.divIcon({
        html,
        className: "",
        iconSize: [size + 8, size + 8],
        iconAnchor: [(size + 8) / 2, (size + 8) / 2],
      });

      L.marker(coords, { icon })
        .bindPopup(
          `<div style="font-family:'Share Tech Mono',monospace;font-size:11px;color:#d4cfc8;background:#0f1214;border:1px solid #2a3038;padding:8px 12px;border-radius:3px;min-width:160px;">
          <div style="color:${layerColor};font-weight:700;margin-bottom:4px;">${entity.name}</div>
          <div style="color:#6a6865;">${entity.label} · ${entity.connections} connections</div>
        </div>`,
          { className: "goe-popup", closeButton: false },
        )
        .addTo(group);
    });
  }, [intelEntities, activeDomain]);
  return (
    <div style={s.root}>
      <style>{`
        @keyframes mapPulse { 0%{transform:scale(1);opacity:.4} 100%{transform:scale(3.5);opacity:0} }
        .leaflet-container { background:#080c10 !important; }
        .goe-popup .leaflet-popup-content-wrapper { background:transparent!important;border:none!important;box-shadow:none!important;padding:0!important; }
        .goe-popup .leaflet-popup-tip-container { display:none!important; }
        .goe-popup .leaflet-popup-content { margin:0!important; }
        #layer-search::placeholder { color:#3a3835; }
      `}</style>

      <Group orientation="horizontal" style={{ width: "100%", height: "100%" }}>
        {/* ── LAYERS PANEL ── */}
        <Panel defaultSize={25} minSize={15}>
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
                id="layer-search"
                style={s.search}
                placeholder="Search layers..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
              />
            </div>

            <div style={s.list}>
              {filtered.map((l) => (
                <div key={l.id} style={s.row} onClick={() => toggle(l.id)}>
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
                  <span style={{ ...s.lIcon, color: l.color }}>{l.icon}</span>
                  <span style={s.lLabel}>{l.label}</span>
                </div>
              ))}
            </div>
          </div>
        </Panel>

        {/* ── RESIZE HANDLE ── */}
        <Separator
          style={{
            width: "6px",
            background: "#1a1e22",
            cursor: "col-resize",
          }}
        />

        {/* ── MAP PANEL ── */}
        <Panel defaultSize={75}>
          <div ref={mapRef} style={s.map} />
        </Panel>
      </Group>
    </div>
  );
}

const s = {
  root: {
    width: "100%",
    height: "100%",
    position: "relative",
    overflow: "hidden",
    background: "#080c10",
    border: "1px solid #1a1e22",
    borderRadius: "8px",
  },
  panel: {
    height: "100%",
    background: "rgba(10,12,14,0.93)",
    borderRight: "1px solid #1e2428",
    display: "flex",
    flexDirection: "column",
    backdropFilter: "blur(6px)",
  },
  pHead: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "10px 12px",
    borderBottom: "1px solid #1e2428",
  },
  pTitle: {
    fontFamily: "'Share Tech Mono',monospace",
    fontSize: "12px",
    color: "#8a8880",
    letterSpacing: "2px",
  },
  pIcons: { display: "flex", gap: "5px" },
  iBtn: {
    width: "22px",
    height: "22px",
    background: "#1a1e22",
    border: "1px solid #252b30",
    borderRadius: "3px",
    color: "#6a6865",
    fontSize: "10px",
    cursor: "pointer",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    fontFamily: "'Share Tech Mono',monospace",
    userSelect: "none",
  },
  searchBox: { padding: "8px 10px", borderBottom: "1px solid #1e2428" },
  search: {
    width: "100%",
    background: "#0f1214",
    border: "1px solid #252b30",
    borderRadius: "3px",
    color: "#8a8880",
    fontFamily: "'Share Tech Mono',monospace",
    fontSize: "11px",
    padding: "5px 8px",
    outline: "none",
    letterSpacing: "0.5px",
  },
  list: { flex: 1, overflowY: "auto", padding: "4px 0" },
  row: {
    display: "flex",
    alignItems: "center",
    gap: "8px",
    padding: "7px 12px",
    cursor: "pointer",
    userSelect: "none",
  },
  cb: {
    width: "14px",
    height: "14px",
    border: "1px solid #2a3038",
    borderRadius: "2px",
    flexShrink: 0,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    transition: "all .15s",
  },
  lIcon: {
    fontSize: "12px",
    width: "16px",
    textAlign: "center",
    flexShrink: 0,
  },
  lLabel: {
    fontFamily: "'Share Tech Mono',monospace",
    fontSize: "10px",
    color: "#8a8880",
    letterSpacing: "0.8px",
    whiteSpace: "nowrap",
  },
  pFoot: { padding: "8px 12px", borderTop: "1px solid #1e2428" },
  credit: {
    fontFamily: "'Share Tech Mono',monospace",
    fontSize: "10px",
    color: "#3a3835",
  },
  map: { flex: 1, width: "100%", height: "100%", zIndex: 1 },
  zoom: {
    position: "absolute",
    top: "10px",
    right: "10px",
    zIndex: 1001,
    display: "flex",
    flexDirection: "column",
    gap: "2px",
  },
  zBtn: {
    width: "28px",
    height: "28px",
    background: "rgba(10,12,14,0.9)",
    border: "1px solid #2a3038",
    borderRadius: "3px",
    color: "#8a8880",
    fontSize: "14px",
    cursor: "pointer",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    fontFamily: "monospace",
  },
  legend: {
    position: "absolute",
    bottom: "12px",
    left: "50%",
    transform: "translateX(-50%)",
    zIndex: 1001,
    background: "rgba(10,12,14,0.9)",
    border: "1px solid #2a3038",
    borderRadius: "3px",
    color: "#8a8880",
    fontFamily: "'Share Tech Mono',monospace",
    fontSize: "11px",
    padding: "6px 18px",
    letterSpacing: "1.5px",
    cursor: "pointer",
  },
  webgl: {
    position: "absolute",
    bottom: "8px",
    right: "8px",
    zIndex: 1001,
    background: "rgba(0,200,100,0.1)",
    border: "1px solid rgba(61,220,132,0.3)",
    color: "#3ddc84",
    fontFamily: "'Share Tech Mono',monospace",
    fontSize: "10px",
    padding: "3px 7px",
    borderRadius: "2px",
    letterSpacing: "1px",
  },
};
