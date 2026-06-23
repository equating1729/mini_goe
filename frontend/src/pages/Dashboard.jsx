import Sidebar from "../components/Sidebar";
import WorldMap from "../components/WorldMap";
import IndiaPanel from "../components/IndiaPanel";
import AIConsole from "../components/AIConsole";

import { Group, Panel, Separator } from "react-resizable-panels";

export default function Dashboard({ activeDomain }) {
  return (
    <main style={styles.main}>
      <Group orientation="horizontal" style={styles.group}>
        {/* Sidebar */}
        <Panel defaultSize={28} minSize={20} style={styles.panel}>
          <Sidebar activeDomain={activeDomain} />
        </Panel>

        <Separator style={styles.separatorVertical} />

        {/* Right Side */}
        <Panel defaultSize={72} style={styles.panel}>
          <Group orientation="vertical" style={styles.group}>
            {/* World Map */}
            <Panel defaultSize={50} minSize={25} style={styles.panel}>
              <WorldMap activeDomain={activeDomain} />
            </Panel>

            <Separator style={styles.separatorHorizontal} />

            {/* Bottom Row */}
            <Panel defaultSize={50} minSize={25} style={styles.panel}>
              <Group orientation="horizontal" style={styles.group}>
                <Panel defaultSize={50} minSize={25} style={styles.panel}>
                  <IndiaPanel domain={activeDomain} />
                </Panel>

                <Separator style={styles.separatorVertical} />

                <Panel defaultSize={50} minSize={25} style={styles.panel}>
                  <AIConsole domain = {activeDomain} />
                </Panel>
              </Group>
            </Panel>
          </Group>
        </Panel>
      </Group>
    </main>
  );
}

const styles = {
  main: {
    height: "100%",
    padding: "12px",
    overflow: "hidden",
    display: "flex",
  },

  group: {
    width: "100%",
    height: "100%",
  },

  panel: {
    display: "flex",
    minWidth: 0,
    minHeight: 0,
    overflow: "hidden",
  },

  separatorVertical: {
    width: "6px",
    background: "#1a1e22",
    cursor: "col-resize",
  },

  separatorHorizontal: {
    height: "6px",
    background: "#1a1e22",
    cursor: "row-resize",
  },
};
