import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, test, vi } from "vitest";
import type { NodeMetrics, NodeStats } from "@/lib/metrics";
import { diskPercent } from "@/lib/metrics";
import { NodeMonitoring } from "./node-monitoring";

vi.mock("@/lib/use-polling", () => ({
  usePolling: () => ({ loading: false, data: undefined, error: "" }),
}));
afterEach(cleanup);

const snap = {
  device: "/dev/loop0",
  mount: "/snap/core22/2133",
  filesystem: "squashfs",
  total_bytes: 100,
  available_bytes: 0,
  used_bytes: 100,
  inodes: 100,
  free_inodes: 0,
};
const root = {
  ...snap,
  device: "/dev/loop1",
  mount: "/data",
  filesystem: "ext4",
  available_bytes: 50,
  used_bytes: 50,
  free_inodes: 50,
};
function node(filesystems: NodeMetrics["filesystems"]): NodeStats {
  return {
    id: "node-1",
    address: "127.0.0.1",
    stale: false,
    age_seconds: 0,
    history: [],
    latest: {
      received_at_unix_ms: 1000,
      metrics: {
        sampled_at_unix_ms: 1000,
        interval_seconds: 5,
        uptime_seconds: 100,
        cpu_count: 2,
        cpu_percent: null,
        io_wait_percent: null,
        load_average: null,
        memory: null,
        filesystems,
        disks: [],
        networks: [],
        errors: [],
      },
    },
  };
}
function show(sample: NodeStats) {
  render(
    <NodeMonitoring
      id={sample.id}
      node={sample}
      elapsed={0}
      staleAfter={30}
      now={1000}
    />,
  );
}

test("Snap images do not affect disk usage or appear in filesystem rows", () => {
  const sample = node([snap, root]);
  expect(diskPercent(sample.latest!.metrics)).toBe(50);
  show(sample);
  expect(screen.queryByText(snap.mount)).toBeNull();
  expect(screen.getByText(root.mount)).toBeTruthy();
  expect(screen.queryByText("100.0%")).toBeNull();

  cleanup();
  const full = node([snap, { ...root, used_bytes: 100, available_bytes: 0 }]);
  expect(diskPercent(full.latest!.metrics)).toBe(100);
  show(full);
  expect(screen.getByText(root.mount)).toBeTruthy();
  expect(screen.getAllByText("100.0%").length).toBeGreaterThan(0);
});

test("a sample containing only Snap images shows no filesystem data", () => {
  const sample = node([snap]);
  expect(diskPercent(sample.latest!.metrics)).toBeNull();
  show(sample);
  expect(screen.getByText("No local filesystem data available.")).toBeTruthy();
  expect(screen.queryByText(snap.mount)).toBeNull();
  expect(screen.queryByText("100.0%")).toBeNull();
});
