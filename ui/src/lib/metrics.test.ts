import { expect, test } from "vitest";
import {
  chartSegments,
  freshness,
  sum,
  type MetricPoint,
  type NodeStats,
} from "./metrics";
const point = (time: number, cpu: number | null): MetricPoint => ({
  received_at_unix_ms: time,
  cpu_percent: cpu,
  memory_percent: null,
  disk_read: null,
  disk_write: null,
  net_receive: null,
  net_transmit: null,
  sample_count: 1,
  peaks: [cpu, null, null, null, null, null],
});
test("rates remain unavailable when a contributing device is missing", () => {
  expect(sum([])).toBeNull();
  expect(sum([4, null])).toBeNull();
  expect(sum([0, 0])).toBe(0);
});
test("charts preserve missing readings and reporting gaps", () => {
  const samples = [
    point(5000, 10),
    point(10000, 20),
    point(15000, null),
    point(20000, 40),
    point(60000, 50),
  ];
  const paths = chartSegments(samples, (p) => p.cpu_percent, 0, 60000, 100);
  expect(paths).toHaveLength(3);
  expect(paths[0]).toContain(" L");
  expect(paths[1]).not.toContain(" L");
  expect(
    chartSegments(
      [point(3600000, 10), point(7200000, 20)],
      (p) => p.cpu_percent,
      0,
      86400000,
      100,
      3600,
    ),
  ).toHaveLength(1);
});
test("cached readings become stale even when polling fails", () => {
  const node = {
    latest: { metrics: { errors: [] } },
    age_seconds: 5,
    stale: false,
  } as unknown as NodeStats;
  expect(freshness(node, 0, 30)).toBe("Reporting");
  expect(freshness(node, 26, 30)).toBe("Stale");
  expect(freshness(undefined)).toBe("No data");
});
