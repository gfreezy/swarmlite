export interface NodeMetrics {
  sampled_at_unix_ms: number;
  interval_seconds: number | null;
  uptime_seconds: number | null;
  cpu_count: number;
  cpu_percent: number | null;
  io_wait_percent: number | null;
  load_average: number[] | null;
  memory: {
    total_bytes: number;
    available_bytes: number;
    swap_total_bytes: number;
    swap_free_bytes: number;
  } | null;
  filesystems: {
    device: string;
    mount: string;
    filesystem: string;
    total_bytes: number;
    available_bytes: number;
    used_bytes: number;
    inodes: number;
    free_inodes: number;
  }[];
  disks: {
    device: string;
    aggregate: boolean;
    read_bytes_per_second: number | null;
    write_bytes_per_second: number | null;
    read_iops: number | null;
    write_iops: number | null;
    busy_percent: number | null;
  }[];
  networks: {
    interface: string;
    aggregate: boolean;
    receive_bytes_per_second: number | null;
    transmit_bytes_per_second: number | null;
    receive_errors: number;
    transmit_errors: number;
    receive_dropped: number;
    transmit_dropped: number;
  }[];
  errors: string[];
}
export interface MetricSample {
  received_at_unix_ms: number;
  metrics: NodeMetrics;
}
export interface MetricPoint {
  received_at_unix_ms: number;
  cpu_percent: number | null;
  memory_percent: number | null;
  disk_read: number | null;
  disk_write: number | null;
  net_receive: number | null;
  net_transmit: number | null;
  sample_count: number;
  peaks: (number | null)[];
}
export interface NodeStats {
  id: string;
  address: string;
  stale: boolean;
  age_seconds: number | null;
  latest: MetricSample | null;
  history: MetricPoint[];
}
export interface NodeStatsResponse {
  nodes: NodeStats[];
  retention_seconds: number;
  resolution_seconds: number;
  history_range_seconds: number;
  range_start_unix_ms: number;
  range_end_unix_ms: number;
  stale_after_seconds: number;
  storage_error?: string | null;
}
export const ratio = (used: number, total: number) =>
  total > 0 ? (100 * used) / total : null;
export const memoryPercent = (m: NodeMetrics) =>
  m.memory
    ? ratio(
        Math.max(0, m.memory.total_bytes - m.memory.available_bytes),
        m.memory.total_bytes,
      )
    : null;
export const diskPercent = (m: NodeMetrics) => {
  const values = m.filesystems
    .map((f) => ratio(f.used_bytes, f.used_bytes + f.available_bytes))
    .filter((v): v is number => v !== null);
  return values.length ? Math.max(...values) : null;
};
export const sum = (values: (number | null)[]) =>
  values.length && values.every((v) => v !== null)
    ? values.reduce<number>((a, b) => a + b!, 0)
    : null;
export const diskRead = (m: NodeMetrics) =>
  sum(m.disks.filter((d) => d.aggregate).map((d) => d.read_bytes_per_second));
export const diskWrite = (m: NodeMetrics) =>
  sum(m.disks.filter((d) => d.aggregate).map((d) => d.write_bytes_per_second));
export const netReceive = (m: NodeMetrics) =>
  sum(
    m.networks
      .filter((n) => n.aggregate)
      .map((n) => n.receive_bytes_per_second),
  );
export const netTransmit = (m: NodeMetrics) =>
  sum(
    m.networks
      .filter((n) => n.aggregate)
      .map((n) => n.transmit_bytes_per_second),
  );
export const percent = (v: number | null | undefined) =>
  v == null ? "—" : `${v.toFixed(1)}%`;
export const bytes = (v: number | null | undefined) => {
  if (v == null) return "—";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(1)} ${units[i]}`;
};
export const speed = (v: number | null | undefined) =>
  v == null ? "—" : `${bytes(v)}/s`;
export const metricTone = (v: number | null | undefined) =>
  v == null ? "muted" : v >= 90 ? "critical" : v >= 75 ? "warning" : "normal";
export function freshness(
  node: NodeStats | undefined,
  elapsed = 0,
  staleAfter = 30,
) {
  if (!node?.latest) return "No data";
  if (node.stale || (node.age_seconds ?? 0) + elapsed > staleAfter)
    return "Stale";
  return node.latest.metrics.errors.length ? "Partial" : "Reporting";
}
// Gaps remain gaps; never bridge an unavailable sample or a pause in reporting.
export function chartSegments(
  samples: MetricPoint[],
  pick: (m: MetricPoint) => number | null,
  start: number,
  end: number,
  max: number,
  resolution = 5,
) {
  const paths: string[] = [];
  let last = 0;
  let path = "";
  for (const sample of samples) {
    const time = sample.received_at_unix_ms,
      value = pick(sample);
    if (time + resolution * 1000 < start || time > end) continue;
    if (value == null) {
      if (path) paths.push(path);
      path = "";
      last = 0;
      continue;
    }
    const gap = time - last > Math.max(15_000, resolution * 3000);
    if (gap && path) {
      paths.push(path);
      path = "";
    }
    const x = 4 + ((Math.max(start, time) - start) / (end - start)) * 392,
      y = 78 - Math.min(1, Math.max(0, value / max)) * 70;
    path += `${path ? " L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`;
    last = time;
  }
  if (path) paths.push(path);
  return paths;
}
