import type { MonitoringRange } from "@/lib/monitoring-range";
import { useState } from "react";
import { Activity, Cpu, MemoryStick, HardDrive, Network } from "lucide-react";
import { MonitoringRangePicker } from "@/components/monitoring-range";
import { usePolling } from "@/lib/use-polling";
import {
  Table,
  TableHeader,
  TableRow,
  TableHead,
  TableBody,
  TableCell,
} from "@/components/ui/table";
import { Empty } from "@/components/status";
import { get, timestamp } from "@/lib/api";
import {
  type NodeMetrics,
  type NodeStats,
  type MetricPoint,
  type NodeStatsResponse,
  percent,
  bytes,
  speed,
  ratio,
  memoryPercent,
  diskPercent,
  diskRead,
  diskWrite,
  netReceive,
  netTransmit,
  metricTone,
  freshness,
  chartSegments,
} from "@/lib/metrics";

export function MetricValue({ value }: { value: number | null | undefined }) {
  return (
    <span className={`node-metric-value ${metricTone(value)}`}>
      {percent(value)}
    </span>
  );
}
export function MetricState({ state }: { state: string }) {
  return (
    <span className={`metric-state ${state.toLowerCase().replace(" ", "-")}`}>
      <i />
      {state}
    </span>
  );
}
function Chart({
  history,
  pick,
  second,
  end,
  seconds,
  resolution,
  percentage,
  label,
  peakIndex,
}: {
  history: MetricPoint[];
  pick: (m: MetricPoint) => number | null;
  second?: (m: MetricPoint) => number | null;
  end: number;
  seconds: number;
  resolution: number;
  percentage?: boolean;
  label: string;
  peakIndex: number;
}) {
  const [hover, setHover] = useState<number | null>(null);
  const start = end - seconds * 1000;
  const samples = history.filter(
    (s) =>
      s.received_at_unix_ms + resolution * 1000 >= start &&
      s.received_at_unix_ms <= end,
  );
  const values = samples
    .flatMap((s) => [pick(s), second?.(s)])
    .filter((v): v is number => v != null);
  const peaks = samples
    .map((s) => s.peaks[peakIndex])
    .filter((v): v is number => v != null);
  const peak = peaks.length ? Math.max(...peaks) : null;
  const max = percentage ? 100 : Math.max(1, ...values) * 1.15;
  const active =
    hover == null || !samples.length
      ? undefined
      : samples.reduce((a, b) =>
          Math.abs(a.received_at_unix_ms - hover) <
          Math.abs(b.received_at_unix_ms - hover)
            ? a
            : b,
        );
  const x = (t: number) => 4 + Math.max(0, (t - start) / (end - start)) * 392;
  const valueText = percentage ? percent : speed;
  const dateText = (t: number) =>
    new Date(t).toLocaleString(
      undefined,
      seconds <= 86400
        ? { hour: "2-digit", minute: "2-digit" }
        : { month: "short", day: "numeric" },
    );
  const primary = second ? (peakIndex === 2 ? "Read" : "Receive") : "Usage";
  const secondary = peakIndex === 2 ? "Write" : "Transmit";
  return (
    <div className="metric-chart">
      <div className="metric-chart-scale">
        <span>{percentage ? "100%" : speed(max)}</span>
        <span className="chart-series-legend">
          <i />
          {primary}
          {second && (
            <>
              <i className="secondary" />
              {secondary}
            </>
          )}
        </span>
      </div>
      <div className="metric-chart-plot" onPointerLeave={() => setHover(null)}>
        <svg
          viewBox="0 0 400 88"
          preserveAspectRatio="none"
          role="img"
          tabIndex={0}
          aria-label={`${label} history chart; arrow keys explore samples`}
          onPointerMove={(e) => {
            const r = e.currentTarget.getBoundingClientRect();
            setHover(
              start +
                Math.max(0, Math.min(1, (e.clientX - r.left) / r.width)) *
                  seconds *
                  1000,
            );
          }}
          onFocus={() => {
            if (samples.length)
              setHover(samples[samples.length - 1].received_at_unix_ms);
          }}
          onBlur={() => setHover(null)}
          onKeyDown={(e) => {
            if (!samples.length || !["ArrowLeft", "ArrowRight"].includes(e.key))
              return;
            e.preventDefault();
            const at = active ? samples.indexOf(active) : samples.length - 1;
            setHover(
              samples[
                Math.max(
                  0,
                  Math.min(
                    samples.length - 1,
                    at + (e.key === "ArrowLeft" ? -1 : 1),
                  ),
                )
              ].received_at_unix_ms,
            );
          }}
        >
          {[8, 43, 78].map((y) => (
            <line
              key={y}
              x1="4"
              x2="396"
              y1={y}
              y2={y}
              className="metric-gridline"
            />
          ))}
          {chartSegments(samples, pick, start, end, max, resolution).map(
            (d, i) => (
              <path key={`a${i}`} d={d} className="metric-line" />
            ),
          )}
          {second &&
            chartSegments(samples, second, start, end, max, resolution).map(
              (d, i) => (
                <path key={`b${i}`} d={d} className="metric-line secondary" />
              ),
            )}
          {samples.length === 1 && pick(samples[0]) != null && (
            <circle
              cx={x(samples[0].received_at_unix_ms)}
              cy={78 - (pick(samples[0])! / max) * 70}
              r="2.5"
              fill="#347553"
            />
          )}
          {active && (
            <line
              x1={x(active.received_at_unix_ms)}
              x2={x(active.received_at_unix_ms)}
              y1="4"
              y2="84"
              className="metric-crosshair"
            />
          )}
        </svg>
        {!values.length && (
          <span className="metric-chart-empty">No samples in this range</span>
        )}
        {active && (
          <div className="metric-tooltip" role="status">
            <strong>{timestamp(active.received_at_unix_ms)}</strong>
            <span>
              {primary} avg <b>{valueText(pick(active))}</b>
            </span>
            <span>
              {primary} peak <b>{valueText(active.peaks[peakIndex])}</b>
            </span>
            {second && (
              <>
                <span>
                  {secondary} avg <b>{valueText(second(active))}</b>
                </span>
                <span>
                  {secondary} peak{" "}
                  <b>{valueText(active.peaks[peakIndex + 1])}</b>
                </span>
              </>
            )}
            <small>{active.sample_count} samples</small>
          </div>
        )}
      </div>
      <div className="metric-time-axis">
        {[0, 0.5, 1].map((f) => (
          <span key={f}>{dateText(start + seconds * 1000 * f)}</span>
        ))}
      </div>
      <div className="metric-chart-footer">
        <span>{primary} peak</span>
        <strong>{valueText(peak)}</strong>
      </div>
    </div>
  );
}
export function NodeMonitoring({
  id,
  node,
  elapsed,
  staleAfter,
  now,
}: {
  id: string;
  node?: NodeStats;
  elapsed: number;
  staleAfter: number;
  now: number;
}) {
  const [range, setRange] = useState<MonitoringRange>({ seconds: 300 });
  const rangeQuery =
    range.from !== undefined
      ? `from=${range.from}&to=${range.to}`
      : `seconds=${range.seconds}`;
  const history = usePolling(
    async (signal) => ({
      response: await get<NodeStatsResponse>(
        `/node-stats?node=${encodeURIComponent(id)}&history=true&${rangeQuery}`,
        signal,
      ),
      rangeQuery,
    }),
    `metrics:${id}:${rangeQuery}`,
  );
  const response =
    history.data?.rangeQuery === rangeQuery ? history.data.response : undefined;
  const historyData = response?.nodes.find((n) => n.id === id);
  const points = historyData?.history || [];
  const resolution = response?.resolution_seconds ?? 5;
  const seconds =
    range.from !== undefined ? (range.to - range.from) / 1000 : range.seconds;
  const chartEnd = range.to ?? response?.range_end_unix_ms ?? now;
  const granularity =
    resolution < 60
      ? `${resolution}s`
      : resolution < 3600
        ? `${resolution / 60}m`
        : resolution < 86400
          ? `${resolution / 3600}h`
          : `${resolution / 86400}d`;
  const state = freshness(node, elapsed, staleAfter);
  const m: NodeMetrics | undefined =
    node?.latest?.metrics ??
    (points.length
      ? {
          sampled_at_unix_ms: 0,
          interval_seconds: null,
          uptime_seconds: null,
          cpu_count: 0,
          cpu_percent: null,
          io_wait_percent: null,
          load_average: null,
          memory: null,
          filesystems: [],
          disks: [],
          networks: [],
          errors: [],
        }
      : undefined);
  return (
    <section className="node-monitoring" aria-label="Node monitoring">
      <div className="section-heading monitoring-heading">
        <div>
          <h2>
            <Activity size={17} /> Host monitoring
          </h2>
          <p>
            {node?.latest
              ? `Received ${timestamp(node.latest.received_at_unix_ms)} · Sample every 5s`
              : "Linux host metrics reported by the Agent"}
          </p>
        </div>
        <div className="action-group">
          <MetricState state={state} />
        </div>
      </div>
      <div className="monitoring-toolbar">
        <MonitoringRangePicker value={range} onChange={setRange} />
        <span className="monitoring-granularity">
          Auto interval <strong>{granularity}</strong>
        </span>
      </div>
      <div className="monitoring-period">
        <span>
          {timestamp(chartEnd - seconds * 1000)} — {timestamp(chartEnd)}
        </span>
        <span>
          {range.from !== undefined ? "Fixed range" : "Live · updates every 5s"}{" "}
          · local time
        </span>
      </div>
      {(history.error || response?.storage_error) && (
        <p className="monitoring-warning" role="status">
          {history.error || response?.storage_error}
        </p>
      )}
      {!m ? (
        <Empty>
          No metrics reported yet. Upgrade the Linux Agent to a version with
          monitoring support, then allow two samples for rates.
        </Empty>
      ) : (
        <>
          {state === "Stale" && (
            <p className="monitoring-warning" role="status">
              No recent sample. Values below are the last reported readings, not
              current usage.
            </p>
          )}
          {!!m.errors.length && (
            <div className="monitoring-warning" role="status">
              <strong>Partial metrics</strong>
              {m.errors.map((e, i) => (
                <p key={i}>{e}</p>
              ))}
            </div>
          )}
          <div className="monitoring-grid">
            <article className="metric-card">
              <h3>
                <Cpu size={16} /> CPU
              </h3>
              <strong className={`metric-number ${metricTone(m.cpu_percent)}`}>
                {percent(m.cpu_percent)}
              </strong>
              <p>
                {m.cpu_count || "—"} cores · I/O wait{" "}
                {percent(m.io_wait_percent)}
              </p>
              <Chart
                history={points}
                pick={(p) => p.cpu_percent}
                end={chartEnd}
                seconds={seconds}
                resolution={resolution}
                percentage
                label="CPU"
                peakIndex={0}
              />
            </article>
            <article className="metric-card">
              <h3>
                <MemoryStick size={16} /> Memory
              </h3>
              <strong
                className={`metric-number ${metricTone(memoryPercent(m))}`}
              >
                {percent(memoryPercent(m))}
              </strong>
              <p>
                {m.memory
                  ? `${bytes(m.memory.total_bytes - m.memory.available_bytes)} / ${bytes(m.memory.total_bytes)}`
                  : "Unavailable"}
              </p>
              <Chart
                history={points}
                pick={(p) => p.memory_percent}
                end={chartEnd}
                seconds={seconds}
                resolution={resolution}
                percentage
                label="Memory"
                peakIndex={1}
              />
            </article>
            <article className="metric-card">
              <h3>
                <HardDrive size={16} /> Disk I/O
              </h3>
              <div className="metric-pair">
                <span>
                  <small>Read</small>
                  <strong>{speed(diskRead(m))}</strong>
                </span>
                <span>
                  <small>Write</small>
                  <strong>{speed(diskWrite(m))}</strong>
                </span>
              </div>
              <p>Leaf devices · read / write</p>
              <Chart
                history={points}
                pick={(p) => p.disk_read}
                second={(p) => p.disk_write}
                end={chartEnd}
                seconds={seconds}
                resolution={resolution}
                label="Disk I/O"
                peakIndex={2}
              />
            </article>
            <article className="metric-card">
              <h3>
                <Network size={16} /> Network I/O
              </h3>
              <div className="metric-pair">
                <span>
                  <small>Receive</small>
                  <strong>{speed(netReceive(m))}</strong>
                </span>
                <span>
                  <small>Transmit</small>
                  <strong>{speed(netTransmit(m))}</strong>
                </span>
              </div>
              <p>Physical interfaces · receive / transmit</p>
              <Chart
                history={points}
                pick={(p) => p.net_receive}
                second={(p) => p.net_transmit}
                end={chartEnd}
                seconds={seconds}
                resolution={resolution}
                label="Network I/O"
                peakIndex={4}
              />
            </article>
          </div>
          <div className="monitoring-facts">
            <span>
              Load 1 / 5 / 15m{" "}
              <strong>
                {m.load_average?.map((n) => n.toFixed(2)).join(" / ") || "—"}
              </strong>
            </span>
            <span>
              Swap used{" "}
              <strong>
                {m.memory
                  ? `${bytes(m.memory.swap_total_bytes - m.memory.swap_free_bytes)} / ${bytes(m.memory.swap_total_bytes)}`
                  : "—"}
              </strong>
            </span>
            <span>
              Uptime{" "}
              <strong>
                {m.uptime_seconds == null
                  ? "—"
                  : `${(m.uptime_seconds / 86400).toFixed(1)} days`}
              </strong>
            </span>
            <span>
              Fullest filesystem <MetricValue value={diskPercent(m)} />
            </span>
          </div>
          <h3 className="monitoring-table-title">
            Filesystems{" "}
            <small>Local filesystems · bind mounts deduplicated</small>
          </h3>
          <Table>
            <TableHeader>
              <TableRow>
                {[
                  "Mount / device",
                  "Used / total",
                  "Available",
                  "Usage",
                  "Inodes used",
                ].map((h) => (
                  <TableHead key={h}>{h}</TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {m.filesystems.map((f) => (
                <TableRow key={`${f.device}:${f.mount}`}>
                  <TableCell>
                    <strong>{f.mount}</strong>
                    <small className="block muted">
                      {f.device} · {f.filesystem}
                    </small>
                  </TableCell>
                  <TableCell>
                    {bytes(f.used_bytes)} / {bytes(f.total_bytes)}
                  </TableCell>
                  <TableCell>{bytes(f.available_bytes)}</TableCell>
                  <TableCell>
                    <MetricValue
                      value={ratio(
                        f.used_bytes,
                        f.used_bytes + f.available_bytes,
                      )}
                    />
                  </TableCell>
                  <TableCell>
                    <MetricValue
                      value={ratio(f.inodes - f.free_inodes, f.inodes)}
                    />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          {!m.filesystems.length && (
            <Empty>No local filesystem data available.</Empty>
          )}
          <h3 className="monitoring-table-title">
            Disk devices{" "}
            <small>Stacked devices are excluded from aggregate rates</small>
          </h3>
          <Table>
            <TableHeader>
              <TableRow>
                {["Device", "Read / write", "Read / write IOPS", "Busy"].map(
                  (h) => (
                    <TableHead key={h}>{h}</TableHead>
                  ),
                )}
              </TableRow>
            </TableHeader>
            <TableBody>
              {m.disks.map((d) => (
                <TableRow key={d.device}>
                  <TableCell>
                    {d.device}
                    {!d.aggregate && (
                      <small className="block muted">Stacked device</small>
                    )}
                  </TableCell>
                  <TableCell>
                    {speed(d.read_bytes_per_second)} /{" "}
                    {speed(d.write_bytes_per_second)}
                  </TableCell>
                  <TableCell>
                    {d.read_iops?.toFixed(1) ?? "—"} /{" "}
                    {d.write_iops?.toFixed(1) ?? "—"}
                  </TableCell>
                  <TableCell>
                    <MetricValue value={d.busy_percent} />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          {!m.disks.length && <Empty>No disk device data available.</Empty>}
          <h3 className="monitoring-table-title">
            Network interfaces{" "}
            <small>Errors and drops are cumulative RX / TX counters</small>
          </h3>
          <Table>
            <TableHeader>
              <TableRow>
                {[
                  "Interface",
                  "Receive / transmit",
                  "Errors RX / TX",
                  "Drops RX / TX",
                ].map((h) => (
                  <TableHead key={h}>{h}</TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {m.networks.map((n) => (
                <TableRow key={n.interface}>
                  <TableCell>
                    {n.interface}
                    {!n.aggregate && (
                      <small className="block muted">
                        Virtual · excluded from total
                      </small>
                    )}
                  </TableCell>
                  <TableCell>
                    {speed(n.receive_bytes_per_second)} /{" "}
                    {speed(n.transmit_bytes_per_second)}
                  </TableCell>
                  <TableCell>
                    {n.receive_errors} / {n.transmit_errors}
                  </TableCell>
                  <TableCell>
                    {n.receive_dropped} / {n.transmit_dropped}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          {!m.networks.length && (
            <Empty>No network interface data available.</Empty>
          )}
          <p className="section-note">
            Cards show the latest readings; charts show the selected period.
            Aggregated buckets can extend beyond custom range boundaries. CPU is
            normalized to 100% across all cores. Memory uses MemAvailable; disk
            usage includes reserved-space effects. History is stored in SQLite,
            flushed every 30s, and automatically aggregated up to daily buckets.
            Retention: 5s → 15m, 1m → 24h, 1h → 30d, 1d → 365d. Blank trends
            indicate missing samples.
          </p>
        </>
      )}
    </section>
  );
}
