import { Select } from "@/components/ui/select";
import { useState } from "react";
import { Plus, Tag, Power, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Empty, ErrorNotice, Status } from "@/components/status";
import {
  Facts,
  JsonDetails,
  TaskFacts,
  type TaskRecord,
} from "@/components/inspect-view";
import { Logs } from "@/components/logs";
import { get, timestamp, type Task } from "@/lib/api";
import { usePolling } from "@/lib/use-polling";
import { type Operate } from "@/components/resource-action";

export function TasksView({
  refreshToken,
  onOperate,
}: {
  refreshToken: number;
  onOperate: Operate;
}) {
  const result = usePolling(
    (signal) => get<{ tasks: Task[] }>("/tasks", signal),
    "tasks",
    refreshToken,
  );
  const [search, setSearch] = useState("");
  const [node, setNode] = useState("");
  const [state, setState] = useState("");
  const [selected, setSelected] = useState("");
  const [logs, setLogs] = useState("");
  const tasks = (result.data?.tasks || []).filter(
    (task) =>
      `${task.id} ${task.stack} ${task.stack}.${task.service} ${task.image}`
        .toLowerCase()
        .includes(search.toLowerCase()) &&
      (!node || task.node_id === node) &&
      (!state || task.observed === state),
  );
  return (
    <>
      <div className="filter-toolbar">
        <Input
          aria-label="Search tasks"
          placeholder="Task, Stack, workload or image…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
        <Select
          aria-label="Filter node"
          value={node}
          onValueChange={(value) => setNode(value)}
        >
          <option value="">All nodes</option>
          {[...new Set(result.data?.tasks.map((t) => t.node_id))].map((n) => (
            <option key={n}>{n}</option>
          ))}
        </Select>
        <Select
          aria-label="Filter task state"
          value={state}
          onValueChange={(value) => setState(value)}
        >
          <option value="">All states</option>
          {[...new Set(result.data?.tasks.map((t) => t.observed))].map((s) => (
            <option key={s}>{s}</option>
          ))}
        </Select>
      </div>
      <ErrorNotice error={result.error} stale={Boolean(result.data)} />
      <Card className="table-card">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Task / workload</TableHead>
              <TableHead>Node</TableHead>
              <TableHead>Observed / desired</TableHead>
              <TableHead>Ports / error</TableHead>
              <TableHead />
            </TableRow>
          </TableHeader>
          <TableBody>
            {tasks.map((task) => (
              <TableRow key={task.id}>
                <TableCell>
                  <button
                    className="resource-link mono"
                    onClick={() => setSelected(task.id)}
                  >
                    {task.id}
                  </button>
                  <small className="block muted">
                    {task.stack}.{task.service}
                  </small>
                </TableCell>
                <TableCell>{task.node_id}</TableCell>
                <TableCell>
                  <Status value={task.observed} />
                  <small className="block muted">{task.desired}</small>
                </TableCell>
                <TableCell className="task-error">
                  {task.error ||
                    task.ports
                      .map(
                        (p) =>
                          `${p.published ?? "—"} → ${p.target}/${p.protocol}`,
                      )
                      .join(", ") ||
                    "—"}
                </TableCell>
                <TableCell>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setLogs(task.id)}
                  >
                    Logs
                  </Button>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
        {!tasks.length && (
          <Empty>
            {result.loading ? "Loading tasks…" : "No matching tasks."}
          </Empty>
        )}
      </Card>
      <JsonDetails
        title={`Task IDs (${tasks.length})`}
        value={tasks.map((t) => t.id)}
      />
      {selected && (
        <TaskDetail
          key={selected}
          id={selected}
          refreshToken={refreshToken}
          onClose={() => setSelected("")}
          onOperate={onOperate}
        />
      )}
      {logs && (
        <>
          <div className="section-heading">
            <h2>Logs · {logs}</h2>
            <Button variant="ghost" onClick={() => setLogs("")}>
              Close logs
            </Button>
          </div>
          <Logs key={logs} target={logs} />
        </>
      )}
    </>
  );
}
function TaskDetail({
  id,
  refreshToken,
  onClose,
  onOperate,
}: {
  id: string;
  refreshToken: number;
  onClose: () => void;
  onOperate: Operate;
}) {
  const result = usePolling(
    (signal) =>
      get<{ task: TaskRecord; recovery: boolean }>(
        `/tasks/${encodeURIComponent(id)}`,
        signal,
      ),
    id,
    refreshToken,
  );
  return (
    <Card className="resource-detail-card">
      <CardHeader className="resource-card-header">
        <CardTitle>Task inspection</CardTitle>
        <Button
          variant="ghost"
          size="icon-sm"
          aria-label="Close task details"
          onClick={onClose}
        >
          <X />
        </Button>
      </CardHeader>
      <CardContent>
        <ErrorNotice error={result.error} stale={Boolean(result.data)} />
        {result.data ? (
          <>
            {result.data.recovery && (
              <p className="section-note">
                Recovered container awaiting adoption.
              </p>
            )}
            <TaskFacts task={result.data.task} />
            {result.data.task.job && (
              <Button
                variant="outline"
                onClick={() => onOperate("job cancel", { task_id: [id] })}
              >
                Cancel execution…
              </Button>
            )}
          </>
        ) : (
          <Empty>Loading task…</Empty>
        )}
      </CardContent>
    </Card>
  );
}
interface NodeData {
  id: string;
  member?: {
    address: string;
    labels: Record<string, string>;
    joined_at_unix_ms: number;
    gateway_enabled: boolean;
  };
  report?: {
    address: string;
    labels: Record<string, string>;
    swarmlite_version?: string;
    cpu_millis: number;
    memory_bytes: number;
    port_range_start: number;
    port_range_end: number;
    supports_jobs: boolean;
  };
  tasks: TaskRecord[];
  unclaimed_tasks: TaskRecord[];
}
export function NodesView({
  refreshToken,
  onOperate,
}: {
  refreshToken: number;
  onOperate: Operate;
}) {
  const result = usePolling(
    (signal) =>
      get<{ controller_id: string; nodes: NodeData[]; recovery: unknown }>(
        "/nodes",
        signal,
      ),
    "nodes",
    refreshToken,
  );
  const [selected, setSelected] = useState("");
  const node = result.data?.nodes.find((n) => n.id === selected);
  return (
    <>
      <div className="resource-toolbar">
        <Button
          variant="outline"
          onClick={() => onOperate("connection-info", { json: ["true"] })}
        >
          Local connection details…
        </Button>
        <Button variant="outline" onClick={() => onOperate("join-token")}>
          Node join command…
        </Button>
      </div>
      <p className="section-note">
        Capacity and labels are reported metadata, not live CPU usage or
        reachability probes.
      </p>
      <ErrorNotice error={result.error} stale={Boolean(result.data)} />
      <Card className="table-card">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Node</TableHead>
              <TableHead>Address</TableHead>
              <TableHead>Capacity</TableHead>
              <TableHead>Tasks / recovery</TableHead>
              <TableHead>Labels</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {result.data?.nodes.map((n) => (
              <TableRow
                key={n.id}
                data-state={selected === n.id ? "selected" : undefined}
              >
                <TableCell>
                  <button
                    className="resource-link"
                    onClick={() => setSelected(n.id)}
                  >
                    {n.id}
                  </button>
                  <small className="block muted">
                    {n.id === result.data?.controller_id
                      ? "Controller + Agent"
                      : "Agent"}{" "}
                    · {n.report?.swarmlite_version || "No report"}
                  </small>
                </TableCell>
                <TableCell>{n.member?.address || n.report?.address}</TableCell>
                <TableCell>
                  {n.report
                    ? `${n.report.cpu_millis / 1000} CPU · ${(n.report.memory_bytes / 1024 ** 3).toFixed(1)} GiB`
                    : "—"}
                </TableCell>
                <TableCell>
                  {n.tasks.length} / {n.unclaimed_tasks.length}
                </TableCell>
                <TableCell>
                  {Object.entries(n.member?.labels || {})
                    .map(([k, v]) => `${k}=${v}`)
                    .join(", ") || "—"}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
        {!result.data?.nodes.length && (
          <Empty>{result.loading ? "Loading nodes…" : "No nodes."}</Empty>
        )}
      </Card>
      {node && (
        <Card className="resource-detail-card">
          <CardHeader className="resource-card-header">
            <div className="resource-card-heading">
              <CardTitle>{node.id}</CardTitle>
              <p>
                {node.member?.address || node.report?.address} ·{" "}
                {node.id === result.data?.controller_id
                  ? "Controller + Agent"
                  : "Agent"}
              </p>
            </div>
            <div className="action-group" aria-label="Node actions">
              <Button
                size="sm"
                onClick={() =>
                  onOperate("node label set", { node_id: [node.id] })
                }
              >
                <Plus />
                Set label
              </Button>
              <Button
                size="sm"
                variant="outline"
                disabled={!Object.keys(node.member?.labels || {}).length}
                onClick={() =>
                  onOperate(
                    "node label remove",
                    { node_id: [node.id] },
                    { choices: Object.keys(node.member?.labels || {}) },
                  )
                }
              >
                <Tag />
                Remove label
              </Button>
              <Button
                size="sm"
                variant="outline"
                className={
                  node.member?.gateway_enabled
                    ? "destructive-action"
                    : undefined
                }
                onClick={() =>
                  onOperate(
                    node.member?.gateway_enabled
                      ? "gateway disable"
                      : "gateway enable",
                    { node_id: [node.id] },
                  )
                }
              >
                <Power />
                {node.member?.gateway_enabled ? "Disable" : "Enable"} gateway
              </Button>
            </div>
          </CardHeader>
          <CardContent>
            <Facts
              className="facts-summary"
              values={{
                Joined: timestamp(node.member?.joined_at_unix_ms),
                Gateway: node.member?.gateway_enabled ? "Enabled" : "Disabled",
                "Job support": node.report?.supports_jobs
                  ? "Supported"
                  : "Unavailable",
                "Port range": node.report
                  ? `${node.report.port_range_start}–${node.report.port_range_end}`
                  : undefined,
              }}
            />
            <JsonDetails
              title="Authoritative placement labels"
              value={node.member?.labels}
            />
            <JsonDetails
              title="Last reported node metadata"
              value={node.report}
            />
            <JsonDetails
              title="Assigned task runtime details"
              value={node.tasks}
            />
            <JsonDetails
              title="Unclaimed recovery containers"
              value={node.unclaimed_tasks}
            />
          </CardContent>
        </Card>
      )}
      <JsonDetails title="Recovery summary" value={result.data?.recovery} />
    </>
  );
}
interface ConfigField {
  key: string;
  value: unknown;
  type: string;
  values?: string;
  constraints: string;
  default: string;
  description: string;
  apply_mode: string;
}
export function ConfigurationView({
  refreshToken,
  onOperate,
}: {
  refreshToken: number;
  onOperate: Operate;
}) {
  const result = usePolling(
    (signal) =>
      get<{ generation: number; fields: ConfigField[] }>("/config", signal),
    "configuration",
    refreshToken,
  );
  const [query, setQuery] = useState("");
  const [selected, setSelected] = useState("");
  const field = result.data?.fields.find((f) => f.key === selected);
  return (
    <>
      <p className="section-note">
        Mutable cluster settings · generation #{result.data?.generation ?? "—"}.
        Proxy credentials are hidden in this view.
      </p>
      <Input
        className="configuration-search"
        aria-label="Search configuration"
        placeholder="Search key, scope or description…"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
      />
      <ErrorNotice error={result.error} stale={Boolean(result.data)} />
      <Card className="table-card">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Setting</TableHead>
              <TableHead>Current value</TableHead>
              <TableHead>Type</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {result.data?.fields
              .filter((f) =>
                `${f.key} ${f.description}`
                  .toLowerCase()
                  .includes(query.toLowerCase()),
              )
              .map((f) => (
                <TableRow
                  key={f.key}
                  data-state={selected === f.key ? "selected" : undefined}
                >
                  <TableCell>
                    <button
                      className="resource-link mono"
                      onClick={() => setSelected(f.key)}
                    >
                      {f.key}
                    </button>
                    <small className="block muted">{f.description}</small>
                  </TableCell>
                  <TableCell className="task-error">
                    {f.value === null
                      ? "Unset · uses default"
                      : typeof f.value === "string"
                        ? f.value
                        : JSON.stringify(f.value)}
                  </TableCell>
                  <TableCell>{f.type}</TableCell>
                </TableRow>
              ))}
          </TableBody>
        </Table>
      </Card>
      {field && (
        <Card className="resource-detail-card">
          <CardHeader>
            <CardTitle>{field.key}</CardTitle>
          </CardHeader>
          <CardContent>
            <Facts
              values={{
                Current: field.value,
                Default: field.default,
                Constraints: field.constraints,
                "Allowed values": field.values,
                Applies: field.apply_mode,
              }}
            />
            <div className="resource-toolbar card-actions">
              <Button
                onClick={() =>
                  onOperate(
                    "config set",
                    {
                      key: [field.key],
                      ...(field.value !== null &&
                      !field.key.startsWith("proxy.")
                        ? { value: [String(field.value)] }
                        : {}),
                    },
                    {
                      valueType: field.type,
                      choices: field.values?.split(", "),
                      description: `${field.description} ${field.constraints}. Applies: ${field.apply_mode}.`,
                    },
                  )
                }
              >
                Set value…
              </Button>
              <Button
                variant="outline"
                onClick={() => onOperate("config unset", { key: [field.key] })}
              >
                Reset to default…
              </Button>
            </div>
          </CardContent>
        </Card>
      )}
    </>
  );
}
export function LogExplorer() {
  const [input, setInput] = useState("");
  const [target, setTarget] = useState("");
  return (
    <>
      <form
        className="filter-toolbar"
        onSubmit={(e) => {
          e.preventDefault();
          setTarget(input.trim());
        }}
      >
        <Input
          aria-label="Log target"
          placeholder="STACK.SERVICE, STACK.JOB, task name or ID/prefix"
          value={input}
          onChange={(e) => setInput(e.target.value)}
        />
        <Button type="submit" disabled={!input.trim()}>
          Open logs
        </Button>
      </form>
      {target ? (
        <Logs key={target} target={target} />
      ) : (
        <Empty>
          Select a log target. Supports the same targets as swarmlite logs.
        </Empty>
      )}
    </>
  );
}
