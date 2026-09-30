import { ResourcePanel } from "@/components/resource-panel";
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Empty, ErrorNotice, Status } from "@/components/status";
import { get, timestamp } from "@/lib/api";
import { usePolling } from "@/lib/use-polling";

export interface TaskRecord {
  id: string;
  service_id?: string;
  revision: number;
  slot: number;
  node_id: string;
  desired?: string;
  observed: string;
  container_id?: string;
  applied_generation?: number;
  drain_until_unix_ms?: number;
  config_digests?: string[];
  ports: { target: number; published?: number; protocol: string }[];
  reconcile_error?: { phase: string; message: string };
  job?: Record<string, unknown>;
}
interface Inspect {
  service: {
    id: string;
    stack: string;
    revision: number;
    deleted: boolean;
    job_cursor?: unknown;
    spec: Record<string, unknown>;
  };
  stack: {
    name: string;
    applied_at_unix_ms: number;
    generation?: number;
    gateway: unknown;
  };
  tasks: TaskRecord[];
}
export function JsonDetails({
  title,
  value,
}: {
  title: string;
  value: unknown;
}) {
  return (
    <details className="json-details">
      <summary>{title}</summary>
      <pre>
        {typeof value === "string" ? value : JSON.stringify(value, null, 2)}
      </pre>
    </details>
  );
}
export function Facts({
  values,
  className = "",
}: {
  values: Record<string, unknown>;
  className?: string;
}) {
  return (
    <dl className={`facts ${className}`}>
      {Object.entries(values).map(([name, value]) => (
        <div key={name}>
          <dt>{name}</dt>
          <dd>
            {value == null
              ? "—"
              : typeof value === "object"
                ? JSON.stringify(value)
                : String(value)}
          </dd>
        </div>
      ))}
    </dl>
  );
}
export function TaskFacts({ task }: { task: TaskRecord }) {
  return (
    <>
      <Facts
        values={{
          "Task ID": task.id,
          "Container ID": task.container_id,
          Service: task.service_id,
          Node: task.node_id,
          Revision: task.revision,
          Slot: task.slot,
          "Applied generation": task.applied_generation,
          Observed: task.observed,
          Desired: task.desired,
          "Drain deadline": timestamp(task.drain_until_unix_ms),
        }}
      />
      {task.reconcile_error && (
        <ErrorNotice
          error={`${task.reconcile_error.phase}: ${task.reconcile_error.message}`}
        />
      )}
      <JsonDetails
        title="Port bindings and config digests"
        value={{ ports: task.ports, config_digests: task.config_digests || [] }}
      />
      {task.job && (
        <JsonDetails title="Job execution metadata" value={task.job} />
      )}
    </>
  );
}
export function InspectView({
  target,
  refreshToken,
}: {
  target: string;
  refreshToken: number;
}) {
  const result = usePolling(
    (signal) => get<Inspect>(`/inspect/${encodeURIComponent(target)}`, signal),
    target,
    refreshToken,
  );
  const [selected, setSelected] = useState<string>();
  if (!result.data)
    return (
      <>
        <ErrorNotice error={result.error} />
        <Empty>
          {result.loading
            ? "Loading workload definition…"
            : "Definition unavailable."}
        </Empty>
      </>
    );
  const { service, stack, tasks } = result.data;
  const spec = service.spec;
  const arrays = (key: string) => (spec[key] as unknown[] | undefined) || [];
  return (
    <>
      <ErrorNotice error={result.error} stale />
      <div className="inspect-grid">
        <Card>
          <CardHeader>
            <CardTitle>Identity & deployment</CardTitle>
          </CardHeader>
          <CardContent>
            <Facts
              values={{
                ID: service.id,
                Stack: service.stack,
                Revision: service.revision,
                Generation: stack.generation,
                "Stack applied": timestamp(stack.applied_at_unix_ms),
                Deleted: service.deleted,
                Image: spec.image,
                "Pull policy": spec.pull_policy || "missing",
                Replicas: spec.replicas,
              }}
            />
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Placement & lifecycle</CardTitle>
          </CardHeader>
          <CardContent>
            <Facts
              values={{
                Constraints: arrays("constraints").join(", ") || "None",
                "Max replicas per node":
                  spec.max_replicas_per_node ?? "Unlimited",
                "Max surge": spec.max_surge,
                "Stop grace (seconds)": spec.stop_grace_period_seconds,
                "Stop signal": spec.stop_signal || "Default",
                "Command arguments": arrays("command").length,
                "Entrypoint arguments": arrays("entrypoint").length,
              }}
            />
          </CardContent>
        </Card>
      </div>
      <div className="section-heading">
        <h2>Container configuration</h2>
      </div>
      <Card>
        <CardContent>
          <JsonDetails
            title="Command and entrypoint"
            value={{ command: spec.command, entrypoint: spec.entrypoint }}
          />
          <JsonDetails
            title={`Environment (${arrays("environment").length} variables)`}
            value={spec.environment}
          />
          <JsonDetails
            title="Exposed and published ports"
            value={{ expose: spec.expose || [], ports: spec.ports }}
          />
          <JsonDetails
            title="Volumes and configuration file mounts"
            value={{ volumes: spec.volumes, configs: spec.configs || [] }}
          />
          <JsonDetails
            title="Health check (intervals in nanoseconds)"
            value={spec.healthcheck}
          />
          <JsonDetails
            title="Container and service labels"
            value={{
              container: spec.container_labels,
              service: spec.service_labels,
            }}
          />
          <JsonDetails title="Stack Gateway routes" value={stack.gateway} />
          {spec.job != null && (
            <JsonDetails
              title="Job policy & scheduler cursor"
              value={{ policy: spec.job, cursor: service.job_cursor }}
            />
          )}
        </CardContent>
      </Card>
      <div className="section-heading">
        <h2>Task runtime details</h2>
      </div>
      <Card className="table-card">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Task ID</TableHead>
              <TableHead>Revision</TableHead>
              <TableHead>Node</TableHead>
              <TableHead>State</TableHead>
              <TableHead />
            </TableRow>
          </TableHeader>
          <TableBody>
            {tasks.map((task) => (
              <TableRow key={task.id}>
                <TableCell className="mono">{task.id}</TableCell>
                <TableCell>{task.revision}</TableCell>
                <TableCell>{task.node_id}</TableCell>
                <TableCell>
                  <Status value={task.observed} />
                </TableCell>
                <TableCell>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() =>
                      setSelected(selected === task.id ? undefined : task.id)
                    }
                  >
                    Details
                  </Button>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
        {!tasks.length && <Empty>No retained tasks.</Empty>}
      </Card>
      {selected && (
        <ResourcePanel
          title="Task runtime"
          items={tasks.map((task) => ({
            id: task.id,
            label: `${task.id} · ${task.node_id}`,
          }))}
          selected={selected}
          onSelect={setSelected}
          onClose={() => setSelected(undefined)}
        >
          {tasks
            .filter((task) => task.id === selected)
            .map((task) => (
              <TaskFacts key={task.id} task={task} />
            ))}
        </ResourcePanel>
      )}
      <JsonDetails title="Full inspect JSON" value={result.data} />
    </>
  );
}
