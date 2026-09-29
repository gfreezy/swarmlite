import { Select } from "@/components/ui/select";
import { type Operate } from "@/components/resource-action";
import { useState } from "react";
import { Clock3, Terminal } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Status, Empty, ErrorNotice } from "@/components/status";
import { InspectView } from "@/components/inspect-view";
import { Logs } from "@/components/logs";
import { usePolling } from "@/lib/use-polling";
import {
  get,
  timestamp,
  jobFinished,
  jobDuration,
  type Service,
  type JobInfo,
  type JobExecution,
} from "@/lib/api";

export function JobDetail({
  service,
  refreshToken,
  onOperate,
}: {
  service: Service;
  refreshToken: number;
  onOperate?: Operate;
}) {
  const info = usePolling(
    (signal) => get<JobInfo>(`/jobs/${encodeURIComponent(service.id)}`, signal),
    service.id,
    refreshToken,
  );
  const history = usePolling(
    (signal) =>
      get<JobExecution[]>(
        `/jobs/${encodeURIComponent(service.id)}/history`,
        signal,
      ),
    service.id,
    refreshToken,
  );
  const [tab, setTab] = useState("history");
  const [logTarget, setLogTarget] = useState(service.id);
  const policy = info.data?.policy || service.job!;
  const executions = history.data || [];
  return (
    <>
      <div className="resource-toolbar">
        <div className="detail-meta">
          <Badge variant="secondary">Job</Badge>
          <span>Stack: {service.stack}</span>
          <Status
            value={
              policy.suspend
                ? "suspended"
                : policy.schedule
                  ? "scheduled"
                  : "manual"
            }
          />
        </div>
      </div>
      <div className="progress-grid">
        <Card>
          <CardContent>
            <div className="metric-label">
              <Clock3 size={15} />
              Schedule
            </div>
            <p className="job-fact mono">{policy.schedule || "Manual only"}</p>
            <p className="muted">
              {policy.timezone}
              {policy.suspend ? " · Automatic triggers suspended" : ""}
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardContent>
            <div className="metric-label">Next scheduled trigger</div>
            <p className="job-fact">
              {policy.suspend
                ? "Suspended"
                : timestamp(info.data?.next_at_unix_ms)}
            </p>
            <p className="muted">
              Runtime limit:{" "}
              {policy.timeout_seconds
                ? `${policy.timeout_seconds}s`
                : "Default"}
            </p>
          </CardContent>
        </Card>
      </div>
      <ErrorNotice error={info.error} stale={Boolean(info.data)} />
      <ErrorNotice error={history.error} stale={Boolean(history.data)} />
      <Tabs value={tab} onValueChange={setTab}>
        <TabsList>
          <TabsTrigger value="inspect">Inspect</TabsTrigger>
          <TabsTrigger value="history">
            <Clock3 size={15} />
            Executions
          </TabsTrigger>
          <TabsTrigger value="logs">
            <Terminal size={15} />
            Logs
          </TabsTrigger>
        </TabsList>
        <TabsContent value="inspect">
          <InspectView target={service.id} refreshToken={refreshToken} />
        </TabsContent>
        <TabsContent value="history">
          {onOperate && (
            <div className="resource-toolbar">
              <Button
                variant="outline"
                onClick={() =>
                  onOperate("job history", {
                    target: [service.id],
                    json: ["true"],
                  })
                }
              >
                Full execution records…
              </Button>
            </div>
          )}
          <Card className="table-card">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Execution</TableHead>
                  <TableHead>State</TableHead>
                  <TableHead>Scheduled / started</TableHead>
                  <TableHead>Duration</TableHead>
                  <TableHead>Node</TableHead>
                  <TableHead>Exit / reason</TableHead>
                  <TableHead />
                </TableRow>
              </TableHeader>
              <TableBody>
                {executions.map((execution) => (
                  <TableRow key={execution.id}>
                    <TableCell className="mono" title={execution.id}>
                      {execution.id.length > 22
                        ? `${execution.id.slice(0, 8)}…${execution.id.slice(-8)}`
                        : execution.id}
                    </TableCell>
                    <TableCell>
                      <Status
                        value={
                          execution.desired === "stopped" &&
                          !jobFinished(execution)
                            ? "stopping"
                            : execution.observed
                        }
                      />
                    </TableCell>
                    <TableCell>
                      <span>{timestamp(execution.scheduled_at_unix_ms)}</span>
                      <small className="block muted">
                        {timestamp(execution.started_at_unix_ms)}
                      </small>
                    </TableCell>
                    <TableCell
                      title={
                        execution.finished_at_unix_ms
                          ? `Finished ${timestamp(execution.finished_at_unix_ms)}`
                          : undefined
                      }
                    >
                      {jobDuration(execution)}
                    </TableCell>
                    <TableCell>{execution.node_id || "—"}</TableCell>
                    <TableCell className="task-error">
                      {execution.exit_code ?? "—"}
                      {(execution.stop_reason || execution.error) && (
                        <small className="block">
                          {execution.stop_reason || execution.error}
                        </small>
                      )}
                    </TableCell>
                    <TableCell>
                      <div className="flex gap-1">
                        {onOperate &&
                          !jobFinished(execution) &&
                          execution.desired !== "stopped" && (
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() =>
                                onOperate("job cancel", {
                                  task_id: [execution.id],
                                })
                              }
                            >
                              Cancel execution…
                            </Button>
                          )}
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => {
                            setLogTarget(execution.id);
                            setTab("logs");
                          }}
                        >
                          <Terminal />
                          Logs
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            {!executions.length && (
              <Empty>
                {history.loading
                  ? "Loading execution history…"
                  : "No retained executions yet."}
              </Empty>
            )}
          </Card>
          <p className="section-note">
            The Controller retains the last 20 confirmed finished executions and
            unresolved executions. A lost run is not confirmed finished.
            Duration is unavailable when older Agents have not reported an end
            time.
          </p>
        </TabsContent>
        <TabsContent value="logs">
          <div className="log-selector">
            <label htmlFor="job-log-source">Source</label>
            <Select
              id="job-log-source"
              value={logTarget}
              onValueChange={(value) => setLogTarget(value)}
            >
              <option value={service.id}>
                Retained executions · {service.id}
              </option>
              {executions.map((execution) => (
                <option key={execution.id} value={execution.id}>
                  {execution.id} · {execution.observed}
                </option>
              ))}
            </Select>
          </div>
          <Logs target={logTarget} />
        </TabsContent>
      </Tabs>
    </>
  );
}
