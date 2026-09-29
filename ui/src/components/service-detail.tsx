import { Select } from "@/components/ui/select";
import { useState } from "react";
import { Boxes, Terminal } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
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
import { get, type Service, type Task } from "@/lib/api";

export function ServiceDetail({
  service,
  refreshToken,
}: {
  service: Service;
  refreshToken: number;
}) {
  const result = usePolling(
    (signal) =>
      get<{ tasks: Task[] }>(
        `/services/${encodeURIComponent(service.id)}/tasks`,
        signal,
      ),
    service.id,
    refreshToken,
  );
  const [logTarget, setLogTarget] = useState(service.id);
  const [tab, setTab] = useState("inspect");
  const showLogs = (target: string) => {
    setLogTarget(target);
    setTab("logs");
  };
  return (
    <>
      <div className="resource-toolbar">
        <div className="detail-meta">
          <Badge variant="secondary">Service</Badge>
          <span>Stack: {service.stack}</span>
          <span>
            Replicas: {service.running_replicas} / {service.replicas}
          </span>
        </div>
      </div>
      <ErrorNotice error={result.error} stale={Boolean(result.data)} />
      <Tabs value={tab} onValueChange={setTab}>
        <TabsList>
          <TabsTrigger value="inspect">Inspect</TabsTrigger>
          <TabsTrigger value="tasks">
            <Boxes size={15} />
            Tasks
          </TabsTrigger>
          <TabsTrigger value="logs">
            <Terminal size={15} />
            Logs
          </TabsTrigger>
        </TabsList>
        <TabsContent value="inspect">
          <InspectView target={service.id} refreshToken={refreshToken} />
        </TabsContent>
        <TabsContent value="tasks">
          <Card className="table-card">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Task</TableHead>
                  <TableHead>Node</TableHead>
                  <TableHead>Observed</TableHead>
                  <TableHead>Desired</TableHead>
                  <TableHead>Details</TableHead>
                  <TableHead />
                </TableRow>
              </TableHeader>
              <TableBody>
                {result.data?.tasks.map((task) => (
                  <TableRow key={task.id}>
                    <TableCell className="mono" title={task.id}>
                      {task.id.length > 22
                        ? `${task.id.slice(0, 8)}…${task.id.slice(-8)}`
                        : task.id}
                    </TableCell>
                    <TableCell>{task.node_id}</TableCell>
                    <TableCell>
                      <Status value={task.observed} />
                    </TableCell>
                    <TableCell>{task.desired}</TableCell>
                    <TableCell className="task-error">
                      {task.error ||
                        task.ports
                          .map(
                            (port) =>
                              `${port.published ?? "—"} → ${port.target}/${port.protocol}`,
                          )
                          .join(", ") ||
                        "—"}
                    </TableCell>
                    <TableCell>
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={() => showLogs(task.id)}
                      >
                        <Terminal />
                        Logs
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            {!result.data?.tasks.length && (
              <Empty>
                {result.loading
                  ? "Loading tasks…"
                  : "No tasks reported for this workload."}
              </Empty>
            )}
          </Card>
        </TabsContent>
        <TabsContent value="logs">
          <div className="log-selector">
            <label htmlFor="log-task">Source</label>
            <Select
              id="log-task"
              value={logTarget}
              onValueChange={setLogTarget}
            >
              <option value={service.id}>All tasks · {service.id}</option>
              {result.data?.tasks.map((task) => (
                <option key={task.id} value={task.id}>
                  {task.id} @ {task.node_id}
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
