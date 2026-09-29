import { Select } from "@/components/ui/select";
import { useRef, useState, type ReactNode } from "react";
import {
  Activity,
  ArrowLeft,
  ArrowUpRight,
  Boxes,
  CircleAlert,
  Clock3,
  GitBranch,
  Layers3,
  LayoutDashboard,
  Radio,
  RefreshCw,
  Search,
  Server,
  ShieldCheck,
  Terminal,
  Settings,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { usePolling } from "@/lib/use-polling";
import { Status, Empty, ErrorNotice } from "@/components/status";
import { ServiceDetail } from "@/components/service-detail";
import { RoutesView } from "@/components/routes-view";
import { DeploymentCompare } from "@/components/deployment-compare";
import {
  ResourceActionDialog,
  ResourceActivity,
  type ResourceAction,
  type Operate,
} from "@/components/resource-action";
import {
  TasksView,
  NodesView,
  ConfigurationView,
  LogExplorer,
} from "@/components/cluster-views";
import { JobDetail } from "@/components/job-detail";
import {
  get,
  timestamp,
  type Overview,
  type Service,
  type Stack,
  type Deployment,
} from "@/lib/api";

interface Snapshot {
  overview: Overview;
  services: Service[];
  stacks: Stack[];
}
async function loadSnapshot(signal: AbortSignal): Promise<Snapshot> {
  const [overview, services, deployments] = await Promise.all([
    get<Overview>("/overview", signal),
    get<{ services: Service[] }>("/services", signal),
    get<{ stacks: Stack[] }>("/deployments", signal),
  ]);
  return { overview, services: services.services, stacks: deployments.stacks };
}

const pageDescriptions: Record<string, string> = {
  Overview: "Cluster health, running workloads and recent changes.",
  Workloads: "Services and jobs across your stacks.",
  Deployments: "Track rollout progress and compare retained generations.",
  Nodes: "Manage node placement, capacity and gateway availability.",
  Routes: "Trace traffic from hostnames and paths to running tasks.",
  Jobs: "Schedules, execution history and job controls.",
  Tasks: "Inspect task state, runtime details and logs.",
  Logs: "Live output from services, jobs and individual tasks.",
  Configuration: "Review cluster settings, defaults and how changes apply.",
  Registries: "Manage credentials for private container images.",
};

export default function App() {
  const snapshot = usePolling(loadSnapshot, "cluster");
  const [page, setPage] = useState("Overview");
  const [search, setSearch] = useState("");
  const [selectedService, setSelectedService] = useState<Service>();
  const [selectedStack, setSelectedStack] = useState<string>();
  const [action, setAction] = useState<ResourceAction>();
  const [recentActions, setRecentActions] = useState<ResourceAction[]>([]);
  const actionScope = selectedService
    ? `workload:${selectedService.id}`
    : selectedStack
      ? `stack:${selectedStack}`
      : page;
  const actionSequence = useRef(0);
  const operate: Operate = (command, values, options) => {
    setAction({
      command,
      values,
      options,
      key: ++actionSequence.current,
      scope: actionScope,
    });
  };
  const [refreshToken, setRefreshToken] = useState(0);
  const refresh = () => {
    snapshot.refresh();
    setRefreshToken((value) => value + 1);
  };
  const { overview, services = [], stacks = [] } = snapshot.data || {};
  const go = (next: string) => {
    setPage(next);
    setSelectedService(undefined);
    setSelectedStack(undefined);
    setSearch("");
  };
  const openService = (service: Service) => {
    setSelectedStack(undefined);
    setSelectedService(service);
  };
  const openStack = (name: string) => {
    setSelectedService(undefined);
    setSelectedStack(name);
  };
  const workloads = services.filter((service) => !service.job);
  const desired = workloads.reduce((sum, service) => sum + service.replicas, 0);
  const running = workloads.reduce(
    (sum, service) => sum + service.running_replicas,
    0,
  );
  const issues = stacks.filter(
    (stack) =>
      stack.current &&
      ["blocked", "stalled", "failed"].includes(stack.current.status),
  );
  const filtered = services.filter((service) =>
    `${service.id} ${service.image}`
      .toLowerCase()
      .includes(search.toLowerCase()),
  );
  const navigation = [
    { name: "Overview", icon: LayoutDashboard },
    { name: "Workloads", icon: Boxes },
    { name: "Deployments", icon: GitBranch },
    { name: "Nodes", icon: Server },
    { name: "Routes", icon: Radio },
    { name: "Jobs", icon: Clock3 },
    { name: "Tasks", icon: Activity },
    { name: "Logs", icon: Terminal },
    { name: "Configuration", icon: Settings },
    { name: "Registries", icon: ShieldCheck },
  ];
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <a
          className="brand"
          href="/"
          onClick={(event) => {
            event.preventDefault();
            go("Overview");
          }}
        >
          <span className="brand-icon">
            <Layers3 size={23} />
          </span>
          swarmlite<span className="brand-tag">UI</span>
        </a>
        <div className="workspace-label">WORKSPACE</div>
        <div className="cluster-switch">
          <span className="cluster-avatar">S</span>
          <div>
            <strong>Cluster console</strong>
            <small title={overview?.cluster_id}>
              {overview?.cluster_id || "Awaiting connection"}
            </small>
          </div>
        </div>
        <nav>
          {navigation.map((item) => (
            <button
              key={item.name}
              className={page === item.name ? "nav-item selected" : "nav-item"}
              onClick={() => go(item.name)}
            >
              <item.icon size={18} />
              {item.name}
              {item.name === "Workloads" && <span>{services.length}</span>}
            </button>
          ))}
        </nav>
        <div className="sidebar-bottom">
          <ShieldCheck size={18} />
          <div>
            <strong>Local session</strong>
            <p>Cluster management access</p>
          </div>
        </div>
      </aside>
      <div className="main-area">
        <header className="topbar">
          <span>
            Workspace <span className="separator">/</span>{" "}
            <strong>{page}</strong>
          </span>
          <span className="connection-label">
            <i
              className={
                snapshot.error
                  ? "dot disconnected"
                  : snapshot.updated
                    ? "dot"
                    : "dot pending"
              }
            />
            {snapshot.error
              ? "Connection interrupted"
              : snapshot.updated
                ? "Controller connected"
                : "Connecting"}
          </span>
        </header>
        <main>
          <div className="page-heading">
            <div>
              <div className="eyebrow">CLUSTER WORKSPACE</div>
              <h1>{selectedService?.id || selectedStack || page}</h1>
              <p>
                {selectedService
                  ? selectedService.image
                  : selectedStack
                    ? "Deployment progress and generation history."
                    : pageDescriptions[page]}
              </p>
            </div>
            <Button
              variant="outline"
              onClick={refresh}
              disabled={snapshot.loading}
            >
              <RefreshCw className={snapshot.loading ? "animate-spin" : ""} />
              Refresh
            </Button>
          </div>
          <ErrorNotice error={snapshot.error} stale={Boolean(snapshot.data)} />
          {(selectedService || selectedStack) && (
            <Button
              variant="ghost"
              className="back-button"
              onClick={() => {
                setSelectedService(undefined);
                setSelectedStack(undefined);
              }}
            >
              <ArrowLeft />
              Back to {page.toLowerCase()}
            </Button>
          )}
          {selectedService && (
            <div className="resource-toolbar">
              <Button
                variant="outline"
                onClick={() =>
                  operate("inspect", { target: [selectedService.id] })
                }
              >
                Full definition…
              </Button>
              {selectedService.job ? (
                <Button
                  variant="outline"
                  onClick={() =>
                    operate("job run", { target: [selectedService.id] })
                  }
                >
                  Run job…
                </Button>
              ) : (
                <>
                  <Button
                    variant="outline"
                    onClick={() =>
                      operate("service scale", {
                        services: [
                          `${selectedService.id}=${services.find((s) => s.id === selectedService.id)?.replicas ?? selectedService.replicas}`,
                        ],
                      })
                    }
                  >
                    Scale…
                  </Button>
                  <Button
                    variant="outline"
                    onClick={() =>
                      operate("service restart", {
                        service: [selectedService.id],
                      })
                    }
                  >
                    Rolling restart…
                  </Button>
                </>
              )}
            </div>
          )}
          {selectedStack && (
            <div className="resource-toolbar">
              <Button
                variant="outline"
                onClick={() =>
                  operate("deployment retry", { stack: [selectedStack] })
                }
              >
                Retry…
              </Button>
              <Button
                variant="outline"
                onClick={() =>
                  operate(
                    "deployment rollback",
                    { stack: [selectedStack] },
                    {
                      generations: stacks
                        .find((s) => s.stack === selectedStack)
                        ?.history.map((d) => d.generation),
                    },
                  )
                }
              >
                Roll back…
              </Button>
              <Button
                variant="outline"
                onClick={() => operate("rm", { stacks: [selectedStack] })}
              >
                Remove Stack…
              </Button>
            </div>
          )}
          {selectedService ? (
            selectedService.job ? (
              <JobDetail
                key={selectedService.id}
                service={
                  services.find(
                    (service) => service.id === selectedService.id,
                  ) || selectedService
                }
                refreshToken={refreshToken}
                onOperate={operate}
              />
            ) : (
              <ServiceDetail
                key={selectedService.id}
                service={
                  services.find(
                    (service) => service.id === selectedService.id,
                  ) || selectedService
                }
                refreshToken={refreshToken}
              />
            )
          ) : selectedStack ? (
            <DeploymentDetail
              key={selectedStack}
              name={selectedStack}
              onOperate={operate}
              refreshToken={refreshToken}
            />
          ) : (
            <>
              {page === "Overview" && (
                <>
                  <div className="resource-toolbar">
                    <Button
                      variant="outline"
                      onClick={() => operate("status", { json: ["true"] })}
                    >
                      Full cluster state…
                    </Button>
                  </div>
                  <div className="metrics">
                    <Metric
                      label="Registered nodes"
                      value={overview ? String(overview.nodes.length) : "—"}
                      description="Cluster membership"
                      icon={<Server />}
                    />
                    <Metric
                      label="Workloads"
                      value={overview ? String(services.length) : "—"}
                      description={`${workloads.length} services · ${services.length - workloads.length} jobs`}
                      icon={<Boxes />}
                    />
                    <Metric
                      label="Running replicas"
                      value={overview ? `${running} / ${desired}` : "—"}
                      description="Reported / desired service replicas"
                      icon={<Activity />}
                    />
                    <Metric
                      label="Stack deployments"
                      value={overview ? String(stacks.length) : "—"}
                      description={`${issues.length} need attention`}
                      icon={<GitBranch />}
                    />
                  </div>
                  {(issues.length > 0 ||
                    Object.keys(overview?.gateway.endpoint_errors || {})
                      .length > 0 ||
                    (overview?.recovery.conflicting_slots || 0) > 0) && (
                    <section className="attention">
                      <div className="section-title">
                        <CircleAlert size={18} />
                        <h2>Needs attention</h2>
                      </div>
                      {issues.map((stack) => (
                        <button
                          key={stack.stack}
                          onClick={() => openStack(stack.stack)}
                        >
                          <span>
                            <strong>{stack.stack}</strong>
                            <small>
                              {stack.current?.errors?.[0]?.message ||
                                "Inspect deployment progress for more details."}
                            </small>
                          </span>
                          <Status value={stack.current!.status} />
                          <ArrowUpRight size={16} />
                        </button>
                      ))}
                      {Object.entries(
                        overview?.gateway.endpoint_errors || {},
                      ).map(([node, message]) => (
                        <p key={node}>
                          <strong>Gateway · {node}</strong> — {message}
                        </p>
                      ))}
                      {Boolean(overview?.recovery.conflicting_slots) && (
                        <p>
                          {overview!.recovery.conflicting_slots} conflicting
                          recovery slots need attention.
                        </p>
                      )}
                    </section>
                  )}
                  <div className="section-heading">
                    <div>
                      <h2>Workloads</h2>
                      <p>Services and jobs across your stacks</p>
                    </div>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => go("Workloads")}
                    >
                      View all
                      <ArrowUpRight />
                    </Button>
                  </div>
                  <WorkloadTable
                    services={services.slice(0, 8)}
                    open={openService}
                    loading={snapshot.loading && !snapshot.data}
                  />
                  <div className="overview-bottom">
                    <Card>
                      <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                          <Radio size={17} />
                          Gateway
                        </CardTitle>
                      </CardHeader>
                      <CardContent>
                        <div className="gateway-line">
                          <span>Routing configuration</span>
                          <Status
                            value={
                              !overview
                                ? "unknown"
                                : !overview.gateway.enabled
                                  ? "disabled"
                                  : Object.keys(
                                        overview.gateway.endpoint_errors,
                                      ).length
                                    ? "error"
                                    : overview.gateway.applied_generation ===
                                        overview.gateway.desired_generation
                                      ? "ready"
                                      : "pending"
                            }
                          />
                        </div>
                        <p className="muted">
                          Desired generation{" "}
                          {overview?.gateway.desired_generation ?? "—"} ·
                          Applied {overview?.gateway.applied_generation ?? "—"}
                        </p>
                      </CardContent>
                    </Card>
                    <Card>
                      <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                          <Clock3 size={17} />
                          Latest snapshot
                        </CardTitle>
                      </CardHeader>
                      <CardContent>
                        <p>{timestamp(snapshot.updated)}</p>
                        <p className="muted">
                          Refreshes every 5 seconds. Node records describe
                          reported state, not a live traffic probe.
                        </p>
                      </CardContent>
                    </Card>
                  </div>
                </>
              )}
              {page === "Workloads" && (
                <>
                  <div className="resource-toolbar">
                    <Button
                      variant="outline"
                      onClick={() => operate("ls", { json: ["true"] })}
                    >
                      Full definitions…
                    </Button>
                  </div>
                  <div className="search-bar">
                    <Search size={17} />
                    <Input
                      aria-label="Search workloads"
                      placeholder="Search by workload or image…"
                      value={search}
                      onChange={(event) => setSearch(event.target.value)}
                    />
                    <span>{filtered.length} workloads</span>
                  </div>
                  <WorkloadTable
                    services={filtered}
                    open={openService}
                    loading={snapshot.loading && !snapshot.data}
                  />
                </>
              )}
              {page === "Deployments" && (
                <Card className="table-card">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Stack</TableHead>
                        <TableHead>Generation</TableHead>
                        <TableHead>Status</TableHead>
                        <TableHead>Started</TableHead>
                        <TableHead />
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {stacks.map((stack) => (
                        <TableRow key={stack.stack}>
                          <TableCell>
                            <button
                              className="resource-link"
                              onClick={() => openStack(stack.stack)}
                            >
                              <Layers3 size={16} />
                              {stack.stack}
                            </button>
                          </TableCell>
                          <TableCell className="mono">
                            {stack.current
                              ? `#${stack.current.generation}`
                              : "—"}
                          </TableCell>
                          <TableCell>
                            <Status
                              value={stack.current?.status || "unknown"}
                            />
                          </TableCell>
                          <TableCell>
                            {timestamp(stack.current?.started_at_unix_ms)}
                          </TableCell>
                          <TableCell>
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => openStack(stack.stack)}
                            >
                              Inspect
                              <ArrowUpRight />
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                  {!stacks.length && (
                    <Empty>
                      {snapshot.loading
                        ? "Loading deployments…"
                        : "No deployments yet."}
                    </Empty>
                  )}
                </Card>
              )}
              {page === "Routes" && (
                <RoutesView
                  refreshToken={refreshToken}
                  services={services}
                  onService={openService}
                />
              )}
              {page === "Jobs" && (
                <>
                  <p className="section-note">
                    Inspect schedules and execution history. Open a job to run
                    it or cancel an execution.
                  </p>
                  <WorkloadTable
                    services={services.filter((service) => service.job)}
                    open={openService}
                    loading={snapshot.loading && !snapshot.data}
                  />
                </>
              )}
              {page === "Nodes" && (
                <NodesView refreshToken={refreshToken} onOperate={operate} />
              )}
              {page === "Tasks" && (
                <TasksView refreshToken={refreshToken} onOperate={operate} />
              )}
              {page === "Configuration" && (
                <ConfigurationView
                  refreshToken={refreshToken}
                  onOperate={operate}
                />
              )}
              {page === "Logs" && <LogExplorer />}
              {page === "Registries" && (
                <Card>
                  <CardHeader>
                    <CardTitle>Private image registries</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <p className="section-note">
                      Save or update credentials used by all nodes when pulling
                      private images.
                    </p>
                    <Button onClick={() => operate("registry login")}>
                      Add or update credentials…
                    </Button>
                  </CardContent>
                </Card>
              )}
            </>
          )}
          {recentActions.some((a) => a.scope === actionScope) && (
            <ResourceActivity
              actions={recentActions.filter((a) => a.scope === actionScope)}
              onOpen={setAction}
            />
          )}
          {action && (
            <ResourceActionDialog
              key={action.key}
              action={action}
              onClose={() => setAction(undefined)}
              onAccepted={(id) => {
                setRecentActions((previous) =>
                  previous.some((a) => a.runId === id)
                    ? previous
                    : [{ ...action, runId: id }, ...previous],
                );
              }}
              onComplete={refresh}
            />
          )}
          <footer>
            Swarmlite console <span>Local connection · Management</span>
          </footer>
        </main>
      </div>
    </div>
  );
}

function Metric({
  label,
  value,
  description,
  icon,
}: {
  label: string;
  value: string;
  description: string;
  icon: ReactNode;
}) {
  return (
    <Card className="metric">
      <CardContent>
        <div className="metric-label">
          {label}
          {icon}
        </div>
        <div className="metric-value">{value}</div>
        <p>{description}</p>
      </CardContent>
    </Card>
  );
}
function WorkloadTable({
  services,
  open,
  loading,
}: {
  services: Service[];
  open: (service: Service) => void;
  loading: boolean;
}) {
  return (
    <Card className="table-card">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Workload</TableHead>
            <TableHead>Image</TableHead>
            <TableHead>Type</TableHead>
            <TableHead>Replicas</TableHead>
            <TableHead />
          </TableRow>
        </TableHeader>
        <TableBody>
          {services.map((service) => (
            <TableRow key={service.id}>
              <TableCell>
                <button className="resource-link" onClick={() => open(service)}>
                  <span className="resource-icon">
                    <Boxes size={17} />
                  </span>
                  <span>
                    <strong>{service.name}</strong>
                    <small>{service.stack}</small>
                  </span>
                </button>
              </TableCell>
              <TableCell>
                <span className="image-name" title={service.image}>
                  {service.image}
                </span>
              </TableCell>
              <TableCell>
                <Badge variant="secondary">
                  {service.job ? "Job" : "Service"}
                </Badge>
              </TableCell>
              <TableCell>
                {service.job ? (
                  "—"
                ) : (
                  <span
                    className={
                      service.running_replicas < service.replicas
                        ? "replicas-short"
                        : "replicas-ready"
                    }
                  >
                    <i className="tiny-dot" />
                    {service.running_replicas} / {service.replicas}
                  </span>
                )}
              </TableCell>
              <TableCell>
                <Button variant="ghost" size="sm" onClick={() => open(service)}>
                  Inspect
                  <ArrowUpRight />
                </Button>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
      {!services.length && (
        <Empty>
          {loading
            ? "Loading workloads…"
            : "No matching workloads. Deploy a Stack to get started."}
        </Empty>
      )}
    </Card>
  );
}
function DeploymentDetail({
  name,
  refreshToken,
  onOperate,
}: {
  name: string;
  refreshToken: number;
  onOperate: Operate;
}) {
  const [generation, setGeneration] = useState<number>();
  const result = usePolling(
    async (signal) => {
      const [stack, selected] = await Promise.all([
        get<Stack>(`/stacks/${encodeURIComponent(name)}/deployments`, signal),
        generation === undefined
          ? Promise.resolve(undefined)
          : get<Deployment>(
              `/stacks/${encodeURIComponent(name)}/deployment?generation=${generation}`,
              signal,
            ),
      ]);
      return { stack, selected };
    },
    `${name}:${generation ?? "current"}`,
    refreshToken,
  );
  const stack = result.data?.stack;
  const current =
    generation === undefined
      ? stack?.current
      : result.data?.selected?.generation === generation
        ? result.data.selected
        : undefined;
  return (
    <>
      <div className="resource-toolbar">
        <label className="generation-picker">
          Viewing
          <Select
            className="generation-select"
            aria-label="Deployment generation"
            value={generation ?? "current"}
            onValueChange={(value) =>
              setGeneration(value === "current" ? undefined : Number(value))
            }
          >
            <option value="current">Current generation</option>
            {[
              ...(stack?.current ? [stack.current] : []),
              ...(stack?.history || []),
            ].map((item) => (
              <option key={item.generation} value={item.generation}>
                #{item.generation} · {item.status}
              </option>
            ))}
          </Select>
        </label>
        <Button
          variant="outline"
          onClick={() =>
            onOperate("deployment attach", {
              stack: [name],
              ...(generation !== undefined
                ? { generation: [String(generation)] }
                : {}),
            })
          }
        >
          Follow progress…
        </Button>
        <Button
          variant="outline"
          onClick={() =>
            onOperate("deployment status", {
              stack: [name],
              json: ["true"],
              ...(generation !== undefined
                ? { generation: [String(generation)] }
                : {}),
            })
          }
        >
          Full state…
        </Button>
        <Button
          variant="outline"
          onClick={() =>
            onOperate("deployment history", { stack: [name], json: ["true"] })
          }
        >
          Full history…
        </Button>
      </div>
      <ErrorNotice error={result.error} stale={Boolean(result.data)} />
      {generation !== undefined &&
        stack?.current?.generation !== generation && (
          <p className="section-note">
            Viewing generation #{generation}. The current deployment is #
            {stack?.current?.generation ?? "—"}.
          </p>
        )}
      {current ? (
        <>
          <div className="detail-meta">
            <Status value={current.status} />
            <span>Generation #{current.generation}</span>
            <span>Started {timestamp(current.started_at_unix_ms)}</span>
          </div>
          <div className="progress-grid">
            {current.services.map((service) => (
              <Card key={service.service}>
                <CardContent>
                  <div className="progress-title">
                    <strong>{service.service}</strong>
                    <span>
                      {service.healthy} / {service.replicas} healthy
                    </span>
                  </div>
                  <progress
                    value={service.healthy}
                    max={Math.max(1, service.replicas)}
                  />
                  <p className="muted">
                    {service.applied} applied · {service.replicas} desired
                  </p>
                </CardContent>
              </Card>
            ))}
          </div>
          {Boolean(current.task_phases?.length) && (
            <div className="detail-meta">
              {current.task_phases?.map((phase) => (
                <Badge variant="outline" key={phase.phase}>
                  {phase.phase.replaceAll("_", " ")}: {phase.tasks}
                </Badge>
              ))}
            </div>
          )}
          {current.image_resolutions?.map((image) => (
            <div className="detail-meta" key={image.service}>
              <span>
                {image.service} · {image.image}
              </span>
              <Status value={image.status} />
              <span>
                {image.completed_nodes} / {image.total_nodes} nodes
              </span>
            </div>
          ))}
          {current.gateway && (
            <Card>
              <CardContent className="gateway-line">
                <strong>Gateway configuration</strong>
                <span>
                  {current.gateway.applied_nodes} /{" "}
                  {current.gateway.total_nodes} gateways applied generation #
                  {current.gateway.generation}
                </span>
              </CardContent>
            </Card>
          )}
          {current.errors?.map((error, index) => (
            <ErrorNotice
              key={index}
              error={`${error.service || error.node_id || name}: ${error.message}`}
            />
          ))}
          {Object.entries(current.gateway?.errors || {}).map(
            ([node, message]) => (
              <ErrorNotice key={node} error={`${node}: ${message}`} />
            ),
          )}
          {current.conditions?.map((condition, index) => (
            <p className="section-note" key={index}>
              {condition.message}
            </p>
          ))}
        </>
      ) : (
        <Empty>
          {result.loading ? "Loading deployment…" : "No current deployment."}
        </Empty>
      )}
      <div className="section-heading">
        <h2>Generation history</h2>
      </div>
      <Card className="table-card">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Generation</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Started</TableHead>
              <TableHead>Finished</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {stack?.history.map((item) => (
              <TableRow key={item.generation}>
                <TableCell className="mono">
                  <button
                    className="resource-link"
                    onClick={() => setGeneration(item.generation)}
                  >
                    #{item.generation}
                    <ArrowUpRight size={13} />
                  </button>
                </TableCell>
                <TableCell>
                  <Status value={item.status} />
                </TableCell>
                <TableCell>{timestamp(item.started_at_unix_ms)}</TableCell>
                <TableCell>{timestamp(item.finished_at_unix_ms)}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
        {!stack?.history.length && <Empty>No previous generations.</Empty>}
      </Card>
      {stack && <DeploymentCompare stack={stack} refreshToken={refreshToken} />}
    </>
  );
}
