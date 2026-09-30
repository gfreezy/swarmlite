import { ResourceExplorer } from "@/components/resource-explorer";
import { RouteGraph, type RouteRow } from "@/components/route-graph";
import { Button } from "@/components/ui/button";
import { JsonDetails } from "@/components/inspect-view";
import { useState } from "react";
import { ArrowUpRight, Search } from "lucide-react";
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
import { Status, Empty, ErrorNotice } from "@/components/status";
import { get, type Service } from "@/lib/api";
import { usePolling } from "@/lib/use-polling";

interface GatewayNode {
  node_id: string;
  address: string;
  status: string;
  desired_generation?: number;
  applied_generation?: number;
  error?: string;
}

export function RoutesView({
  refreshToken,
  services,
  onService,
}: {
  refreshToken: number;
  services: Service[];
  onService: (service: Service) => void;
}) {
  const [search, setSearch] = useState("");
  const [view, setView] = useState<"graph" | "list">("graph");
  const [selected, setSelected] = useState("");
  const routes = usePolling(
    (signal) =>
      get<{ generation: number; routes: RouteRow[] }>("/routes", signal),
    "routes",
    refreshToken,
  );
  const gateways = usePolling(
    (signal) =>
      get<{ generation: number; nodes: GatewayNode[]; config?: unknown }>(
        "/gateways",
        signal,
      ),
    "gateways",
    refreshToken,
  );
  const filtered =
    routes.data?.routes.filter((route) =>
      `${route.stack} ${route.hostnames.join(" ")} ${route.matches.map((m) => m.path).join(" ")} ${route.service_id || route.backend.host}`
        .toLowerCase()
        .includes(search.toLowerCase()),
    ) || [];
  return (
    <>
      <div hidden={Boolean(selected)}>
        <p className="section-note">
          Published routing snapshot #{routes.data?.generation ?? "—"}. Gateway
          acknowledgements are shown separately; this is not a live traffic
          probe.
        </p>
        <ErrorNotice error={gateways.error} stale={Boolean(gateways.data)} />
        <div
          className="gateway-summary"
          aria-label="Gateway application status"
        >
          {gateways.data?.nodes.map((node) => (
            <div
              key={node.node_id}
              className={`gateway-summary-item${node.error ? " has-error" : ""}`}
            >
              <div>
                <strong>{node.node_id}</strong>
                <Status value={node.status} />
              </div>
              <small>
                {node.address} · applied #{node.applied_generation ?? "—"} /
                desired #{node.desired_generation ?? "—"}
              </small>
              {node.error && <p>{node.error}</p>}
            </div>
          ))}
          {!gateways.loading && !gateways.data?.nodes.length && (
            <p className="section-note">No registered gateways.</p>
          )}
        </div>
        <div className="section-heading">
          <div>
            <h2>Routing map</h2>
            <p>
              {filtered.length} published rules · hostname → path → backend →
              upstream
            </p>
          </div>
          <div className="view-switch" aria-label="Route display">
            <Button
              size="sm"
              variant={view === "graph" ? "secondary" : "ghost"}
              aria-pressed={view === "graph"}
              onClick={() => setView("graph")}
            >
              Diagram
            </Button>
            <Button
              size="sm"
              variant={view === "list" ? "secondary" : "ghost"}
              aria-pressed={view === "list"}
              onClick={() => setView("list")}
            >
              List
            </Button>
          </div>
        </div>
        <div className="search-bar">
          <Search size={17} />
          <Input
            aria-label="Search routes"
            placeholder="Search hostname, path, Stack or service…"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
          />
        </div>
        <ErrorNotice error={routes.error} stale={Boolean(routes.data)} />
        {view === "graph" && filtered.length > 0 && (
          <RouteGraph
            routes={filtered}
            services={services}
            onInspect={setSelected}
            onService={onService}
          />
        )}
        {view === "list" && filtered.length > 0 && (
          <Card className="table-card">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Hostname / Stack</TableHead>
                  <TableHead>Path</TableHead>
                  <TableHead>Backend</TableHead>
                  <TableHead>Upstreams</TableHead>
                  <TableHead />
                </TableRow>
              </TableHeader>
              <TableBody>
                {filtered.map((route) => (
                  <TableRow key={route.id}>
                    <TableCell>
                      <button
                        className="resource-link"
                        onClick={() => setSelected(route.id)}
                      >
                        {route.hostnames.join(", ") || "Any hostname"}
                      </button>
                      <small className="block muted">{route.stack}</small>
                    </TableCell>
                    <TableCell>
                      {route.matches.map((m) => m.path).join(", ") ||
                        "All paths"}
                    </TableCell>
                    <TableCell>
                      {route.service_id || route.backend.host}
                    </TableCell>
                    <TableCell>{route.upstreams.length}</TableCell>
                    <TableCell>
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={() => setSelected(route.id)}
                      >
                        Inspect
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Card>
        )}
        {!filtered.length && (
          <Empty>
            {routes.loading
              ? "Loading routes…"
              : "No matching published routes."}
          </Empty>
        )}
        <JsonDetails
          title="Gateway configuration"
          value={gateways.data?.config}
        />
      </div>
      {selected && (
        <ResourceExplorer
          title="Routes"
          items={filtered.map((route) => ({
            id: route.id,
            title: route.hostnames.join(", ") || "Any hostname",
            description: `${route.matches.map((m) => m.path).join(", ") || "All paths"} · ${route.stack}`,
            meta: route.service_id || route.backend.host,
          }))}
          selected={selected}
          onSelect={setSelected}
          onClose={() => setSelected("")}
        >
          {filtered
            .filter((route) => route.id === selected)
            .map((route) => (
              <RouteDetail
                key={route.id}
                route={route}
                services={services}
                onService={onService}
              />
            ))}
        </ResourceExplorer>
      )}
    </>
  );
}

function RouteDetail({
  route,
  services,
  onService,
}: {
  route: RouteRow;
  services: Service[];
  onService: (service: Service) => void;
}) {
  const service = services.find((item) => item.id === route.service_id);
  return (
    <Card key={route.id}>
      <CardHeader>
        <CardTitle>{route.hostnames.join(", ") || "Any hostname"}</CardTitle>
        <p className="muted">
          {route.stack} · TLS {route.tls} · HTTP {route.http}
          {route.canonical_hostname
            ? ` · Canonical host: ${route.canonical_hostname}`
            : ""}
        </p>
      </CardHeader>
      <CardContent>
        <div className="route-chain">
          <div>
            <small>PATH MATCH</small>
            {route.matches.length ? (
              route.matches.map((match, index) => (
                <code key={index}>
                  {match.type} {match.path}
                  {match.ignore_case ? " (ignore case)" : ""}
                </code>
              ))
            ) : (
              <code>All paths</code>
            )}
          </div>
          <span>→</span>
          <div>
            <small>BACKEND</small>
            {service ? (
              <button
                className="resource-link"
                onClick={() => onService(service)}
              >
                {service.id}
                <ArrowUpRight size={14} />
              </button>
            ) : (
              <code>{route.service_id || route.backend.host}</code>
            )}
            <span className="muted">
              {route.backend.protocol} · target port {route.backend.port}
            </span>
          </div>
          <span>→</span>
          <div>
            <small>PUBLISHED UPSTREAMS</small>
            {route.upstreams.length ? (
              route.upstreams.map((endpoint) => (
                <code key={endpoint}>{endpoint}</code>
              ))
            ) : (
              <span className="route-warning">No upstreams available</span>
            )}
          </div>
        </div>
        {route.rewrite && (
          <p className="section-note">
            Rewrite:{" "}
            {route.rewrite.strip_prefix
              ? "strip matched prefix"
              : route.rewrite.replace_prefix !== undefined
                ? `replace prefix with ${route.rewrite.replace_prefix}`
                : `replace path with ${route.rewrite.replace_path}`}
          </p>
        )}
        {route.service_id && (
          <>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Replica</TableHead>
                  <TableHead>Node endpoint</TableHead>
                  <TableHead>Observed / desired</TableHead>
                  <TableHead>In routing snapshot</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {route.replicas.map((task) => (
                  <TableRow key={task.id}>
                    <TableCell className="mono">{task.id}</TableCell>
                    <TableCell>
                      {task.node}
                      <small className="block mono muted">
                        {task.endpoint || "No published port"}
                      </small>
                    </TableCell>
                    <TableCell>
                      <Status value={task.observed} />
                      <small className="block muted">{task.desired}</small>
                    </TableCell>
                    <TableCell>{task.routed ? "Yes" : "No"}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            {!route.replicas.length && (
              <p className="section-note">
                No matching task records. Retained upstreams may still be
                serving.
              </p>
            )}
          </>
        )}
      </CardContent>
    </Card>
  );
}
