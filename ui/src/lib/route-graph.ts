export interface RouteRow {
  id: string;
  stack: string;
  hostnames: string[];
  canonical_hostname?: string;
  tls: string;
  http: string;
  matches: { path: string; type: string; ignore_case: boolean }[];
  rewrite?: {
    strip_prefix?: boolean;
    replace_prefix?: string;
    replace_path?: string;
  };
  backend: { service?: string; host?: string; port: number; protocol: string };
  service_id?: string;
  upstreams: string[];
  replicas: {
    id: string;
    node: string;
    endpoint?: string;
    observed: string;
    desired: string;
    routed: boolean;
  }[];
}
interface GraphNode {
  id: string;
  column: number;
  title: string;
  subtitle: string;
  meta: string;
  routes: string[];
  service?: string;
  tone?: "warning" | "error";
  x: number;
  y: number;
}
interface GraphEdge {
  id: string;
  from: string;
  to: string;
  routes: string[];
  missing: boolean;
}
export const WIDTH = 200;
export const HEIGHT = 104;
export const STEP = 244;
export const PAD = 24;
const ROW = 132;
export const stages = [
  "Hostname",
  "Path rule",
  "Backend",
  "Published upstream",
];

export function buildRouteGraph(routes: RouteRow[]) {
  const nodes = new Map<string, GraphNode>();
  const edges = new Map<string, GraphEdge>();
  function node(
    id: string,
    route: RouteRow,
    info: Omit<GraphNode, "id" | "routes" | "x" | "y">,
  ) {
    if (!nodes.has(id)) nodes.set(id, { id, ...info, routes: [], x: 0, y: 0 });
    const n = nodes.get(id)!;
    if (!n.routes.includes(route.id)) n.routes.push(route.id);
    return id;
  }
  function edge(from: string, to: string, route: RouteRow, missing = false) {
    const id = JSON.stringify([from, to]);
    if (!edges.has(id)) edges.set(id, { id, from, to, routes: [], missing });
    const e = edges.get(id)!;
    if (!e.routes.includes(route.id)) e.routes.push(route.id);
  }
  for (const route of routes) {
    const host = node(
      JSON.stringify([
        "host",
        route.stack,
        route.hostnames,
        route.tls,
        route.http,
        route.canonical_hostname,
      ]),
      route,
      {
        column: 0,
        title: route.hostnames.join(", ") || "Any hostname",
        subtitle: route.stack,
        meta: `TLS ${route.tls} · HTTP ${route.http}`,
      },
    );
    const rule = node(JSON.stringify(["rule", route.id]), route, {
      column: 1,
      title: route.matches.map((m) => m.path).join(" OR ") || "All paths",
      subtitle:
        route.matches
          .map((m) => `${m.type}${m.ignore_case ? " · ignore case" : ""}`)
          .join(" / ") || "Catch-all",
      meta: route.rewrite ? "Rewrites request path" : "Preserves request path",
    });
    const backend = node(
      JSON.stringify([
        "backend",
        route.stack,
        route.service_id,
        route.backend.host,
        route.backend.port,
        route.backend.protocol,
      ]),
      route,
      {
        column: 2,
        title: route.service_id || route.backend.host || "Unknown backend",
        subtitle: `${route.backend.protocol.toUpperCase()} · target port ${route.backend.port}`,
        meta: route.service_id ? "Service" : "External backend",
        service: route.service_id,
      },
    );
    edge(host, rule, route);
    edge(rule, backend, route);
    if (!route.upstreams.length) {
      const missing = node(JSON.stringify(["missing", backend]), route, {
        column: 3,
        title: "No published upstreams",
        subtitle: "No target in routing snapshot",
        meta: "Needs attention",
        tone: "warning",
      });
      edge(backend, missing, route, true);
    }
    for (const endpoint of route.upstreams) {
      const tasks = route.replicas.filter(
        (t) => t.endpoint === endpoint && t.routed,
      );
      const failed = tasks.some((t) =>
        ["failed", "unhealthy", "lost", "stopped", "exited", "error"].includes(
          t.observed,
        ),
      );
      const target = node(
        JSON.stringify(["upstream", backend, endpoint]),
        route,
        {
          column: 3,
          title: endpoint,
          subtitle: tasks.length
            ? [...new Set(tasks.map((t) => t.node))].join(", ")
            : route.service_id
              ? "Retained · no matching task record"
              : "External endpoint",
          meta: tasks.length
            ? [...new Set(tasks.map((t) => t.observed))].join(", ")
            : "In routing snapshot",
          tone: failed ? "error" : undefined,
        },
      );
      edge(backend, target, route);
    }
  }
  const columns = stages.map((_, column) =>
    [...nodes.values()].filter((n) => n.column === column),
  );
  const height = Math.max(1, ...columns.map((c) => c.length)) * ROW + 72;
  // Stable layered layout; shared backends and endpoints remain shared nodes.
  for (let column = 0; column < columns.length; column++) {
    if (column > 0) {
      const parentCenter = (n: GraphNode) => {
        const parents = [...edges.values()]
          .filter((e) => e.to === n.id)
          .map((e) => nodes.get(e.from)!.y);
        return parents.reduce((sum, y) => sum + y, 0) / (parents.length || 1);
      };
      columns[column].sort((a, b) => parentCenter(a) - parentCenter(b));
    }
    columns[column].forEach((n, index) => {
      n.x = PAD + column * STEP;
      n.y = 54 + (height - 72 - columns[column].length * ROW) / 2 + index * ROW;
    });
  }
  return {
    nodes: [...nodes.values()],
    edges: [...edges.values()],
    width: PAD * 2 + STEP * 3 + WIDTH,
    height,
  };
}
