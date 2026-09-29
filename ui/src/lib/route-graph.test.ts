import { expect, test } from "vitest";
import { buildRouteGraph, type RouteRow } from "./route-graph";
const route: RouteRow = {
  id: "r1",
  stack: "demo",
  hostnames: ["example.com"],
  tls: "serve",
  http: "redirect",
  matches: [{ path: "/api", type: "prefix", ignore_case: false }],
  backend: { service: "api", port: 8080, protocol: "http" },
  service_id: "demo.api",
  upstreams: ["10.0.0.1:20000", "10.0.0.2:20000"],
  replicas: [
    {
      id: "not-routed",
      node: "node3",
      endpoint: "10.0.0.3:20000",
      observed: "healthy",
      desired: "running",
      routed: false,
    },
  ],
};
test("shared backends form a directed graph without duplicating upstreams or routing unpublished tasks", () => {
  const graph = buildRouteGraph([
    route,
    {
      ...route,
      id: "r2",
      matches: [{ path: "/v2", type: "prefix", ignore_case: false }],
    },
  ]);
  expect(graph.nodes.filter((n) => n.column === 0)).toHaveLength(1);
  expect(graph.nodes.filter((n) => n.column === 1)).toHaveLength(2);
  expect(graph.nodes.filter((n) => n.column === 2)).toHaveLength(1);
  expect(graph.nodes.filter((n) => n.column === 3)).toHaveLength(2);
  expect(graph.nodes.some((n) => n.title === "10.0.0.3:20000")).toBe(false);
  for (const edge of graph.edges) {
    const from = graph.nodes.find((n) => n.id === edge.from)!;
    const to = graph.nodes.find((n) => n.id === edge.to)!;
    expect(to.column).toBe(from.column + 1);
    expect(edge.routes.length).toBe(from.column === 2 ? 2 : 1);
  }
  expect(
    graph.nodes.find((n) => n.title === "10.0.0.1:20000")?.subtitle,
  ).toContain("Retained");
});
test("missing targets use diagnostic edges and external endpoints remain distinct", () => {
  const graph = buildRouteGraph([
    { ...route, upstreams: [] },
    {
      ...route,
      id: "external",
      service_id: undefined,
      backend: { host: "origin.example", port: 443, protocol: "https" },
      upstreams: ["origin.example:443"],
      replicas: [],
    },
  ]);
  expect(graph.edges.filter((e) => e.missing)).toHaveLength(1);
  expect(
    graph.nodes.find((n) => n.title === "No published upstreams")?.tone,
  ).toBe("warning");
  expect(
    graph.nodes.find((n) => n.title === "origin.example:443")?.subtitle,
  ).toBe("External endpoint");
});
