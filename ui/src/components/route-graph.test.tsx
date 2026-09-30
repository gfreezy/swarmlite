import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, test, vi } from "vitest";
import { RouteGraph, type RouteRow } from "./route-graph";
afterEach(cleanup);
const route: RouteRow = {
  id: "r1",
  stack: "demo",
  hostnames: ["example.com"],
  tls: "serve",
  http: "redirect",
  matches: [{ type: "prefix", path: "/api", ignore_case: false }],
  backend: { service: "api", port: 80, protocol: "http" },
  service_id: "demo.api",
  upstreams: ["10.0.0.1:20000"],
  replicas: [],
};
test("map selection stays in the map, toggles by node, and inspection is explicit", async () => {
  const inspect = vi.fn();
  render(
    <RouteGraph
      routes={[route]}
      services={[]}
      onInspect={inspect}
      onService={vi.fn()}
    />,
  );
  const path = screen.getByRole("button", { name: "Trace path rule: /api" });
  const backend = screen.getByRole("button", {
    name: "Trace backend: demo.api",
  });
  await userEvent.click(path);
  expect(path.getAttribute("aria-pressed")).toBe("true");
  expect(inspect).not.toHaveBeenCalled();
  // A different node on the same route becomes selected rather than clearing it.
  await userEvent.click(backend);
  expect(backend.getAttribute("aria-pressed")).toBe("true");
  expect(path.getAttribute("aria-pressed")).toBe("false");
  await userEvent.click(
    screen.getByRole("button", { name: "/api → demo.api" }),
  );
  expect(inspect).toHaveBeenCalledWith("r1");
  await userEvent.click(backend);
  expect(backend.getAttribute("aria-pressed")).toBe("false");
  expect(screen.queryByLabelText("Selected route actions")).toBeNull();
});
test("Escape, empty canvas and clear button all cancel selection", async () => {
  render(
    <RouteGraph
      routes={[route]}
      services={[]}
      onInspect={vi.fn()}
      onService={vi.fn()}
    />,
  );
  const path = screen.getByRole("button", { name: "Trace path rule: /api" });
  await userEvent.click(path);
  await userEvent.keyboard("{Escape}");
  expect(path.getAttribute("aria-pressed")).toBe("false");
  await userEvent.click(path);
  fireEvent.click(
    screen.getByLabelText("Routing diagram canvas; scroll to explore"),
  );
  expect(path.getAttribute("aria-pressed")).toBe("false");
  await userEvent.click(path);
  await userEvent.click(
    screen.getByRole("button", { name: "Clear selection" }),
  );
  expect(path.getAttribute("aria-pressed")).toBe("false");
});
