import { afterEach, expect, test, vi } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { DeploymentCompare } from "./deployment-compare";
import { JobDetail } from "./job-detail";
import { RoutesView } from "./routes-view";
import { get, type Stack, type Service } from "@/lib/api";
vi.mock("@/lib/api", async (original) => ({
  ...(await original<object>()),
  hasSession: true,
  get: vi.fn(),
}));
afterEach(() => {
  cleanup();
  vi.resetAllMocks();
});
const stack = {
  stack: "demo",
  current: { generation: 42 },
  history: [{ generation: 41 }],
} as Stack;
test("changing compared generations discards old results and displays missing-snapshot errors", async () => {
  vi.mocked(get)
    .mockResolvedValueOnce({
      from: 41,
      to: 42,
      changes: [
        {
          path: "/services/web/environment/TOKEN",
          kind: "changed",
          before: "old-value",
          after: "new-value",
        },
      ],
    })
    .mockRejectedValue(new Error("Generation #41 is no longer retained."));
  render(<DeploymentCompare stack={stack} refreshToken={0} />);
  await screen.findByText("/services/web/environment/TOKEN");
  expect(screen.getByText("old-value")).toBeTruthy();
  expect(screen.getByText("new-value")).toBeTruthy();
  await userEvent.click(
    screen.getByRole("combobox", { name: "Compare to generation" }),
  );
  await userEvent.click(screen.getByRole("option", { name: "#41" }));
  await screen.findByRole("alert");
  expect(screen.queryByText("/services/web/environment/TOKEN")).toBeNull();
  expect(vi.mocked(get).mock.calls[1][0]).toContain("from=41&to=41");
});
test("job panel renders exit zero and duration with no mutation controls", async () => {
  const service = {
    id: "demo.backup",
    stack: "demo",
    job: { schedule: "0 2 * * *", timezone: "UTC", suspend: false },
  } as Service;
  vi.mocked(get).mockImplementation(async (path) =>
    path.endsWith("/history")
      ? [
          {
            id: "run1",
            node_id: "node1",
            observed: "succeeded",
            desired: "stopped",
            scheduled_at_unix_ms: 1000,
            started_at_unix_ms: 1000,
            finished_at_unix_ms: 32000,
            exit_code: 0,
            stop_reason: "completed",
          },
        ]
      : { policy: service.job, next_at_unix_ms: 90000 },
  );
  render(<JobDetail service={service} refreshToken={0} />);
  await screen.findByText("run1");
  expect(screen.getByText("31s")).toBeTruthy();
  expect(screen.getByText("0")).toBeTruthy();
  expect(screen.queryByRole("button", { name: /run now|cancel/i })).toBeNull();
});
test("routes distinguish retained upstreams from gateway application and link services", async () => {
  const service = { id: "demo.web" } as Service;
  const open = vi.fn();
  vi.mocked(get).mockImplementation(async (path) =>
    path === "/gateways"
      ? {
          generation: 9,
          nodes: [
            {
              node_id: "node1",
              address: "10.0.0.1",
              status: "error",
              desired_generation: 9,
              applied_generation: 8,
              error: "Caddy unavailable",
            },
          ],
        }
      : {
          generation: 9,
          routes: [
            {
              id: "r1",
              stack: "demo",
              hostnames: ["example.com"],
              tls: "serve",
              http: "redirect",
              matches: [{ type: "prefix", path: "/api" }],
              backend: { service: "web", port: 80, protocol: "http" },
              service_id: "demo.web",
              upstreams: ["10.0.0.1:20000"],
              replicas: [],
            },
          ],
        },
  );
  render(<RoutesView services={[service]} onService={open} refreshToken={0} />);
  await screen.findByText("example.com");
  expect(screen.getByText("Caddy unavailable")).toBeTruthy();
  expect(screen.getByText("10.0.0.1:20000")).toBeTruthy();
  await userEvent.click(screen.getByRole("button", { name: "demo.web" }));
  expect(open).toHaveBeenCalledWith(service);
  await userEvent.type(
    screen.getByLabelText("Search routes"),
    "missing.example",
  );
  await waitFor(() => expect(screen.queryByText("example.com")).toBeNull());
});
