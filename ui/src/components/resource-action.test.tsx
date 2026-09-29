import { afterEach, expect, test, vi } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ResourceActionDialog, type ResourceAction } from "./resource-action";
import { post, OperationError } from "@/lib/api";
vi.mock("@/lib/api", async (original) => ({
  ...(await original<object>()),
  hasSession: true,
  post: vi.fn(),
  get: vi
    .fn()
    .mockResolvedValue({
      id: "run1",
      status: "succeeded",
      exit_code: 0,
      stdout: "Done",
    }),
}));
afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});
const action: ResourceAction = {
  command: "service scale",
  values: { services: ["demo.web=2"] },
  key: 1,
  scope: "workload:demo.web",
};
test("scales the selected service in place and refreshes after completion", async () => {
  let resolve!: (value: unknown) => void;
  vi.mocked(post).mockReturnValue(
    new Promise((r) => {
      resolve = r;
    }),
  );
  const complete = vi.fn();
  render(
    <ResourceActionDialog
      action={action}
      onClose={() => {}}
      onAccepted={() => {}}
      onComplete={complete}
    />,
  );
  const input = screen.getByLabelText("Replicas");
  await userEvent.clear(input);
  await userEvent.type(input, "4");
  await userEvent.dblClick(
    screen.getByRole("button", { name: "Scale service" }),
  );
  expect(post).toHaveBeenCalledTimes(1);
  expect(vi.mocked(post).mock.calls[0][1]).toMatchObject({
    command: "service scale",
    values: { services: ["demo.web=4"] },
  });
  resolve({ id: "run1" });
  await waitFor(() => expect(complete).toHaveBeenCalledTimes(1));
  expect(screen.getByText("Done")).toBeTruthy();
});
test("lost responses recover the same submission without changing its target", async () => {
  vi.mocked(post)
    .mockRejectedValueOnce(new OperationError("Response lost", true))
    .mockResolvedValueOnce({ id: "run1" });
  render(
    <ResourceActionDialog
      action={action}
      onClose={() => {}}
      onAccepted={() => {}}
      onComplete={() => {}}
    />,
  );
  await userEvent.click(screen.getByRole("button", { name: "Scale service" }));
  await screen.findByText("Response lost");
  expect(screen.getByLabelText("Replicas").matches(":disabled")).toBe(true);
  await userEvent.click(
    screen.getByRole("button", { name: "Recover submission" }),
  );
  expect(vi.mocked(post).mock.calls[0][1]).toEqual(
    vi.mocked(post).mock.calls[1][1],
  );
});
test("a completed failed action can be reopened from its resource activity", async () => {
  const { get } = await import("@/lib/api");
  vi.mocked(get).mockResolvedValueOnce({
    id: "failed-run",
    status: "failed",
    exit_code: 1,
    stderr: "Controller rejected the request",
  });
  render(
    <ResourceActionDialog
      action={{ ...action, runId: "failed-run" }}
      onClose={() => {}}
      onAccepted={() => {}}
      onComplete={() => {}}
    />,
  );
  await screen.findByText("Controller rejected the request");
  expect(screen.queryByLabelText("Replicas")).toBeNull();
  expect(post).not.toHaveBeenCalled();
});
