import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, test, vi } from "vitest";
import { MonitoringRangePicker } from "./monitoring-range";
import { validateRange, localInput } from "@/lib/monitoring-range";
afterEach(cleanup);
test("custom ranges reject reversed, future and expired intervals", () => {
  const now = Date.parse("2026-09-30T12:00:00Z");
  expect(
    validateRange("2026-09-30T10:00:00Z", "2026-09-30T11:00:00Z", now),
  ).toBe("");
  expect(
    validateRange("2026-09-30T11:00:00Z", "2026-09-30T10:00:00Z", now),
  ).toContain("after");
  expect(
    validateRange("2026-09-30T10:00:00Z", "2026-09-30T13:00:00Z", now),
  ).toContain("future");
  expect(validateRange("2025-01-01", "2026-09-29", now)).toContain("365");
});
test("quick ranges and custom date form apply explicit query ranges", async () => {
  const change = vi.fn();
  render(<MonitoringRangePicker value={{ seconds: 300 }} onChange={change} />);
  await userEvent.click(
    screen.getByRole("button", { name: "Last 24 hours" }),
  );
  expect(change).toHaveBeenLastCalledWith({ seconds: 86400 });
  await userEvent.click(
    screen.getByRole("button", { name: "Choose monitoring time range" }),
  );
  const from = localInput(Date.now() - 7200000),
    to = localInput(Date.now() - 3600000);
  fireEvent.change(screen.getByLabelText("Start"), { target: { value: from } });
  fireEvent.change(screen.getByLabelText("End"), { target: { value: to } });
  await userEvent.click(screen.getByRole("button", { name: "Apply range" }));
  expect(change).toHaveBeenLastCalledWith({
    from: new Date(from).getTime(),
    to: new Date(to).getTime(),
  });
});
