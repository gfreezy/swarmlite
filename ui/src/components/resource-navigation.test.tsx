import { useState } from "react";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, test, vi } from "vitest";
import { ResourceExplorer } from "./resource-explorer";
import { ResourcePanel } from "./resource-panel";

const items = [
  { id: "a", title: "node-a", description: "10.0.0.1" },
  { id: "b", title: "node-b", description: "10.0.0.2" },
];
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});
function Harness() {
  const [selected, select] = useState("");
  return (
    <>
      <button hidden={Boolean(selected)} onClick={() => select("a")}>
        Open node
      </button>
      {selected && (
        <ResourceExplorer
          title="Nodes"
          items={items}
          selected={selected}
          onSelect={select}
          onClose={() => select("")}
        >
          <p>Details for {selected}</p>
        </ResourceExplorer>
      )}
    </>
  );
}
test("selection keeps navigation available, resets only detail scroll and preserves a filtered detail", async () => {
  render(<Harness />);
  await userEvent.click(screen.getByText("Open node"));
  const detail = screen.getByRole("region", { name: "Nodes detail" });
  detail.scrollTop = 400;
  await userEvent.click(screen.getByRole("button", { name: "Next item" }));
  expect(screen.getByText("Details for b")).toBeTruthy();
  expect(detail.scrollTop).toBe(0);
  expect(screen.getByRole("navigation", { name: "Select nodes" })).toBeTruthy();
  await userEvent.type(screen.getByLabelText("Find nodes"), "missing");
  expect(screen.getByText("No matching nodes.")).toBeTruthy();
  expect(screen.getByText("Details for b")).toBeTruthy();
  expect(
    (screen.getByRole("button", { name: "Next item" }) as HTMLButtonElement)
      .disabled,
  ).toBe(true);
});
test("returning restores the overview trigger focus", async () => {
  vi.spyOn(window, "scrollTo").mockImplementation(() => {});
  render(<Harness />);
  const open = screen.getByText("Open node");
  await userEvent.click(open);
  await userEvent.click(screen.getByRole("button", { name: "All nodes" }));
  await waitFor(() => expect(document.activeElement).toBe(open));
});
function PanelHarness() {
  const [selected, select] = useState("");
  return (
    <>
      <button onClick={() => select("a")}>Open record</button>
      {selected && (
        <ResourcePanel
          title="Runtime"
          items={items.map((item) => ({ id: item.id, label: item.title }))}
          selected={selected}
          onSelect={select}
          onClose={() => select("")}
        >
          <p>Record {selected}</p>
        </ResourcePanel>
      )}
    </>
  );
}
test("nested records can switch without losing the parent and Escape returns focus", async () => {
  render(<PanelHarness />);
  const open = screen.getByText("Open record");
  await userEvent.click(open);
  await userEvent.click(screen.getByRole("button", { name: "Next record" }));
  expect(screen.getByText("Record b")).toBeTruthy();
  await userEvent.keyboard("{Escape}");
  await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  expect(document.activeElement).toBe(open);
});
