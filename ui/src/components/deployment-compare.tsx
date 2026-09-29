import { Select } from "@/components/ui/select";
import { useState } from "react";
import { Card } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Empty, ErrorNotice } from "@/components/status";
import { get, type Stack } from "@/lib/api";
import { usePolling } from "@/lib/use-polling";

interface Comparison {
  from: number;
  to: number;
  changes: {
    path: string;
    kind: string;
    before: unknown;
    after: unknown;
  }[];
}
const display = (value: unknown) =>
  value === null
    ? "—"
    : typeof value === "string"
      ? value
      : JSON.stringify(value, null, 2);
export function DeploymentCompare({
  stack,
  refreshToken,
}: {
  stack: Stack;
  refreshToken: number;
}) {
  const generations = [
    ...new Set([
      ...(stack.current ? [stack.current.generation] : []),
      ...stack.history.map((item) => item.generation),
    ]),
  ].sort((a, b) => b - a);
  const [from, setFrom] = useState(generations[1] ?? generations[0]);
  const [to, setTo] = useState(generations[0]);
  return (
    <section className="comparison">
      <div className="section-heading">
        <div>
          <h2>Compare deployed configuration</h2>
          <p>
            Compare configuration saved in the Controller’s deployment
            snapshots.
          </p>
        </div>
      </div>
      <div className="resource-toolbar">
        <div className="compare-pickers">
          <label>
            From
            <Select
              aria-label="Compare from generation"
              className="generation-select"
              value={from}
              onValueChange={(value) => setFrom(Number(value))}
            >
              {generations.map((g) => (
                <option key={g} value={g}>
                  #{g}
                </option>
              ))}
            </Select>
          </label>
          <span>→</span>
          <label>
            To
            <Select
              aria-label="Compare to generation"
              className="generation-select"
              value={to}
              onValueChange={(value) => setTo(Number(value))}
            >
              {generations.map((g) => (
                <option key={g} value={g}>
                  #{g}
                </option>
              ))}
            </Select>
          </label>
        </div>
      </div>
      {from !== undefined && to !== undefined ? (
        <ComparisonResult
          key={`${stack.stack}:${from}:${to}`}
          name={stack.stack}
          from={from}
          to={to}
          refreshToken={refreshToken}
        />
      ) : (
        <Empty>No retained snapshots to compare.</Empty>
      )}
    </section>
  );
}
function ComparisonResult({
  name,
  from,
  to,
  refreshToken,
}: {
  name: string;
  from: number;
  to: number;
  refreshToken: number;
}) {
  const result = usePolling(
    (signal) =>
      get<Comparison>(
        `/stacks/${encodeURIComponent(name)}/compare?from=${from}&to=${to}`,
        signal,
      ),
    `${name}:${from}:${to}`,
    refreshToken,
  );
  return (
    <>
      <ErrorNotice error={result.error} stale={Boolean(result.data)} />
      <Card className="table-card">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Field</TableHead>
              <TableHead>Change</TableHead>
              <TableHead>Before #{from}</TableHead>
              <TableHead>After #{to}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {result.data?.changes.map((change) => (
              <TableRow key={change.path}>
                <TableCell className="diff-path">{change.path}</TableCell>
                <TableCell>{change.kind}</TableCell>
                <TableCell>
                  <pre className="diff-value">{display(change.before)}</pre>
                </TableCell>
                <TableCell>
                  <pre className="diff-value">{display(change.after)}</pre>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
        {!result.data?.changes.length && (
          <Empty>
            {result.loading
              ? "Comparing snapshots…"
              : result.error
                ? "Comparison unavailable."
                : "No configuration differences."}
          </Empty>
        )}
      </Card>
    </>
  );
}
