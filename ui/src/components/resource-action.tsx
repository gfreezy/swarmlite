import { Select } from "@/components/ui/select";
import { useEffect, useRef, useState } from "react";
import { Dialog as DialogPrimitive } from "radix-ui";
import { X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ErrorNotice, Status } from "@/components/status";
import { get, post, OperationError, timestamp } from "@/lib/api";
import { usePolling } from "@/lib/use-polling";

export interface ActionOptions {
  valueType?: string;
  choices?: string[];
  description?: string;
  generations?: number[];
}
export interface ResourceAction {
  command: string;
  values?: Record<string, string[]>;
  options?: ActionOptions;
  key: number;
  scope: string;
  runId?: string;
}
export type Operate = (
  command: string,
  values?: Record<string, string[]>,
  options?: ActionOptions,
) => void;
export interface ActionRun {
  id: string;
  command: string;
  started: number;
  status: string;
  exit_code?: number;
  stdout?: string;
  stderr?: string;
  truncated: boolean;
}
interface Submission {
  id: string;
  command: string;
  values: Record<string, string[]>;
  stdin: string;
  confirmed: true;
}
const definitions: Record<
  string,
  { title: string; submit: string; description: string }
> = {
  "service scale": {
    title: "Scale service",
    submit: "Scale service",
    description:
      "Set the desired number of replicas. The Controller will create or remove tasks to match.",
  },
  "service restart": {
    title: "Rolling restart",
    submit: "Restart service",
    description:
      "Replace this service’s tasks using its current configuration and rollout policy.",
  },
  "job run": {
    title: "Run job",
    submit: "Run job",
    description:
      "Start a manual execution. An unfinished execution may prevent a new run.",
  },
  "job cancel": {
    title: "Cancel execution",
    submit: "Cancel execution",
    description:
      "Request termination of this job execution. Completed work is not reversed.",
  },
  "deployment retry": {
    title: "Retry deployment",
    submit: "Retry deployment",
    description:
      "Retry the current stalled, blocked or failed deployment using its saved configuration.",
  },
  "deployment rollback": {
    title: "Roll back deployment",
    submit: "Roll back",
    description:
      "Create a new deployment from a retained generation’s configuration.",
  },
  rm: {
    title: "Remove stack",
    submit: "Remove stack",
    description:
      "Remove this stack and stop its workloads. This action changes the cluster immediately.",
  },
  "config set": {
    title: "Edit setting",
    submit: "Save setting",
    description:
      "Update this cluster setting. Its documented apply behavior determines when it takes effect.",
  },
  "config unset": {
    title: "Reset setting",
    submit: "Reset to default",
    description:
      "Clear the configured value and use the built-in or Caddy default.",
  },
  "node label set": {
    title: "Set node label",
    submit: "Save label",
    description: "Update this node’s authoritative placement labels.",
  },
  "node label remove": {
    title: "Remove node label",
    submit: "Remove label",
    description: "Remove a placement label from this node.",
  },
  "gateway enable": {
    title: "Enable gateway",
    submit: "Enable gateway",
    description: "Enable the shared gateway on this node.",
  },
  "gateway disable": {
    title: "Disable gateway",
    submit: "Disable gateway",
    description: "Stop serving gateway traffic from this node.",
  },
  "registry login": {
    title: "Registry credentials",
    submit: "Save credentials",
    description:
      "Store credentials that all cluster nodes use to pull private images.",
  },
  "connection-info": {
    title: "Local connection details",
    submit: "Show connection details",
    description:
      "Read the Controller address and cluster token stored on the machine running this UI. These may differ from the currently connected cluster.",
  },
  "join-token": {
    title: "Node join command",
    submit: "Show join command",
    description:
      "Read the join command and cluster token stored on the machine running this UI.",
  },
  inspect: {
    title: "Full workload definition",
    submit: "Show full definition",
    description:
      "Show the complete saved definition and tasks, including environment values and other sensitive configuration.",
  },
  status: {
    title: "Full cluster state",
    submit: "Show cluster state",
    description:
      "Show the complete Controller state. The result may contain sensitive workload configuration.",
  },
  ls: {
    title: "Workload definitions",
    submit: "Show definitions",
    description:
      "Show the saved service and job definitions, including sensitive configuration values.",
  },
  "job history": {
    title: "Execution records",
    submit: "Show execution records",
    description:
      "Show full retained job execution records, including saved configuration.",
  },
  "deployment attach": {
    title: "Deployment progress",
    submit: "Follow progress",
    description: "Follow progress until the selected deployment finishes.",
  },
  "deployment status": {
    title: "Deployment state",
    submit: "Show deployment state",
    description: "Show the full state of the selected deployment.",
  },
  "deployment history": {
    title: "Deployment records",
    submit: "Show deployment records",
    description: "Show current and retained deployment records.",
  },
};
function actionTitle(command: string) {
  return definitions[command]?.title || command;
}
function subject(action: ResourceAction) {
  const v = action.values || {};
  if (action.command === "service scale")
    return (
      v.services?.map((s) => s.slice(0, s.lastIndexOf("="))).join(", ") || ""
    );
  return (
    v.service ||
    v.target ||
    v.stack ||
    v.stacks ||
    v.node_id ||
    v.task_id ||
    v.key ||
    []
  ).join(", ");
}

export function ResourceActionDialog({
  action,
  onClose,
  onAccepted,
  onComplete,
}: {
  action: ResourceAction;
  onClose: () => void;
  onAccepted: (id: string) => void;
  onComplete: () => void;
}) {
  const definition = definitions[action.command];
  const [values, setValues] = useState<Record<string, string[]>>(
    action.values || {},
  );
  const [replicas, setReplicas] = useState(
    action.values?.services?.[0]?.split("=").pop() || "1",
  );
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [uncertain, setUncertain] = useState(false);
  const [error, setError] = useState("");
  const [runId, setRunId] = useState(action.runId || "");
  const [stopping, setStopping] = useState(false);
  const pending = useRef<Submission | undefined>(undefined);
  const inFlight = useRef(false);
  const completed = useRef(false);
  const result = usePolling(
    (signal) =>
      runId
        ? get<ActionRun>(`/commands/${runId}`, signal)
        : Promise.resolve(undefined),
    `resource-action:${runId}`,
  );
  const run = result.data?.id === runId ? result.data : undefined;
  useEffect(() => {
    if (run && run.status !== "running" && !completed.current) {
      completed.current = true;
      onComplete();
    }
  }, [run, onComplete]);
  const change = (id: string, value: string) => {
    setValues({ ...values, [id]: value ? [value] : [] });
    setError("");
  };
  const field = (
    id: string,
    label: string,
    options: {
      required?: boolean;
      type?: string;
      choices?: string[];
      placeholder?: string;
    } = {},
  ) => (
    <label key={id}>
      <span>{label}</span>
      {options.choices ? (
        <Select
          aria-label={label}
          required={options.required}
          value={values[id]?.[0] || ""}
          onValueChange={(value) => change(id, value)}
        >
          <option value="">{options.placeholder || "Choose a value"}</option>
          {options.choices.map((choice) => (
            <option key={choice}>{choice}</option>
          ))}
        </Select>
      ) : (
        <Input
          aria-label={label}
          autoComplete="off"
          required={options.required}
          type={options.type || "text"}
          min={options.type === "number" ? 0 : undefined}
          step={options.type === "number" ? 1 : undefined}
          placeholder={options.placeholder}
          value={values[id]?.[0] || ""}
          onChange={(e) => change(id, e.target.value)}
        />
      )}
    </label>
  );
  async function submit() {
    if (inFlight.current) return;
    inFlight.current = true;
    setBusy(true);
    setError("");
    const args = { ...values };
    if (action.command === "service scale")
      args.services = [`${subject(action)}=${replicas}`];
    if (action.command === "registry login") args.password_stdin = ["true"];
    const request = pending.current || {
      id: crypto.randomUUID(),
      command: action.command,
      values: args,
      stdin: password,
      confirmed: true as const,
    };
    pending.current = request;
    try {
      const accepted = await post<ActionRun>("/commands", request);
      setRunId(accepted.id);
      setUncertain(false);
      setPassword("");
      onAccepted(accepted.id);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
      const unknown = cause instanceof OperationError && cause.uncertain;
      setUncertain(unknown);
      if (!unknown) pending.current = undefined;
    } finally {
      inFlight.current = false;
      setBusy(false);
    }
  }
  return (
    <DialogPrimitive.Root
      open
      onOpenChange={(open) => {
        if (!open && !busy && !uncertain) onClose();
      }}
    >
      <DialogPrimitive.Portal>
        <DialogPrimitive.Overlay className="action-overlay" />
        <DialogPrimitive.Content
          className="action-dialog"
          onPointerDownOutside={(e) => e.preventDefault()}
        >
          <div className="section-heading">
            <DialogPrimitive.Title>{definition.title}</DialogPrimitive.Title>
            <Button
              variant="ghost"
              size="sm"
              aria-label="Close action"
              disabled={busy || uncertain}
              onClick={onClose}
            >
              <X />
            </Button>
          </div>
          {subject(action) && (
            <p className="action-subject mono">{subject(action)}</p>
          )}
          <DialogPrimitive.Description className="muted">
            {definition.description}
          </DialogPrimitive.Description>
          <ErrorNotice error={error || result.error} />
          {!runId ? (
            <form
              onSubmit={(e) => {
                e.preventDefault();
                void submit();
              }}
            >
              <fieldset
                className="operation-fields"
                disabled={busy || uncertain}
              >
                {action.command === "service scale" && (
                  <label>
                    <span>Replicas</span>
                    <Input
                      aria-label="Replicas"
                      type="number"
                      min={0}
                      step={1}
                      required
                      value={replicas}
                      onChange={(e) => setReplicas(e.target.value)}
                    />
                  </label>
                )}
                {action.command === "deployment rollback" &&
                  field("generation", "Restore generation", {
                    type: "number",
                    choices: action.options?.generations?.map(String),
                    placeholder: "Latest previous healthy generation",
                  })}
                {action.command === "config set" &&
                  field("value", "Value", {
                    required: true,
                    choices:
                      action.options?.valueType === "bool"
                        ? ["true", "false"]
                        : action.options?.choices,
                  })}
                {action.options?.description && (
                  <p className="section-note">{action.options.description}</p>
                )}
                {(action.command === "node label set" ||
                  action.command === "node label remove") &&
                  field("key", "Label name", {
                    required: true,
                    choices:
                      action.command === "node label remove"
                        ? action.options?.choices
                        : undefined,
                  })}
                {action.command === "node label set" &&
                  field("value", "Label value", { required: true })}
                {action.command === "registry login" && (
                  <>
                    {field("registry", "Registry hostname", {
                      required: true,
                      placeholder: "ghcr.io",
                    })}
                    {field("username", "Username", { required: true })}
                    <label>
                      <span>Password / access token</span>
                      <Input
                        aria-label="Password / access token"
                        type="password"
                        autoComplete="new-password"
                        required
                        value={password}
                        onChange={(e) => setPassword(e.target.value)}
                      />
                    </label>
                  </>
                )}
              </fieldset>
              {uncertain ? (
                <div className="operation-review">
                  <p>
                    The response was lost. Recover this submission to check
                    whether it started; this does not send a second action.
                  </p>
                  <Button
                    type="button"
                    disabled={busy}
                    onClick={() => void submit()}
                  >
                    Recover submission
                  </Button>
                </div>
              ) : (
                <div className="action-footer">
                  <Button
                    type="button"
                    variant="outline"
                    disabled={busy}
                    onClick={onClose}
                  >
                    Cancel
                  </Button>
                  <Button type="submit" disabled={busy}>
                    {busy ? "Submitting…" : definition.submit}
                  </Button>
                </div>
              )}
            </form>
          ) : (
            <div className="action-result">
              <div className="resource-toolbar">
                <Status value={run?.status || "running"} />
                <span className="muted">
                  {run?.exit_code != null
                    ? `Exit ${run.exit_code}`
                    : "Waiting for result…"}
                </span>
              </div>
              {run?.status === "running" && (
                <details className="json-details">
                  <summary>Stop waiting…</summary>
                  <p>
                    Stop this local request. Changes already accepted by the
                    Controller will continue.
                  </p>
                  <Button
                    variant="outline"
                    disabled={stopping}
                    onClick={async () => {
                      setStopping(true);
                      try {
                        await post(`/commands/${runId}/stop`, {});
                        result.refresh();
                      } catch (cause) {
                        setError(String(cause));
                      } finally {
                        setStopping(false);
                      }
                    }}
                  >
                    Stop local request
                  </Button>
                </details>
              )}
              {run?.truncated && (
                <p className="section-note">
                  Showing the most recent output; earlier output was discarded.
                </p>
              )}
              {run?.stdout && (
                <pre className="command-output">{run.stdout}</pre>
              )}
              {run?.stderr && (
                <pre className="command-output">{run.stderr}</pre>
              )}
              <p className="muted">
                {run?.status === "succeeded"
                  ? "Completed. This page has been refreshed."
                  : run?.status === "failed"
                    ? "The request failed. See the details above."
                    : run?.status === "stopped"
                      ? "Local request stopped."
                      : "You can close this dialog and return to the result in this page’s recent activity."}
              </p>
              <div className="action-footer">
                <Button onClick={onClose}>Close</Button>
              </div>
            </div>
          )}
        </DialogPrimitive.Content>
      </DialogPrimitive.Portal>
    </DialogPrimitive.Root>
  );
}

export function ResourceActivity({
  actions,
  onOpen,
}: {
  actions: ResourceAction[];
  onOpen: (action: ResourceAction) => void;
}) {
  const result = usePolling(
    (signal) => get<{ runs: ActionRun[] }>("/commands", signal),
    `activity:${actions.map((a) => a.runId).join(",")}`,
  );
  return (
    <section className="resource-activity">
      <h2>Recent activity</h2>
      <ErrorNotice error={result.error} />
      {actions.map((action) => {
        const run = result.data?.runs.find((r) => r.id === action.runId);
        return (
          <button key={action.runId} onClick={() => onOpen(action)}>
            <strong>{actionTitle(action.command)}</strong>
            <span>{subject(action)}</span>
            <Status value={run?.status || "running"} />
            <small>{timestamp(run?.started)}</small>
          </button>
        );
      })}
    </section>
  );
}
