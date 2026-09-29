export interface Service {
  id: string;
  stack: string;
  name: string;
  image: string;
  replicas: number;
  running_replicas: number;
  job?: {
    schedule?: string;
    timezone: string;
    suspend: boolean;
    timeout_seconds?: number;
  };
}
export interface Task {
  id: string;
  stack: string;
  service: string;
  slot: number;
  node_id: string;
  desired: string;
  observed: string;
  image: string;
  error?: string;
  ports: { published?: number; target: number; protocol: string }[];
}
export interface DeploymentSummary {
  generation: number;
  status: string;
  started_at_unix_ms: number;
  finished_at_unix_ms?: number;
}
export interface Deployment extends DeploymentSummary {
  revision: number;
  last_progress_at_unix_ms: number;
  services: {
    service: string;
    replicas: number;
    applied: number;
    healthy: number;
  }[];
  task_phases?: { phase: string; tasks: number }[];
  image_resolutions?: {
    service: string;
    image: string;
    status: string;
    completed_nodes: number;
    total_nodes: number;
  }[];
  gateway?: {
    generation: number;
    applied_nodes: number;
    total_nodes: number;
    errors?: Record<string, string>;
  };
  errors?: {
    message: string;
    service?: string;
    node_id?: string;
    phase?: string;
  }[];
  conditions?: { kind: string; message: string }[];
}
export interface Stack {
  stack: string;
  current: Deployment | null;
  history: DeploymentSummary[];
}
export interface Overview {
  cluster_id: string;
  controller_id: string;
  generation: number;
  nodes: {
    id: string;
    address: string;
    gateway_enabled: boolean;
    version?: string;
  }[];
  gateway: {
    enabled: boolean;
    desired_generation: number;
    applied_generation?: number;
    endpoint_errors: Record<string, string>;
  };
  recovery: { awaiting_adoption: number; conflicting_slots: number };
}

const fragment = new URLSearchParams(location.hash.slice(1));
const incoming = fragment.get("session");
if (incoming) {
  sessionStorage.setItem("swarmlite-session", incoming);
  history.replaceState(null, "", location.pathname + location.search);
}
const session = incoming || sessionStorage.getItem("swarmlite-session") || "";
export const hasSession = Boolean(session);

// Opening a new CLI session in an existing tab can be a same-document navigation.
// Reload so the new fragment is consumed and all old requests are disconnected.
window.addEventListener("hashchange", () => {
  if (new URLSearchParams(location.hash.slice(1)).get("session")) {
    location.reload();
  }
});

export async function request(
  path: string,
  signal?: AbortSignal,
): Promise<Response> {
  const response = await fetch(`/api${path}`, {
    headers: { Authorization: `Bearer ${session}` },
    signal,
    cache: "no-store",
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.error || `Request failed (${response.status})`);
  }
  return response;
}
export async function get<T>(path: string, signal?: AbortSignal): Promise<T> {
  return (await request(path, signal)).json();
}
export const timestamp = (value?: number) =>
  value ? new Date(value).toLocaleString() : "—";

export interface JobExecution {
  id: string;
  service_id: string;
  node_id: string;
  desired: string;
  observed: string;
  scheduled_at_unix_ms: number;
  start_deadline_unix_ms: number;
  started_at_unix_ms?: number;
  finished_at_unix_ms?: number;
  exit_code?: number;
  stop_reason?: string;
  error?: string;
}
export interface JobInfo {
  id: string;
  policy: NonNullable<Service["job"]>;
  next_at_unix_ms?: number;
}
export const jobFinished = (execution: JobExecution) =>
  ["succeeded", "failed", "cancelled", "timed_out"].includes(
    execution.observed,
  );

export function jobDuration(execution: JobExecution, now = Date.now()): string {
  const start = execution.started_at_unix_ms;
  if (start == null) return "—";
  const end = jobFinished(execution)
    ? execution.finished_at_unix_ms
    : execution.observed === "running"
      ? now
      : undefined;
  if (end == null || end < start) return "Unavailable";
  const seconds = Math.floor((end - start) / 1000);
  const text =
    seconds < 60
      ? `${seconds}s`
      : seconds < 3600
        ? `${Math.floor(seconds / 60)}m ${seconds % 60}s`
        : `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
  return jobFinished(execution) ? text : `${text} elapsed`;
}

export class OperationError extends Error {
  uncertain: boolean;
  constructor(message: string, uncertain = false) {
    super(message);
    this.uncertain = uncertain;
  }
}
export async function post<T>(path: string, body: unknown): Promise<T> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 20000);
  try {
    const response = await fetch(`/api${path}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${session}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
      signal: controller.signal,
      cache: "no-store",
    });
    const value = await response.json();
    if (!response.ok)
      throw new OperationError(
        value.error || `Request failed (${response.status})`,
        response.status >= 500,
      );
    return value;
  } catch (error) {
    if (error instanceof OperationError) throw error;
    throw new OperationError(
      "The response was lost. Check operation history before submitting again.",
      true,
    );
  } finally {
    clearTimeout(timer);
  }
}
