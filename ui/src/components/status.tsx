import type { ReactNode } from "react";
import { Boxes, CircleAlert } from "lucide-react";
import { Badge } from "@/components/ui/badge";

export function Status({ value }: { value: string }) {
  const tone = ["healthy", "running", "ready", "succeeded"].includes(value)
    ? "good"
    : ["failed", "blocked", "error"].includes(value)
      ? "bad"
      : ["stalled", "pending", "reconciling", "starting", "updating"].includes(
            value,
          )
        ? "warning"
        : "neutral";
  return (
    <Badge variant="outline" className={`status status-${tone}`}>
      <span />
      {value.replaceAll("_", " ")}
    </Badge>
  );
}
export function Empty({ children }: { children: ReactNode }) {
  return (
    <div className="empty">
      <Boxes size={28} />
      <p>{children}</p>
    </div>
  );
}
export function ErrorNotice({
  error,
  stale,
}: {
  error: string;
  stale?: boolean;
}) {
  return (
    error && (
      <div className="error-notice" role="alert">
        <CircleAlert size={18} />
        <div>
          {error}
          {stale && (
            <p>
              Showing the last successful snapshot. Workloads may still be
              serving.
            </p>
          )}
        </div>
      </div>
    )
  );
}
