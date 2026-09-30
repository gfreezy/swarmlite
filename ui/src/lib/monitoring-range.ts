export type MonitoringRange =
  | { seconds: number; from?: undefined; to?: undefined }
  | { from: number; to: number; seconds?: undefined };
export const rangePresets = [
  [300, "Last 5 minutes", "5m"],
  [900, "Last 15 minutes", "15m"],
  [3600, "Last hour", "1h"],
  [21600, "Last 6 hours", "6h"],
  [86400, "Last 24 hours", "24h"],
  [604800, "Last 7 days", "7d"],
  [2592000, "Last 30 days", "30d"],
  [31536000, "Last 365 days", "365d"],
] as const;
export function localInput(time: number) {
  const d = new Date(time);
  return new Date(time - d.getTimezoneOffset() * 60000)
    .toISOString()
    .slice(0, 16);
}
export function validateRange(from: string, to: string, now: number): string {
  const start = new Date(from).getTime(),
    end = new Date(to).getTime();
  if (!Number.isFinite(start) || !Number.isFinite(end))
    return "Choose a start and end date.";
  if (start >= end) return "End time must be after start time.";
  if (end > now) return "End time cannot be in the future.";
  if (start < now - 365 * 86400000)
    return "History is available for the last 365 days.";
  return "";
}
export function rangeLabel(range: MonitoringRange) {
  return range.from !== undefined
    ? "Custom range"
    : rangePresets.find((p) => p[0] === range.seconds)?.[1] || "Time range";
}
