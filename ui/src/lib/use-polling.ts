import { useEffect, useState } from "react";
import { hasSession } from "@/lib/api";

export function usePolling<T>(
  load: (signal: AbortSignal) => Promise<T>,
  key: string,
  refreshToken = 0,
) {
  const [data, setData] = useState<T>();
  const [error, setError] = useState("");
  const [updated, setUpdated] = useState<number>();
  const [revision, setRevision] = useState(0);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function poll() {
      try {
        const result = await load(controller.signal);
        if (!controller.signal.aborted) {
          setData(result);
          setError("");
          setUpdated(Date.now());
        }
      } catch (cause) {
        if (!controller.signal.aborted)
          setError(cause instanceof Error ? cause.message : String(cause));
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false);
          timer = setTimeout(poll, 5000);
        }
      }
    }
    if (hasSession) void poll();
    else {
      setError(
        "Open the full URL printed by swarmlite ui to connect this browser.",
      );
      setLoading(false);
    }
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
    // The caller supplies a stable resource key; polling intentionally keeps its last successful snapshot.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, revision, refreshToken]);
  return {
    data,
    error,
    updated,
    loading,
    refresh: () => {
      setLoading(true);
      setRevision((value) => value + 1);
    },
  };
}
