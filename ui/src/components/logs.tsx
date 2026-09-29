import { Select } from "@/components/ui/select";
import { useEffect, useRef, useState } from "react";
import { Pause, Play, RotateCw, Terminal, Download } from "lucide-react";
import { Button } from "@/components/ui/button";
import { request } from "@/lib/api";

interface Stream {
  stream_id: number;
  task_id: string;
  node_id: string;
  stack: string;
  service: string;
  slot: number;
}
interface LogEvent {
  type: string;
  streams?: Stream[];
  stream_id?: number;
  payload?: string;
  message?: string;
  channel?: number;
}
interface Line {
  id: number;
  label: string;
  text: string;
  error: boolean;
}

// Bound both count and text size, including queued output in throttled background tabs.
function boundedLines(lines: Line[]): Line[] {
  let size = 0;
  let start = lines.length;
  while (start > 0 && lines.length - start < 2000) {
    const next = lines[start - 1].text.length;
    if (size + next > 2 * 1024 * 1024) break;
    size += next;
    start--;
  }
  return lines.slice(start);
}

export function Logs({ target }: { target: string }) {
  const [lines, setLines] = useState<Line[]>([]);
  const [follow, setFollow] = useState(true);
  const [raw, setRaw] = useState(false);
  const [tail, setTail] = useState(200);
  const [revision, setRevision] = useState(0);
  const [status, setStatus] = useState("Connecting");
  const [error, setError] = useState("");
  const viewport = useRef<HTMLDivElement>(null);
  const stickToBottom = useRef(true);
  useEffect(() => {
    const controller = new AbortController();
    let active = true;
    const decoders = new Map<number, TextDecoder>();
    const streams = new Map<number, Stream>();
    let sequence = 0;
    let pendingLines: Line[] = [];
    const flush = () => {
      if (active && pendingLines.length) {
        const batch = pendingLines;
        pendingLines = [];
        setLines((previous) => boundedLines([...previous, ...batch]));
      }
    };
    const timer = window.setInterval(flush, 100);
    // A changed target or follow mode starts a new external log session.
    // eslint-disable-next-line react/set-state-in-effect
    setLines([]);
    setError("");
    setStatus("Connecting");
    async function start() {
      try {
        const query = new URLSearchParams({
          target,
          tail: String(tail),
          follow: String(follow),
        });
        const response = await request(`/logs?${query}`, controller.signal);
        if (!active) return;
        setStatus(follow ? "Live" : "Reading");
        const reader = response.body!.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        const consume = (event: LogEvent) => {
          if (event.type === "streams") {
            if (raw && event.streams?.length !== 1)
              throw new Error(
                "Raw output requires exactly one task. Select a task ID.",
              );
            event.streams?.forEach((stream) =>
              streams.set(stream.stream_id, stream),
            );
            return;
          }
          if (event.type === "failure") throw new Error(event.message);
          const id = event.stream_id!;
          const stream = streams.get(id);
          if (!decoders.has(id)) decoders.set(id, new TextDecoder());
          const text = decoders.get(id)!.decode(
            Uint8Array.from(atob(event.payload || ""), (c) => c.charCodeAt(0)),
            { stream: event.type !== "end" },
          );
          if (text)
            pendingLines.push({
              id: sequence++,
              label: stream
                ? `${stream.service}.${stream.slot + 1} @ ${stream.node_id}`
                : String(id),
              text,
              error: event.type === "error" || event.channel === 2,
            });
          pendingLines = boundedLines(pendingLines);
        };
        try {
          for (;;) {
            const { done, value } = await reader.read();
            buffer += decoder.decode(value, { stream: !done });
            const events = buffer.split("\n");
            buffer = events.pop()!;
            for (const event of events) if (event) consume(JSON.parse(event));
            if (done) {
              if (buffer.trim()) consume(JSON.parse(buffer));
              break;
            }
          }
        } finally {
          await reader.cancel().catch(() => {});
          reader.releaseLock();
        }
        if (active) {
          flush();
          setStatus("Finished");
        }
      } catch (cause) {
        if (active) {
          flush();
          setError(String(cause instanceof Error ? cause.message : cause));
          setStatus("Disconnected");
        }
      }
    }
    void start();
    return () => {
      active = false;
      clearInterval(timer);
      controller.abort();
    };
  }, [target, follow, tail, raw, revision]);
  useEffect(() => {
    if (stickToBottom.current && viewport.current)
      viewport.current.scrollTop = viewport.current.scrollHeight;
  }, [lines]);
  const download = () => {
    const url = URL.createObjectURL(
      new Blob(
        lines.map((line) => (raw ? line.text : `[${line.label}] ${line.text}`)),
        { type: "text/plain" },
      ),
    );
    const link = document.createElement("a");
    link.href = url;
    link.download = `${target}.log`;
    link.click();
    URL.revokeObjectURL(url);
  };
  return (
    <section className="log-panel">
      <div className="log-toolbar">
        <span className="flex items-center gap-2">
          <Terminal size={15} />
          <strong>Task logs</strong>
          <span className={status === "Live" ? "live-label" : "muted"}>
            {status}
          </span>
        </span>
        <div className="flex items-center gap-2">
          <Select
            aria-label="Log tail lines"
            value={tail}
            onValueChange={(value) => setTail(Number(value))}
          >
            <option value={0}>Live only (no tail)</option>
            <option value={100}>100 lines</option>
            <option value={200}>200 lines</option>
            <option value={1000}>1,000 lines</option>
            <option value={10000}>10,000 lines</option>
          </Select>
          <label className="raw-toggle">
            <input
              type="checkbox"
              checked={raw}
              onChange={(e) => setRaw(e.target.checked)}
            />{" "}
            Raw task output
          </label>
          <Button variant="ghost" size="sm" onClick={() => setFollow(!follow)}>
            {follow ? <Pause /> : <Play />}
            {follow ? "Stop following" : "Follow"}
          </Button>
          <Button
            variant="ghost"
            size="icon"
            aria-label="Reconnect logs"
            onClick={() => setRevision(revision + 1)}
          >
            <RotateCw />
          </Button>
          <Button
            variant="ghost"
            size="icon"
            aria-label="Download visible logs"
            onClick={download}
          >
            <Download />
          </Button>
        </div>
      </div>
      {error && (
        <div role="alert" className="log-error">
          {error}
        </div>
      )}
      <div
        className="log-output"
        ref={viewport}
        onScroll={() => {
          const el = viewport.current!;
          stickToBottom.current =
            el.scrollHeight - el.scrollTop - el.clientHeight < 60;
        }}
      >
        {!lines.length && (
          <p className="muted">
            {error ? "No log output received." : "Waiting for task output…"}
          </p>
        )}
        {lines.map((line) => (
          <div
            key={line.id}
            className={`${line.error ? "log-line stderr" : "log-line"}${raw ? " raw-log-line" : ""}`}
          >
            {!raw && <span className="log-source">{line.label}</span>}
            <pre>{line.text}</pre>
          </div>
        ))}
      </div>
      <div className="log-footer">
        Showing up to 2,000 recent chunks · Output is streamed from the selected
        tasks
      </div>
    </section>
  );
}
