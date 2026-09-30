import { useEffect, useId, useRef, useState } from "react";
import {
  ArrowUpRight,
  Boxes,
  Filter,
  Globe2,
  Maximize2,
  Minus,
  Plus,
  Server,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { type Service } from "@/lib/api";

import {
  buildRouteGraph,
  WIDTH,
  HEIGHT,
  STEP,
  PAD,
  stages,
  type RouteRow,
} from "@/lib/route-graph";
export type { RouteRow } from "@/lib/route-graph";

export function RouteGraph({
  routes,
  services,
  onInspect,
  onService,
}: {
  routes: RouteRow[];
  services: Service[];
  onInspect: (id: string) => void;
  onService: (service: Service) => void;
}) {
  const graph = buildRouteGraph(routes);
  const [selectedNode, setSelectedNode] = useState<string>();
  const selected =
    graph.nodes.find((node) => node.id === selectedNode)?.routes ?? [];
  const marker = useId().replace(/:/g, "");
  const viewport = useRef<HTMLDivElement>(null);
  const [requestedZoom, setZoom] = useState<number>();
  const [canvasWidth, setCanvasWidth] = useState(0);
  useEffect(() => {
    const el = viewport.current;
    if (!el || typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver(([entry]) =>
      setCanvasWidth(entry.contentRect.width),
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, []);
  const zoom =
    requestedZoom ??
    (canvasWidth >= 500 ? Math.min(1, canvasWidth / graph.width) : 1);
  const active = (ids: string[]) =>
    !selected.length || ids.some((id) => selected.includes(id));
  const icons = [Globe2, Filter, Boxes, Server];
  return (
    <section
      className="route-graph"
      aria-label="Directed routing graph"
      onKeyDown={(event) => {
        if (event.key === "Escape" && selected.length) {
          event.preventDefault();
          setSelectedNode(undefined);
        }
      }}
    >
      <div className="graph-toolbar">
        <div className="graph-legend">
          <span>
            <i />
            Published route
          </span>
          <span>
            <i className="missing" />
            Missing target
          </span>
        </div>
        <div className="action-group">
          {selected.length > 0 && (
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setSelectedNode(undefined)}
            >
              Clear selection
            </Button>
          )}
          <Button
            variant="ghost"
            size="icon-sm"
            aria-label="Zoom out routing graph"
            disabled={zoom <= 0.3}
            onClick={() => setZoom(Math.max(0.3, zoom - 0.1))}
          >
            <Minus />
          </Button>
          <span className="graph-zoom">{Math.round(zoom * 100)}%</span>
          <Button
            variant="ghost"
            size="icon-sm"
            aria-label="Zoom in routing graph"
            disabled={zoom >= 1.4}
            onClick={() => setZoom(Math.min(1.4, zoom + 0.1))}
          >
            <Plus />
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={() =>
              setZoom(
                Math.min(
                  1,
                  Math.max(
                    0.3,
                    (viewport.current?.clientWidth || graph.width) /
                      graph.width,
                  ),
                ),
              )
            }
          >
            <Maximize2 />
            Fit
          </Button>
        </div>
      </div>
      <div
        className="graph-viewport"
        ref={viewport}
        onClick={(event) => {
          if (!(event.target as Element).closest(".graph-node"))
            setSelectedNode(undefined);
        }}
        tabIndex={0}
        aria-label="Routing diagram canvas; scroll to explore"
      >
        <div style={{ width: graph.width * zoom, height: graph.height * zoom }}>
          <div
            className="graph-canvas"
            style={{
              width: graph.width,
              height: graph.height,
              transform: `scale(${zoom})`,
            }}
          >
            {stages.map((name, i) => (
              <div
                key={name}
                className="graph-stage"
                style={{ left: PAD + i * STEP, width: WIDTH }}
              >
                <span>0{i + 1}</span>
                {name}
              </div>
            ))}
            <svg
              className="graph-edges"
              width={graph.width}
              height={graph.height}
              aria-hidden="true"
            >
              <defs>
                <marker
                  id={marker}
                  viewBox="0 0 10 10"
                  refX="9"
                  refY="5"
                  markerWidth="6"
                  markerHeight="6"
                  orient="auto-start-reverse"
                >
                  <path d="M 0 0 L 10 5 L 0 10 z" fill="#709681" />
                </marker>
                <marker
                  id={`${marker}-missing`}
                  viewBox="0 0 10 10"
                  refX="9"
                  refY="5"
                  markerWidth="6"
                  markerHeight="6"
                  orient="auto-start-reverse"
                >
                  <path d="M 0 0 L 10 5 L 0 10 z" fill="#b58a42" />
                </marker>
              </defs>
              {graph.edges.map((e) => {
                const from = graph.nodes.find((n) => n.id === e.from)!;
                const to = graph.nodes.find((n) => n.id === e.to)!;
                const x = from.x + WIDTH,
                  y = from.y + HEIGHT / 2,
                  tx = to.x - 3,
                  ty = to.y + HEIGHT / 2;
                return (
                  <path
                    key={e.id}
                    data-from={e.from}
                    data-to={e.to}
                    className={`graph-edge${e.missing ? " missing" : ""}${active(e.routes) ? "" : " dimmed"}`}
                    d={`M ${x} ${y} C ${(x + tx) / 2} ${y}, ${(x + tx) / 2} ${ty}, ${tx} ${ty}`}
                    markerEnd={`url(#${marker}${e.missing ? "-missing" : ""})`}
                  />
                );
              })}
            </svg>
            {graph.nodes.map((n) => {
              const Icon = icons[n.column];
              const service = services.find((s) => s.id === n.service);
              return (
                <div
                  key={n.id}
                  className={`graph-node${n.tone ? ` ${n.tone}` : ""}${active(n.routes) ? "" : " dimmed"}${selected.length && active(n.routes) ? " highlighted" : ""}${selectedNode === n.id ? " selected" : ""}`}
                  style={{ left: n.x, top: n.y, width: WIDTH, height: HEIGHT }}
                >
                  <button
                    className="graph-node-select"
                    aria-label={`Trace ${stages[n.column].toLowerCase()}: ${n.title}`}
                    aria-pressed={selectedNode === n.id}
                    onClick={() =>
                      setSelectedNode(selectedNode === n.id ? undefined : n.id)
                    }
                  >
                    <span className="graph-node-type">
                      <Icon size={14} />
                      {n.column === 0 ? n.subtitle : stages[n.column]}
                    </span>
                    <strong title={n.title}>{n.title}</strong>
                    <span title={n.column === 0 ? n.meta : n.subtitle}>
                      {n.column === 0 ? n.meta : n.subtitle}
                    </span>
                    {n.column !== 0 && <small>{n.meta}</small>}
                  </button>
                  {service && (
                    <button
                      className="graph-inspect"
                      aria-label={service.id}
                      title={`Inspect ${service.id}`}
                      onClick={() => onService(service)}
                    >
                      <ArrowUpRight size={14} />
                    </button>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      </div>
      {selected.length > 0 && (
        <div className="graph-selection" aria-label="Selected route actions">
          <span>
            {selected.length} matching{" "}
            {selected.length === 1 ? "route" : "routes"}
          </span>
          <div>
            {routes
              .filter((route) => selected.includes(route.id))
              .map((route) => (
                <Button
                  key={route.id}
                  variant="outline"
                  size="sm"
                  onClick={() => onInspect(route.id)}
                >
                  {route.matches.map((match) => match.path).join(", ") ||
                    "All paths"}{" "}
                  → {route.service_id || route.backend.host}
                  <ArrowUpRight size={14} />
                </Button>
              ))}
          </div>
        </div>
      )}
      <div className="graph-hint">
        Click a node to select it; click it again, click empty space, or press
        Esc to clear. Use the selected route actions to inspect details.
      </div>
    </section>
  );
}
