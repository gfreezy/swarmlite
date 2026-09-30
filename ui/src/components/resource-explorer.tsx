import { useLayoutEffect, useRef, useState, type ReactNode } from "react";
import { ArrowLeft, ChevronLeft, ChevronRight, Search } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select } from "@/components/ui/select";

interface ResourceItem {
  id: string;
  title: string;
  description?: string;
  meta?: ReactNode;
}

/** A persistent resource navigator beside an independently scrolling inspector. */
export function ResourceExplorer({
  title,
  backLabel,
  items,
  selected,
  onSelect,
  onClose,
  children,
}: {
  title: string;
  backLabel?: string;
  items: ResourceItem[];
  selected: string;
  onSelect: (id: string) => void;
  onClose: () => void;
  children: ReactNode;
}) {
  const [query, setQuery] = useState("");
  const root = useRef<HTMLElement>(null);
  const heading = useRef<HTMLHeadingElement>(null);
  const detail = useRef<HTMLDivElement>(null);
  const activeButton = useRef<HTMLButtonElement>(null);
  const visible = items.filter((item) =>
    `${item.title} ${item.description ?? ""} ${item.id}`
      .toLowerCase()
      .includes(query.toLowerCase()),
  );
  const index = visible.findIndex((item) => item.id === selected);
  const current = items.find((item) => item.id === selected);
  const origin = useRef<{ element: HTMLElement | null; top: number } | null>(
    null,
  );
  useLayoutEffect(() => {
    origin.current ??= {
      element: document.activeElement as HTMLElement | null,
      top: window.scrollY,
    };
    root.current?.scrollIntoView?.({ block: "start" });
    heading.current?.focus({ preventScroll: true });
  }, []);
  const close = () => {
    onClose();
    requestAnimationFrame(() => {
      const previous = origin.current;
      if (previous?.element?.isConnected) {
        const parentExplorer = previous.element.closest(".resource-explorer");
        if (parentExplorer) parentExplorer.scrollIntoView({ block: "start" });
        else {
          window.scrollTo({ top: previous.top, behavior: "instant" });
          previous.element.scrollIntoView?.({ block: "nearest" });
        }
        previous.element.focus({ preventScroll: true });
      }
    });
  };
  useLayoutEffect(() => {
    if (detail.current) detail.current.scrollTop = 0;
    activeButton.current?.scrollIntoView?.({ block: "nearest" });
  }, [selected]);
  return (
    <section
      ref={root}
      className="resource-explorer"
      aria-label={`${title} explorer`}
    >
      <header className="explorer-header">
        <Button variant="ghost" size="sm" onClick={close}>
          <ArrowLeft /> {backLabel ?? `All ${title.toLowerCase()}`}
        </Button>
        <div className="explorer-heading">
          <small>{title}</small>
          <h2 ref={heading} tabIndex={-1} title={current?.title ?? selected}>
            {current?.title ?? selected}
          </h2>
        </div>
        <div className="explorer-pagination">
          <span>
            {index >= 0 ? `${index + 1} / ${visible.length}` : "Outside filter"}
          </span>
          <Button
            variant="outline"
            size="icon-sm"
            aria-label="Previous item"
            disabled={index <= 0}
            onClick={() => onSelect(visible[index - 1].id)}
          >
            <ChevronLeft />
          </Button>
          <Button
            variant="outline"
            size="icon-sm"
            aria-label="Next item"
            disabled={index < 0 || index >= visible.length - 1}
            onClick={() => onSelect(visible[index + 1].id)}
          >
            <ChevronRight />
          </Button>
        </div>
      </header>
      <div className="explorer-body">
        <aside className="explorer-navigator" aria-label={`${title} navigator`}>
          <div className="explorer-search">
            <Search size={15} />
            <Input
              aria-label={`Find ${title.toLowerCase()}`}
              placeholder={`Find ${title.toLowerCase()}…`}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
            />
          </div>
          <nav
            className="explorer-items"
            aria-label={`Select ${title.toLowerCase()}`}
          >
            {visible.map((item) => (
              <button
                key={item.id}
                ref={item.id === selected ? activeButton : undefined}
                aria-current={item.id === selected ? "true" : undefined}
                className="explorer-item"
                onClick={() => onSelect(item.id)}
              >
                <strong>{item.title}</strong>
                <small>{item.description}</small>
                {item.meta && (
                  <span className="explorer-item-meta">{item.meta}</span>
                )}
              </button>
            ))}
            {!visible.length && (
              <p className="explorer-empty">
                No matching {title.toLowerCase()}.
              </p>
            )}
          </nav>
        </aside>
        <div className="explorer-mobile-picker">
          <Select
            aria-label={`Switch ${title.toLowerCase()}`}
            value={selected}
            onValueChange={onSelect}
          >
            {items.map((item) => (
              <option key={item.id} value={item.id}>
                {item.title}
              </option>
            ))}
          </Select>
        </div>
        <div
          ref={detail}
          className="explorer-detail"
          role="region"
          aria-label={`${title} detail`}
          tabIndex={0}
        >
          {current ? (
            children
          ) : (
            <p className="explorer-empty">
              This item is no longer in the current list. Select another item or
              return to the overview.
            </p>
          )}
        </div>
      </div>
    </section>
  );
}
