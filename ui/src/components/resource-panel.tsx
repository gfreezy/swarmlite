import { useLayoutEffect, useRef, type ReactNode } from "react";
import { Dialog } from "radix-ui";
import { ChevronLeft, ChevronRight, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Select } from "@/components/ui/select";

/** Nested records open above their parent, preserving the parent list position. */
export function ResourcePanel({
  title,
  items,
  selected,
  onSelect,
  onClose,
  children,
}: {
  title: string;
  items: { id: string; label: string }[];
  selected: string;
  onSelect: (id: string) => void;
  onClose: () => void;
  children: ReactNode;
}) {
  const index = items.findIndex((item) => item.id === selected);
  const opener = useRef<HTMLElement | null>(null);
  const body = useRef<HTMLDivElement>(null);
  useLayoutEffect(() => {
    if (body.current) body.current.scrollTop = 0;
  }, [selected]);
  return (
    <Dialog.Root
      open
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
    >
      <Dialog.Portal>
        <Dialog.Overlay className="record-overlay" />
        <Dialog.Content
          className="record-panel"
          onOpenAutoFocus={() => {
            opener.current = document.activeElement as HTMLElement;
          }}
          onCloseAutoFocus={(event) => {
            event.preventDefault();
            opener.current?.focus({ preventScroll: true });
          }}
        >
          <header className="record-panel-header">
            <div>
              <Dialog.Title>{title}</Dialog.Title>
              <Dialog.Description>
                Switch records without leaving the current list.
              </Dialog.Description>
            </div>
            <Dialog.Close asChild>
              <Button variant="ghost" size="icon-sm" aria-label="Close record">
                <X />
              </Button>
            </Dialog.Close>
          </header>
          <div className="record-navigation">
            <Select
              aria-label={`Select ${title.toLowerCase()}`}
              value={selected}
              onValueChange={onSelect}
            >
              {items.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.label}
                </option>
              ))}
            </Select>
            <span>{index < 0 ? "—" : `${index + 1} / ${items.length}`}</span>
            <Button
              variant="outline"
              size="icon-sm"
              aria-label="Previous record"
              disabled={index <= 0}
              onClick={() => onSelect(items[index - 1].id)}
            >
              <ChevronLeft />
            </Button>
            <Button
              variant="outline"
              size="icon-sm"
              aria-label="Next record"
              disabled={index < 0 || index >= items.length - 1}
              onClick={() => onSelect(items[index + 1].id)}
            >
              <ChevronRight />
            </Button>
          </div>
          <div className="record-panel-body" ref={body}>
            {index < 0 ? <p>This record is no longer available.</p> : children}
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
