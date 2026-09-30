import { useState } from "react";
import { Popover } from "radix-ui";
import { CalendarDays, ChevronDown, Check, Clock3 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

import {
  type MonitoringRange,
  rangePresets,
  localInput,
  validateRange,
  rangeLabel,
} from "@/lib/monitoring-range";

export function MonitoringRangePicker({
  value,
  onChange,
}: {
  value: MonitoringRange;
  onChange: (range: MonitoringRange) => void;
}) {
  const [open, setOpen] = useState(false);
  const [from, setFrom] = useState("");
  const [to, setTo] = useState("");
  const [error, setError] = useState("");
  const timezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
  const changeOpen = (next: boolean) => {
    if (next) {
      const end = value.to ?? Date.now();
      setFrom(localInput(value.from ?? end - (value.seconds ?? 300) * 1000));
      setTo(localInput(end));
      setError("");
    }
    setOpen(next);
  };
  return (
    <div className="monitoring-range-controls">
      <div className="monitoring-shortcuts" aria-label="Quick time ranges">
        {rangePresets
          .filter((p) => [300, 3600, 86400, 604800].includes(p[0]))
          .map(([seconds, label, short]) => (
            <button
              key={seconds}
              aria-label={label}
              aria-pressed={value.seconds === seconds}
              onClick={() => onChange({ seconds })}
            >
              {short}
            </button>
          ))}
      </div>
      <Popover.Root open={open} onOpenChange={changeOpen}>
        <Popover.Trigger asChild>
          <Button
            variant="outline"
            size="sm"
            className="monitoring-range-trigger"
            aria-label="Choose monitoring time range"
          >
            <CalendarDays size={14} />
            {rangeLabel(value)}
            <ChevronDown size={13} />
          </Button>
        </Popover.Trigger>
        <Popover.Portal>
          <Popover.Content
            className="monitoring-range-popover"
            sideOffset={8}
            align="end"
            collisionPadding={16}
          >
            <div className="monitoring-presets">
              <h3>
                <Clock3 size={14} /> Quick ranges
              </h3>
              {rangePresets.map(([seconds, label]) => (
                <button
                  key={seconds}
                  onClick={() => {
                    onChange({ seconds });
                    setOpen(false);
                  }}
                >
                  {label}
                  {value.seconds === seconds && <Check size={14} />}
                </button>
              ))}
            </div>
            <form
              className="monitoring-custom-range"
              onSubmit={(e) => {
                e.preventDefault();
                const fields = new FormData(e.currentTarget);
                const from = String(fields.get("from") ?? "");
                const to = String(fields.get("to") ?? "");
                const error = validateRange(from, to, Date.now());
                setError(error);
                if (!error) {
                  onChange({
                    from: new Date(from).getTime(),
                    to: new Date(to).getTime(),
                  });
                  setOpen(false);
                }
              }}
            >
              <h3>Custom range</h3>
              <p>Select any interval within the last 365 days.</p>
              <label htmlFor="monitoring-from">Start</label>
              <Input
                id="monitoring-from"
                type="datetime-local"
                name="from"
                defaultValue={from}
                required
              />
              <label htmlFor="monitoring-to">End</label>
              <Input
                id="monitoring-to"
                type="datetime-local"
                name="to"
                defaultValue={to}
                required
              />
              <span className="monitoring-timezone">{timezone}</span>
              {error && (
                <p className="monitoring-range-error" role="alert">
                  {error}
                </p>
              )}
              <Button type="submit" size="sm">
                Apply range
              </Button>
            </form>
          </Popover.Content>
        </Popover.Portal>
      </Popover.Root>
    </div>
  );
}
