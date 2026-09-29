import * as React from "react";
import { Select as SelectPrimitive } from "radix-ui";
import { Check, ChevronDown, ChevronUp } from "lucide-react";
import { cn } from "@/lib/utils";

// A shared shadcn-style select. Option children keep resource views concise.
export function Select({
  value,
  onValueChange,
  children,
  className,
  id,
  required,
  disabled,
  "aria-label": label,
}: {
  value: string | number;
  onValueChange: (value: string) => void;
  children: React.ReactNode;
  className?: string;
  id?: string;
  required?: boolean;
  disabled?: boolean;
  "aria-label"?: string;
}) {
  const empty = "__swarmlite_empty_selection__";
  const options = React.Children.toArray(children)
    .filter(React.isValidElement)
    .map((child) => {
      const props = (
        child as React.ReactElement<{
          value?: string | number;
          children: React.ReactNode;
          disabled?: boolean;
        }>
      ).props;
      return {
        value: String(props.value ?? props.children),
        label: props.children,
        disabled: props.disabled,
      };
    });
  return (
    <SelectPrimitive.Root
      value={String(value) || empty}
      onValueChange={(next) => onValueChange(next === empty ? "" : next)}
      required={required}
      disabled={disabled}
    >
      <SelectPrimitive.Trigger
        id={id}
        aria-label={label}
        className={cn("select-trigger", className)}
      >
        <SelectPrimitive.Value />
        <SelectPrimitive.Icon>
          <ChevronDown size={15} />
        </SelectPrimitive.Icon>
      </SelectPrimitive.Trigger>
      <SelectPrimitive.Portal>
        <SelectPrimitive.Content
          position="popper"
          sideOffset={5}
          collisionPadding={12}
          className="select-content"
        >
          <SelectPrimitive.ScrollUpButton className="select-scroll">
            <ChevronUp size={14} />
          </SelectPrimitive.ScrollUpButton>
          <SelectPrimitive.Viewport className="select-viewport">
            {options.map((option) => (
              <SelectPrimitive.Item
                key={option.value}
                value={option.value || empty}
                disabled={option.disabled}
                className="select-item"
              >
                <SelectPrimitive.ItemText>
                  {option.label}
                </SelectPrimitive.ItemText>
                <SelectPrimitive.ItemIndicator className="select-indicator">
                  <Check size={14} />
                </SelectPrimitive.ItemIndicator>
              </SelectPrimitive.Item>
            ))}
          </SelectPrimitive.Viewport>
          <SelectPrimitive.ScrollDownButton className="select-scroll">
            <ChevronDown size={14} />
          </SelectPrimitive.ScrollDownButton>
        </SelectPrimitive.Content>
      </SelectPrimitive.Portal>
    </SelectPrimitive.Root>
  );
}
