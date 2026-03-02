"use client";

import { useRef, useState } from "react";

export type HelpInterpretability = {
  lookFor: string;
  good: string;
  distribution?: string;
};

type HelpLabelProps = {
  label: string;
  plain: string;
  math: string;
  interpret?: HelpInterpretability;
};

export default function HelpLabel({ label, plain, math, interpret }: HelpLabelProps) {
  const triggerRef = useRef<HTMLSpanElement | null>(null);
  const [open, setOpen] = useState(false);
  const [bubbleStyle, setBubbleStyle] = useState<{ left: number; top: number; width: number; placeAbove: boolean }>({
    left: 12,
    top: 12,
    width: 280,
    placeAbove: false,
  });

  const placeBubble = () => {
    const el = triggerRef.current;
    if (!el || typeof window === "undefined") return;
    const rect = el.getBoundingClientRect();
    const margin = 12;
    const width = Math.min(300, window.innerWidth - margin * 2);
    let left = rect.left + rect.width * 0.5 - width * 0.5;
    left = Math.max(margin, Math.min(left, window.innerWidth - width - margin));
    const estimatedHeight = interpret ? 240 : 112;
    const spaceBelow = window.innerHeight - rect.bottom - margin;
    const placeAbove = spaceBelow < estimatedHeight && rect.top > estimatedHeight + margin;
    const top = placeAbove ? rect.top - 8 : rect.bottom + 8;
    setBubbleStyle({ left, top, width, placeAbove });
    setOpen(true);
  };

  return (
    <span
      ref={triggerRef}
      className="col-help-trigger"
      tabIndex={0}
      aria-label={`Explain ${label}`}
      onMouseEnter={placeBubble}
      onFocus={placeBubble}
      onMouseLeave={() => setOpen(false)}
      onBlur={() => setOpen(false)}
    >
      {label}
      {open && (
        <span
          className={`col-help-bubble ${bubbleStyle.placeAbove ? "above" : ""}`}
          style={{ left: bubbleStyle.left, top: bubbleStyle.top, width: bubbleStyle.width }}
        >
          <span className="col-help-bubble-plain">{plain}</span>
          {interpret && (
            <span className="col-help-bubble-interpret">
              <span className="col-help-bubble-interpret-line">
                <strong>Look for:</strong> {interpret.lookFor}
              </span>
              <span className="col-help-bubble-interpret-line">
                <strong>Good:</strong> {interpret.good}
              </span>
              {interpret.distribution && (
                <span className="col-help-bubble-interpret-line">
                  <strong>Distribution:</strong> {interpret.distribution}
                </span>
              )}
            </span>
          )}
          <span className="col-help-bubble-math">Math: {math}</span>
        </span>
      )}
    </span>
  );
}
