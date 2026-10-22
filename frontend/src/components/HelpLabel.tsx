"use client";

import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";

export type HelpInterpretability = {
  lookFor: string;
  good: string;
  distribution?: string;
};

type HelpLabelProps = {
  label: string;
  plain: string;
  math?: string;
  interpret?: HelpInterpretability;
  iconOnly?: boolean;
};

export default function HelpLabel({ label, plain, math, interpret, iconOnly = false }: HelpLabelProps) {
  const triggerRef = useRef<HTMLSpanElement | null>(null);
  const bubbleRef = useRef<HTMLSpanElement | null>(null);
  const [open, setOpen] = useState(false);
  const [isMounted, setIsMounted] = useState(false);
  const [bubbleStyle, setBubbleStyle] = useState<{ left: number; top: number; width: number }>({
    left: 12,
    top: 12,
    width: 260,
  });

  const placeBubble = useCallback(() => {
    const el = triggerRef.current;
    if (!el || typeof window === "undefined") return;

    const rect = el.getBoundingClientRect();
    const margin = 12;
    const gap = 4;
    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;

    const measuredWidth = bubbleRef.current?.offsetWidth ?? 280;
    const measuredHeight = bubbleRef.current?.offsetHeight ?? 112;
    const width = Math.min(measuredWidth, viewportWidth - margin * 2);

    let left = rect.left + rect.width * 0.5 - width * 0.5;
    left = Math.max(
      margin,
      Math.min(left, viewportWidth - width - margin),
    );

    const preferredBelowTop = rect.bottom + gap;
    const fitsBelow = preferredBelowTop + measuredHeight <= viewportHeight - margin;
    let top = preferredBelowTop;
    if (!fitsBelow) {
      const preferredAboveTop = rect.top - measuredHeight - gap;
      top = preferredAboveTop >= margin
        ? preferredAboveTop
        : Math.max(margin, viewportHeight - measuredHeight - margin);
    }

    setBubbleStyle({ left, top, width });
  }, []);

  useLayoutEffect(() => {
    if (!open) return;

    const update = () => {
      placeBubble();
    };

    update();
    window.addEventListener("resize", update);
    window.addEventListener("scroll", update, true);
    window.visualViewport?.addEventListener("resize", update);
    window.visualViewport?.addEventListener("scroll", update);
    return () => {
      window.removeEventListener("resize", update);
      window.removeEventListener("scroll", update, true);
      window.visualViewport?.removeEventListener("resize", update);
      window.visualViewport?.removeEventListener("scroll", update);
    };
  }, [open, placeBubble]);

  useEffect(() => {
    setIsMounted(true);
  }, []);

  useEffect(() => {
    if (!open) return;

    const handlePointerDownOutside = (event: PointerEvent) => {
      const target = event.target as Node | null;
      if (!target) return;
      if (triggerRef.current?.contains(target)) return;
      setOpen(false);
    };

    document.addEventListener("pointerdown", handlePointerDownOutside);
    return () => document.removeEventListener("pointerdown", handlePointerDownOutside);
  }, [open]);

  const handleOpen = () => {
    placeBubble();
    setOpen(true);
  };

  const handleClick = (event: React.MouseEvent<HTMLSpanElement>) => {
    event.stopPropagation();
    handleOpen();
  };

  return (
    <span
      ref={triggerRef}
      className={`col-help-trigger${iconOnly ? " icon-only" : ""}`}
      tabIndex={0}
      aria-label={`Explain ${label}`}
      aria-expanded={open}
      onMouseEnter={handleOpen}
      onFocus={handleOpen}
      onClick={handleClick}
      onPointerDown={(event) => event.stopPropagation()}
      onKeyDown={(event) => {
        if (event.key === "Escape") {
          event.stopPropagation();
          setOpen(false);
        }
      }}
      onMouseLeave={() => setOpen(false)}
      onBlur={() => setOpen(false)}
    >
      {iconOnly ? <span className="col-help-icon" aria-hidden="true">i</span> : label}
      {open && isMounted && createPortal(
        <span
          ref={bubbleRef}
          className="col-help-bubble"
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
          {math ? <span className="col-help-bubble-math">Math: {math}</span> : null}
        </span>,
        document.body,
      )}
    </span>
  );
}
