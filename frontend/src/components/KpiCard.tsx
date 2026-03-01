"use client";

import { useLayoutEffect, useRef, useState } from "react";
import type { CSSProperties, ReactNode } from "react";

interface KpiCardProps {
  label: ReactNode;
  value: string;
  subtitle?: string;
  breakdown?: { label: string; value: string; detail?: string }[];
  onClick?: () => void;
  className?: string;
}

export default function KpiCard({ label, value, subtitle, breakdown, onClick, className = "" }: KpiCardProps) {
  const [expanded, setExpanded] = useState(false);
  const [breakdownHeight, setBreakdownHeight] = useState(0);
  const isExpandable = breakdown && breakdown.length > 0;
  const breakdownInnerRef = useRef<HTMLDivElement | null>(null);

  useLayoutEffect(() => {
    if (!isExpandable || !breakdownInnerRef.current) return;
    const node = breakdownInnerRef.current;
    const measure = () => setBreakdownHeight(node.scrollHeight);
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(node);
    window.addEventListener("resize", measure);
    return () => {
      observer.disconnect();
      window.removeEventListener("resize", measure);
    };
  }, [isExpandable, breakdown]);

  return (
    <div
      className={`kpi-card ${isExpandable ? "kpi-card-expandable" : ""} ${className}`}
      onClick={() => {
        if (isExpandable) setExpanded(!expanded);
        onClick?.();
      }}
    >
      <div className="label">
        {label}
        {isExpandable && (
          <span className={`kpi-toggle-glyph ml-1 text-[var(--text-muted)] ${expanded ? "open" : ""}`}>+</span>
        )}
      </div>
      <div className="value">{value}</div>
      {subtitle && <div className="sub">{subtitle}</div>}
      {isExpandable && breakdown && (
        <div
          className={`kpi-breakdown ${expanded ? "open" : ""}`}
          aria-hidden={!expanded}
          style={{ height: expanded ? breakdownHeight : 0 }}
        >
          <div ref={breakdownInnerRef} className="kpi-breakdown-inner pt-3 border-t border-[var(--border)] space-y-1.5">
            {breakdown.map((row, i) => (
              <div
                key={i}
                className="kpi-breakdown-row flex justify-between text-xs"
                style={{ ["--kpi-row-index" as string]: i } as CSSProperties}
              >
                <span className="text-[var(--text-secondary)]">{row.label}</span>
                <span className="text-[var(--text-primary)] font-medium">
                  {row.value}
                  {row.detail && <span className="text-[var(--text-muted)] ml-1">({row.detail})</span>}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
