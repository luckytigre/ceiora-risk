"use client";

import { ReactNode, useState } from "react";

export default function ExploreSectionCard({
  title,
  subtitle,
  badge,
  defaultOpen = false,
  children,
}: {
  title: string;
  subtitle?: string;
  badge?: ReactNode;
  defaultOpen?: boolean;
  children: ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <div className="chart-card mb-4">
      <button
        type="button"
        className={`explore-section-toggle${open ? " open" : ""}`}
        onClick={() => setOpen((prev) => !prev)}
        aria-expanded={open}
      >
        <span>
          <span className="explore-section-title">
            {title}
            {badge}
          </span>
          {subtitle ? <span className="section-subtitle explore-section-subtitle">{subtitle}</span> : null}
        </span>
        <span className="explore-section-chevron" aria-hidden="true">+</span>
      </button>
      {open ? <div className="explore-section-body">{children}</div> : null}
    </div>
  );
}
