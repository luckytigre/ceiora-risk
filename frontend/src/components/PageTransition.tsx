"use client";

import { usePathname } from "next/navigation";
import { useEffect, useLayoutEffect, useRef, type ReactNode } from "react";

export default function PageTransition({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const prevPathRef = useRef(pathname);
  const containerRef = useRef<HTMLDivElement>(null);

  // Hide content synchronously before paint to prevent flash
  useLayoutEffect(() => {
    const prev = prevPathRef.current;
    if (pathname === prev) return;
    prevPathRef.current = pathname;

    if (prev === "/") {
      window.scrollTo({ top: 0 });
      return;
    }

    const el = containerRef.current;
    if (!el) {
      window.scrollTo({ top: 0 });
      return;
    }

    el.style.transition = "none";
    el.style.opacity = "0";
    window.scrollTo({ top: 0 });
  }, [pathname]);

  // Fade in after paint
  useEffect(() => {
    const el = containerRef.current;
    if (!el || el.style.opacity !== "0") return;

    requestAnimationFrame(() => {
      el.style.transition = "opacity 0.6s ease-out";
      el.style.opacity = "1";
    });
  }, [pathname]);

  return <div ref={containerRef} className="dash-page-transition">{children}</div>;
}
