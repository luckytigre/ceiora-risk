"use client";

import { useState, useEffect, useRef } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useBackground, type BgMode } from "./BackgroundContext";

const TABS = [
  { href: "/overview", label: "Overview" },
  { href: "/exposures", label: "Exposures" },
  { href: "/explore", label: "Explore" },
  { href: "/risk", label: "Risk" },
  { href: "/health", label: "Health" },
  { href: "/positions", label: "Positions" },
];

const BG_OPTIONS: { value: BgMode; label: string }[] = [
  { value: "field", label: "Luminous Field" },
  { value: "topo", label: "Topographic" },
  { value: "none", label: "None" },
];

export default function TabNav() {
  const pathname = usePathname();
  const [menuOpen, setMenuOpen] = useState(false);
  const navRef = useRef<HTMLElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const { mode, setMode } = useBackground();

  useEffect(() => {
    const onScroll = () => {
      if (!navRef.current) return;
      const y = window.scrollY;
      const t = Math.min(1, y / 120);
      const opacity = 1 - t * 0.25;
      const r = Math.round(22 + t * 16);
      const g = Math.round(22 + t * 16);
      const b = Math.round(25 + t * 17);
      navRef.current.style.backgroundColor = `rgba(${r}, ${g}, ${b}, ${opacity})`;
    };
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, []);

  // Close menu on outside click
  useEffect(() => {
    if (!menuOpen) return;
    const onClick = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setMenuOpen(false);
      }
    };
    document.addEventListener("mousedown", onClick);
    return () => document.removeEventListener("mousedown", onClick);
  }, [menuOpen]);

  return (
    <nav ref={navRef} className="dash-tabs">
      <span className="dash-tabs-brand">Ceiora</span>

      <div className="dash-tabs-center">
        {TABS.map((tab) => (
          <Link
            key={tab.href}
            href={tab.href}
            className={`dash-tab-btn ${pathname === tab.href ? "active" : ""}`}
          >
            {tab.label}
          </Link>
        ))}
      </div>

      <div ref={menuRef} style={{ position: "relative" }}>
        <button
          className="dash-menu-btn"
          onClick={() => setMenuOpen(!menuOpen)}
          aria-label="Menu"
        >
          <svg
            width="18"
            height="18"
            viewBox="0 0 18 18"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
          >
            {menuOpen ? (
              <>
                <line x1="4" y1="4" x2="14" y2="14" />
                <line x1="14" y1="4" x2="4" y2="14" />
              </>
            ) : (
              <>
                <line x1="3" y1="5" x2="15" y2="5" />
                <line x1="3" y1="9" x2="15" y2="9" />
                <line x1="3" y1="13" x2="15" y2="13" />
              </>
            )}
          </svg>
        </button>

        {menuOpen && (
          <div className="dash-dropdown">
            <div className="dash-dropdown-section">Background</div>
            {BG_OPTIONS.map((opt) => (
              <button
                key={opt.value}
                className={`dash-dropdown-item${mode === opt.value ? " active" : ""}`}
                onClick={() => {
                  setMode(opt.value);
                  setMenuOpen(false);
                }}
              >
                {opt.label}
              </button>
            ))}
          </div>
        )}
      </div>
    </nav>
  );
}
