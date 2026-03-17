"use client";

import { useState, useEffect, useRef, useMemo } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useBackground, type BgMode } from "./BackgroundContext";
import { useOperatorStatus } from "@/hooks/useApi";
import { runServeRefreshAndRevalidate } from "@/lib/refresh";

const TABS = [
  { href: "/exposures", label: "Risk" },
  { href: "/explore", label: "Explore" },
  { href: "/health", label: "Health" },
  { href: "/positions", label: "Positions" },
];

const BG_OPTIONS: { value: BgMode; label: string }[] = [
  { value: "topo", label: "Topographic" },
  { value: "flow", label: "Flow" },
  { value: "none", label: "None" },
];

function parseIsoMs(iso?: string | null): number | null {
  if (!iso) return null;
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? ms : null;
}

function formatAgeFromMs(ms: number, nowMs: number): string {
  const diffMin = Math.max(0, Math.round((nowMs - ms) / 60000));
  if (diffMin < 1) return "just now";
  if (diffMin < 60) return `${diffMin}m ago`;
  const h = Math.round(diffMin / 60);
  if (h < 48) return `${h}h ago`;
  const d = Math.round(h / 24);
  return `${d}d ago`;
}

function formatAgeFromIso(iso: string | null | undefined, nowMs: number): string {
  const ms = parseIsoMs(iso);
  if (ms === null) return "n/a";
  return formatAgeFromMs(ms, nowMs);
}

export default function TabNav() {
  const pathname = usePathname();
  const [menuOpen, setMenuOpen] = useState(false);
  const [refreshActionState, setRefreshActionState] = useState<"idle" | "running" | "failed">("idle");
  const [clockMs, setClockMs] = useState<number>(0);
  const navRef = useRef<HTMLElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const { mode, setMode } = useBackground();
  const { data: operatorStatusData, mutate: mutateOperatorStatus } = useOperatorStatus();
  const holdingsSync = operatorStatusData?.holdings_sync;
  const neonSyncHealth = operatorStatusData?.neon_sync_health;
  const pending = Boolean(holdingsSync?.pending);
  const pendingCount = Number(holdingsSync?.pending_count || 0);
  const dirtySince = holdingsSync?.dirty_since || null;

  const refreshState = operatorStatusData?.refresh;
  const refreshStatus = String(refreshState?.status || "idle").toLowerCase();
  const refreshIsRunning = refreshStatus === "running";

  useEffect(() => {
    const onScroll = () => {
      if (!navRef.current) return;
      const y = window.scrollY;
      const t = Math.min(1, y / 120);
      const opacity = 0.78 - t * 0.26;
      navRef.current.style.backgroundColor = `rgba(16, 16, 19, ${opacity})`;
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

  useEffect(() => {
    if (!pending) {
      setRefreshActionState("idle");
    }
  }, [pending]);

  useEffect(() => {
    setClockMs(Date.now());
    const id = window.setInterval(() => setClockMs(Date.now()), 60000);
    return () => window.clearInterval(id);
  }, []);

  useEffect(() => {
    if (!refreshIsRunning && refreshActionState === "running") {
      setRefreshActionState("idle");
    }
  }, [refreshIsRunning, refreshActionState]);

  useEffect(() => {
    if (!refreshIsRunning) return;
    const id = window.setInterval(() => {
      if (document.visibilityState !== "visible") return;
      void mutateOperatorStatus();
    }, 5000);
    return () => window.clearInterval(id);
  }, [refreshIsRunning, mutateOperatorStatus]);

  useEffect(() => {
    if (refreshIsRunning) return undefined;
    const refreshVisibleState = () => {
      if (document.visibilityState !== "visible") return;
      void mutateOperatorStatus();
    };
    const onFocus = () => refreshVisibleState();
    const onVisibility = () => refreshVisibleState();
    window.addEventListener("focus", onFocus);
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      window.removeEventListener("focus", onFocus);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, [refreshIsRunning, mutateOperatorStatus]);

  async function handleRefreshNow() {
    if (refreshActionState === "running" || refreshIsRunning) return;
    setRefreshActionState("running");
    try {
      await runServeRefreshAndRevalidate();
      await mutateOperatorStatus();
      setRefreshActionState("idle");
    } catch {
      setRefreshActionState("failed");
    }
  }

  const signal = useMemo(() => {
    const lastSyncIso = refreshState?.finished_at || refreshState?.started_at || null;
    const lastSyncAge = formatAgeFromIso(lastSyncIso, clockMs);
    const pendingAge = pending && dirtySince ? formatAgeFromIso(dirtySince, clockMs) : null;
    const neonMirror = String(
      neonSyncHealth?.mirror_status || neonSyncHealth?.status || "",
    ).toLowerCase();
    const neonParity = String(neonSyncHealth?.parity_status || "").toLowerCase();
    const neonMirrorError = neonMirror === "failed" || neonMirror === "mismatch";
    const neonParityError = neonParity === "failed" || neonParity === "mismatch";
    let tone: "success" | "warning" | "error" = "success";
    if (
      refreshActionState === "failed" ||
      refreshStatus === "failed" ||
      refreshStatus === "unknown" ||
      neonMirrorError ||
      neonParityError
    ) {
      tone = "error";
    } else if (
      refreshStatus === "running" ||
      pendingCount > 0 ||
      !lastSyncIso ||
      (() => {
        const ms = parseIsoMs(lastSyncIso);
        if (ms === null) return true;
        const ageMin = Math.max(0, Math.round((clockMs - ms) / 60000));
        return ageMin > 24 * 60;
      })()
    ) {
      tone = "warning";
    }
    const segments: string[] = [`Last sync: ${lastSyncAge}`];
    if (pendingCount > 0) {
      segments.push(`Unsynced edits: ${pendingCount}`);
      if (pendingAge) {
        segments.push(`Pending since: ${pendingAge}`);
      }
    } else {
      segments.push("Unsynced edits: 0");
    }
    if (refreshStatus === "running") segments.push("Sync status: running");
    if (refreshStatus === "failed") segments.push("Sync status: failed");
    if (refreshActionState === "failed") segments.push("Latest refresh attempt did not start");
    if (neonMirror) segments.push(`Neon mirror: ${neonMirror}`);
    if (neonParity) segments.push(`Neon parity: ${neonParity}`);
    return {
      tone,
      detail: segments.join(" | "),
      aria: `Health ${tone}. ${segments.join(". ")}`,
    };
  }, [refreshActionState, refreshState, refreshStatus, pending, pendingCount, dirtySince, clockMs, neonSyncHealth]);

  const refreshActionLabel = pending ? "RECALC" : "SYNC";
  const refreshActionTitle = pending
    ? "Publish latest holdings edits into the served analytics snapshot"
    : "Run serve-refresh";

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

      <div className="dash-tabs-actions">
        <button
          className={`dash-health-signal ${signal.tone}`}
          type="button"
          onClick={() => {
            setRefreshActionState("idle");
            void mutateOperatorStatus();
          }}
          title={signal.detail}
          aria-label={signal.aria}
        >
          <span className="dash-health-dot" />
          <span className="dash-health-detail">{signal.detail}</span>
        </button>
        <button
          className={`dash-recompute-btn ${refreshActionState === "failed" ? "failed" : ""}`}
          onClick={handleRefreshNow}
          disabled={refreshActionState === "running" || refreshIsRunning}
          title={refreshActionTitle}
        >
          {refreshActionState === "running" || refreshIsRunning ? "RUNNING" : refreshActionLabel}
        </button>
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
              <div className="dash-dropdown-section">Settings</div>
              <Link
                href="/data"
                className={`dash-dropdown-item${pathname === "/data" ? " active" : ""}`}
                onClick={() => setMenuOpen(false)}
              >
                Data
              </Link>
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
      </div>
    </nav>
  );
}
