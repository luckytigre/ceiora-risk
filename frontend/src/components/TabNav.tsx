"use client";

import { useState, useEffect, useRef, useMemo } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useBackground, type BgMode } from "./BackgroundContext";
import { triggerRefresh, useOperatorStatus } from "@/hooks/useApi";

const TABS = [
  { href: "/overview", label: "Overview" },
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

function getNeonMirrorStatus(result: unknown): { mirror: string; parity: string } {
  if (!result || typeof result !== "object") {
    return { mirror: "", parity: "" };
  }
  const mirrorObj = (result as Record<string, unknown>).neon_mirror;
  if (!mirrorObj || typeof mirrorObj !== "object") {
    return { mirror: "", parity: "" };
  }
  const mirror = String((mirrorObj as Record<string, unknown>).status || "").toLowerCase();
  const parityObj = (mirrorObj as Record<string, unknown>).parity;
  const parity =
    parityObj && typeof parityObj === "object"
      ? String((parityObj as Record<string, unknown>).status || "").toLowerCase()
      : "";
  return { mirror, parity };
}

export default function TabNav() {
  const pathname = usePathname();
  const [menuOpen, setMenuOpen] = useState(false);
  const [recomputeState, setRecomputeState] = useState<"idle" | "running" | "failed">("idle");
  const [syncState, setSyncState] = useState<"idle" | "running" | "failed">("idle");
  const [clockMs, setClockMs] = useState<number>(0);
  const navRef = useRef<HTMLElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const { mode, setMode } = useBackground();
  const { data: operatorStatusData, mutate: mutateOperatorStatus } = useOperatorStatus();
  const holdingsSync = operatorStatusData?.holdings_sync;
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
      setRecomputeState("idle");
    }
  }, [pending]);

  useEffect(() => {
    setClockMs(Date.now());
    const id = window.setInterval(() => setClockMs(Date.now()), 60000);
    return () => window.clearInterval(id);
  }, []);

  useEffect(() => {
    if (!refreshIsRunning && syncState === "running") {
      setSyncState("idle");
    }
  }, [refreshIsRunning, syncState]);

  useEffect(() => {
    if (!refreshIsRunning) return;
    const id = window.setInterval(() => {
      if (document.visibilityState !== "visible") return;
      void mutateOperatorStatus();
    }, 5000);
    return () => window.clearInterval(id);
  }, [refreshIsRunning, mutateOperatorStatus]);

  useEffect(() => {
    if (refreshIsRunning) return;
    const id = window.setInterval(() => {
      if (document.visibilityState !== "visible") return;
      void mutateOperatorStatus();
    }, 300000);
    return () => window.clearInterval(id);
  }, [refreshIsRunning, mutateOperatorStatus]);

  async function handleRecalculateNow() {
    if (recomputeState === "running" || refreshIsRunning) return;
    setRecomputeState("running");
    try {
      await triggerRefresh("light");
      await mutateOperatorStatus();
      setRecomputeState("idle");
    } catch {
      setRecomputeState("failed");
    }
  }

  async function handleSyncNow() {
    if (syncState === "running" || refreshIsRunning) return;
    setSyncState("running");
    try {
      await triggerRefresh("light");
      await mutateOperatorStatus();
      setSyncState("idle");
    } catch {
      setSyncState("failed");
    }
  }

  const signal = useMemo(() => {
    const lastSyncIso = refreshState?.finished_at || refreshState?.started_at || null;
    const lastSyncAge = formatAgeFromIso(lastSyncIso, clockMs);
    const pendingAge = pending && dirtySince ? formatAgeFromIso(dirtySince, clockMs) : null;
    const neon = getNeonMirrorStatus(refreshState?.result ?? null);
    const neonMirrorError = neon.mirror === "failed" || neon.mirror === "mismatch";
    const neonParityError = neon.parity === "failed" || neon.parity === "mismatch";
    let tone: "success" | "warning" | "error" = "success";
    if (
      syncState === "failed" ||
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
    if (syncState === "failed") segments.push("Latest sync attempt did not start");
    if (neon.mirror) segments.push(`Neon mirror: ${neon.mirror}`);
    if (neon.parity) segments.push(`Neon parity: ${neon.parity}`);
    return {
      tone,
      detail: segments.join(" | "),
      aria: `Health ${tone}. ${segments.join(". ")}`,
    };
  }, [refreshState, refreshStatus, pending, pendingCount, dirtySince, clockMs, syncState]);

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
            setSyncState("idle");
            void mutateOperatorStatus();
          }}
          title={signal.detail}
          aria-label={signal.aria}
        >
          <span className="dash-health-dot" />
          <span className="dash-health-detail">{signal.detail}</span>
        </button>
        {pending && (
          <button
            className={`dash-recompute-btn ${recomputeState === "failed" ? "failed" : ""}`}
            onClick={handleRecalculateNow}
            disabled={recomputeState === "running" || refreshIsRunning}
            title="Recompute factor analytics from latest holdings edits"
          >
            RECALC
          </button>
        )}
        <button
          className={`dash-menu-btn dash-sync-icon-btn ${syncState === "failed" ? "failed" : ""}`}
          type="button"
          onClick={handleSyncNow}
          disabled={syncState === "running" || refreshIsRunning}
          title="Run a light sync refresh"
          aria-label="Sync"
        >
          <svg
            className={syncState === "running" || refreshIsRunning ? "spin" : ""}
            width="18"
            height="18"
            viewBox="0 0 18 18"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <path d="M3.5 8.2A5.8 5.8 0 0 1 13 4.9" />
            <polyline points="12.8 2.8 13.2 5.2 10.8 5.6" />
            <path d="M14.5 9.8A5.8 5.8 0 0 1 5 13.1" />
            <polyline points="5.2 15.2 4.8 12.8 7.2 12.4" />
          </svg>
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
