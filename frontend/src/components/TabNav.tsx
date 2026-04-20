"use client";

import { createInternalNeonAuth } from "@neondatabase/auth";
import { useState, useEffect, useLayoutEffect, useRef, useMemo, useCallback } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import BrandLockup from "@/components/BrandLockup";
import { useAppSettings } from "./AppSettingsContext";
import { useAuthSession } from "@/components/AuthSessionContext";
import { useOperatorStatus } from "@/hooks/useCuse4Api";
import { useOperatorTokenAvailable } from "@/hooks/useOperatorTokenAvailable";
import { isPublicShellPath } from "@/lib/appAccess";
import { clearStoredAuthTokens } from "@/lib/authTokens";
import { runServeRefreshAndRevalidate } from "@/lib/cuse4Refresh";

const CUSE_TABS = [
  { href: "/cuse/exposures", label: "Risk", matchPrefix: "/cuse/exposures" },
  { href: "/cuse/explore", label: "Explore", matchPrefix: "/cuse/explore" },
  { href: "/cuse/health", label: "Health", matchPrefix: "/cuse/health" },
];

const CPAR_TABS = [
  { href: "/cpar/risk", label: "Risk", matchPrefix: "/cpar/risk" },
  { href: "/cpar/explore", label: "Explore", matchPrefix: "/cpar/explore" },
  { href: "/cpar/hedge", label: "Hedge", matchPrefix: "/cpar/hedge" },
  { href: "/cpar/health", label: "Health", matchPrefix: "/cpar/health" },
];

const POSITIONS_TABS = [{ href: "/positions", label: "Positions", matchPrefix: "/positions" }];

const LANDING_FAMILY_TRANSITION_EVENT = "landing-family-transition-start";

function readCssTriplet(name: string, fallback: [number, number, number]): [number, number, number] {
  if (typeof window === "undefined") return fallback;
  const raw = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  const parts = raw.split(",").map((part) => Number(part.trim()));
  if (parts.length !== 3 || parts.some((part) => Number.isNaN(part))) return fallback;
  return [parts[0], parts[1], parts[2]];
}

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
  const { session, context, neonProjectUrl } = useAuthSession();
  const activePath = pathname || "";
  const isPublicShell = isPublicShellPath(activePath);
  const isPositionsPage = activePath === "/positions";
  const activeFamily = activePath.startsWith("/cpar") ? "cpar" : activePath.startsWith("/cuse") ? "cuse" : null;
  const [transitionFamily, setTransitionFamily] = useState<"cuse" | "cpar" | null>(null);
  const prevFamilyRef = useRef<string | null>(null);
  const badgeSlotRef = useRef<HTMLSpanElement>(null);
  const [menuOpen, setMenuOpen] = useState(false);
  const [refreshActionState, setRefreshActionState] = useState<"idle" | "running" | "failed">("idle");
  const [clockMs, setClockMs] = useState<number>(0);
  const navRef = useRef<HTMLElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const { themeMode } = useAppSettings();
  const operatorTokenAvailable = useOperatorTokenAvailable();
  const showOperatorChrome = operatorTokenAvailable && (activePath.startsWith("/cuse") || activePath === "/positions" || activePath === "/data");
  const { data: operatorStatusData, mutate: mutateOperatorStatus } = useOperatorStatus(showOperatorChrome);
  const holdingsSync = operatorStatusData?.holdings_sync;
  const neonSyncHealth = operatorStatusData?.neon_sync_health;
  const pending = Boolean(holdingsSync?.pending);
  const pendingCount = Number(holdingsSync?.pending_count || 0);
  const dirtySince = holdingsSync?.dirty_since || null;

  const refreshState = operatorStatusData?.refresh;
  const refreshStatus = String(refreshState?.status || "idle").toLowerCase();
  const refreshIsRunning = refreshStatus === "running";

  useLayoutEffect(() => {
    const el = badgeSlotRef.current;
    if (!el) return;

    if (activeFamily) {
      setTransitionFamily(null);
      const prev = prevFamilyRef.current;
      prevFamilyRef.current = activeFamily;

      if (!prev) {
        // Entering from landing — start hidden, then fade in via CSS transition
        el.style.transition = "none";
        el.style.opacity = "0";
        requestAnimationFrame(() => {
          el.style.transition = "opacity 0.65s ease-out";
          el.style.opacity = "1";
        });
      } else if (prev !== activeFamily) {
        // Switching families — quick cross-fade
        el.style.transition = "none";
        el.style.opacity = "0";
        setTimeout(() => {
          el.style.transition = "opacity 0.3s ease-out";
          el.style.opacity = "1";
        }, 120);
      }
    } else {
      prevFamilyRef.current = null;
      el.style.transition = "none";
      el.style.opacity = "0";
    }
  }, [activeFamily]);

  useEffect(() => {
    const onTransitionStart = (event: Event) => {
      const detail = (event as CustomEvent<{ family?: "cuse" | "cpar" }>).detail;
      if (detail?.family === "cuse" || detail?.family === "cpar") {
        setTransitionFamily(detail.family);
      }
    };
    window.addEventListener(LANDING_FAMILY_TRANSITION_EVENT, onTransitionStart as EventListener);
    return () => {
      window.removeEventListener(LANDING_FAMILY_TRANSITION_EVENT, onTransitionStart as EventListener);
    };
  }, []);

  const isLanding = activePath === "/" && !transitionFamily;

  useEffect(() => {
    if (!navRef.current) return;
    if (isLanding) {
      navRef.current.style.backgroundColor = "";
      navRef.current.style.boxShadow = "";
      return;
    }
    const navBgRgb = readCssTriplet("--nav-bg-rgb", themeMode === "light" ? [239, 238, 233] : [16, 16, 19]);
    const navPositionsRgb = readCssTriplet("--nav-bg-positions-rgb", themeMode === "light" ? [245, 244, 240] : [0, 0, 0]);
    const shadowRgb = readCssTriplet("--nav-shadow-rgb", themeMode === "light" ? [86, 92, 104] : [2, 6, 14]);
    if (isPositionsPage) {
      navRef.current.style.backgroundColor = `rgba(${navPositionsRgb[0]}, ${navPositionsRgb[1]}, ${navPositionsRgb[2]}, 0.94)`;
      navRef.current.style.boxShadow = `0 10px 28px rgba(${shadowRgb[0]}, ${shadowRgb[1]}, ${shadowRgb[2]}, ${themeMode === "light" ? 0.16 : 0.38})`;
      return;
    }
    const onScroll = () => {
      if (!navRef.current) return;
      const y = window.scrollY;
      const t = Math.min(1, y / 120);
      const bgOpacity = themeMode === "light" ? 0.95 - t * 0.08 : 0.99 - t * 0.07;
      const shadowOpacity = themeMode === "light" ? 0.1 + t * 0.1 : 0.18 + t * 0.18;
      const shadowSpread = 8 + t * 18;
      navRef.current.style.backgroundColor = `rgba(${navBgRgb[0]}, ${navBgRgb[1]}, ${navBgRgb[2]}, ${bgOpacity})`;
      navRef.current.style.boxShadow = `0 ${shadowSpread}px ${shadowSpread * 2.5}px rgba(${shadowRgb[0]}, ${shadowRgb[1]}, ${shadowRgb[2]}, ${shadowOpacity})`;
    };
    window.addEventListener("scroll", onScroll, { passive: true });
    onScroll();
    return () => window.removeEventListener("scroll", onScroll);
  }, [isLanding, isPositionsPage, themeMode]);

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
    if (!showOperatorChrome) return;
    if (!refreshIsRunning) return;
    const id = window.setInterval(() => {
      if (document.visibilityState !== "visible") return;
      void mutateOperatorStatus();
    }, 5000);
    return () => window.clearInterval(id);
  }, [showOperatorChrome, refreshIsRunning, mutateOperatorStatus]);

  useEffect(() => {
    if (!showOperatorChrome) return undefined;
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
  }, [showOperatorChrome, refreshIsRunning, mutateOperatorStatus]);

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
  const tabs = useMemo(() => {
    if (activePath === "/positions") return POSITIONS_TABS;
    if (activePath.startsWith("/cpar")) return CPAR_TABS;
    if (activePath.startsWith("/cuse") || activePath === "/data") return CUSE_TABS;
    return [];
  }, [activePath]);
  const effectiveFamily = activeFamily ?? transitionFamily;

  const tabsCenterRef = useRef<HTMLDivElement>(null);
  const indicatorRef = useRef<HTMLSpanElement>(null);
  const hasAnimatedRef = useRef(false);

  const syncIndicator = useCallback(() => {
    const container = tabsCenterRef.current;
    const indicator = indicatorRef.current;
    if (!container || !indicator) return;
    const activeEl = container.querySelector<HTMLElement>(".dash-tab-btn.active");
    if (!activeEl) {
      indicator.style.opacity = "0";
      return;
    }
    const left = activeEl.offsetLeft;
    const width = activeEl.offsetWidth;
    if (!hasAnimatedRef.current) {
      indicator.style.transition = "none";
      hasAnimatedRef.current = true;
      requestAnimationFrame(() => {
        indicator.style.transition = "";
      });
    }
    indicator.style.transform = `translateX(${left}px)`;
    indicator.style.width = `${width}px`;
    indicator.style.opacity = "1";
  }, []);

  useEffect(() => {
    syncIndicator();
  }, [activePath, tabs, syncIndicator]);

  async function handleLogout() {
    await fetch("/api/auth/logout", { method: "POST" });
    if (session?.authProvider === "neon" && neonProjectUrl) {
      try {
        await createInternalNeonAuth(neonProjectUrl).adapter.signOut();
      } catch {}
    }
    clearStoredAuthTokens();
    window.location.assign("/");
  }

  if (isPublicShell) {
    return null;
  }

  return (
    <nav
      ref={navRef}
      className={`dash-tabs${isLanding ? " dash-tabs-landing" : ""}${isPositionsPage ? " dash-tabs-positions" : ""}`}
    >
      <div className="dash-tabs-brand-cluster">
        <BrandLockup
          href="/home"
          className="dash-tabs-brand"
          markClassName="dash-tabs-brand-mark"
          wordmarkClassName="dash-tabs-brand-wordmark"
          markTitle="Ceiora"
        />
      </div>

      <div ref={tabsCenterRef} className="dash-tabs-center">
        {tabs.map((tab) => (
          <Link
            key={tab.href}
            href={tab.href}
            className={`dash-tab-btn ${activePath === tab.href || (tab.matchPrefix && activePath.startsWith(tab.matchPrefix)) ? "active" : ""}`}
          >
            {tab.label}
          </Link>
        ))}
        <span ref={indicatorRef} className="dash-tab-indicator" aria-hidden="true" />
      </div>

      <div className="dash-tabs-actions">
        {showOperatorChrome && (
          <>
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
          </>
        )}
        <span
          ref={badgeSlotRef}
          className={`dash-tabs-family-badge-slot dash-tabs-family-badge-slot-right${effectiveFamily ? " is-active" : ""}${!activeFamily && transitionFamily ? " is-preview" : ""}`}
          aria-hidden={effectiveFamily ? undefined : "true"}
        >
          {effectiveFamily ? (
            <span
              className={`dash-tabs-family-badge dash-tabs-family-badge-${effectiveFamily}`}
              style={!activeFamily ? { visibility: "hidden" } : undefined}
            >
              {effectiveFamily === "cuse" ? (
                <>
                  <span className="dash-tabs-family-badge-prefix">c</span>USE
                </>
              ) : (
                <>
                  <span className="dash-tabs-family-badge-prefix">c</span>PAR
                </>
              )}
            </span>
          ) : null}
        </span>
        <div ref={menuRef} style={{ position: "relative" }}>
          <button
            className="dash-menu-btn"
            onClick={() => setMenuOpen(!menuOpen)}
            aria-label="Menu"
            aria-expanded={menuOpen}
          >
            <svg
              className="dash-menu-icon"
              width="18"
              height="18"
              viewBox="0 0 18 18"
              fill="none"
              stroke="currentColor"
              strokeWidth="1.5"
              strokeLinecap="round"
            >
              <line className="dash-menu-line dash-menu-line-1" x1="3" y1="5" x2="15" y2="5" />
              <line className="dash-menu-line dash-menu-line-2" x1="3" y1="9" x2="15" y2="9" />
              <line className="dash-menu-line dash-menu-line-3" x1="3" y1="13" x2="15" y2="13" />
            </svg>
          </button>

          {menuOpen && (
            <div className="dash-dropdown">
              <div className="dash-dropdown-group">
                <div className="dash-dropdown-section">Navigation</div>
                <div className="dash-dropdown-group-items">
                  <Link
                    href="/positions"
                    className={`dash-dropdown-item${pathname === "/positions" ? " active" : ""}`}
                    onClick={() => setMenuOpen(false)}
                  >
                    Positions
                  </Link>
                </div>
              </div>
              <div className="dash-dropdown-group">
                <div className="dash-dropdown-section">Settings</div>
                <div className="dash-dropdown-group-items">
                  <Link
                    href="/settings"
                    className={`dash-dropdown-item${pathname === "/settings" ? " active" : ""}`}
                    onClick={() => setMenuOpen(false)}
                  >
                    Global settings
                  </Link>
                  {session?.isAdmin && context?.admin_settings_enabled !== false ? (
                    <Link
                      href="/settings/admin"
                      className={`dash-dropdown-item${pathname === "/settings/admin" ? " active" : ""}`}
                      onClick={() => setMenuOpen(false)}
                    >
                      Admin settings
                    </Link>
                  ) : null}
                  <Link
                    href="/data"
                    className={`dash-dropdown-item${pathname === "/data" ? " active" : ""}`}
                    onClick={() => setMenuOpen(false)}
                  >
                    cUSE data
                  </Link>
                  <span className="dash-dropdown-item disabled" aria-disabled="true">cPAR data</span>
                </div>
              </div>
              <div className="dash-dropdown-group">
                <div className="dash-dropdown-section">Account</div>
                <div className="dash-dropdown-group-items">
                  <button
                    className="dash-dropdown-item"
                    onClick={() => {
                      setMenuOpen(false);
                      void handleLogout();
                    }}
                  >
                    Sign out
                  </button>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </nav>
  );
}
