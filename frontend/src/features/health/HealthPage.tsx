"use client";

import dynamic from "next/dynamic";
import { useState } from "react";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import ApiErrorState from "@/features/cuse4/components/ApiErrorState";
import KpiCard from "@/components/KpiCard";
import {
  triggerDailyMaintenanceRefresh,
  useHealthDiagnostics,
  useOperatorStatus,
  useRisk,
} from "@/hooks/useCuse4Api";
import OperatorStatusSection from "@/features/health/OperatorStatusSection";
import { formatAsOfDate } from "@/lib/cuse4Truth";
import { runServeRefreshAndRevalidate } from "@/lib/cuse4Refresh";
import type { SourceDates } from "@/lib/types/cuse4";

const HealthDiagnosticsRoot = dynamic(() => import("@/features/health/HealthDiagnosticsRoot"), {
  ssr: false,
  loading: () => <AnalyticsLoadingViz message="Loading health diagnostics..." />,
});

function fmtAsOfDate(isoDate?: string | null): string {
  if (!isoDate) return "N/A";
  return formatAsOfDate(isoDate);
}

function latestSourceDate(dates: SourceDates | undefined): string {
  const rows = Object.values(dates ?? {}).filter((value): value is string => Boolean(value));
  if (rows.length === 0) return "";
  return [...rows].sort().at(-1) ?? "";
}

export default function HealthPage() {
  const [loadDiagnostics, setLoadDiagnostics] = useState(false);
  const [refreshState, setRefreshState] = useState<"idle" | "running" | "done" | "failed">("idle");
  const [dismissUpdatePrompt, setDismissUpdatePrompt] = useState(false);
  const { data, isLoading, error } = useHealthDiagnostics(loadDiagnostics);
  const {
    data: operatorData,
    isLoading: operatorLoading,
    error: operatorError,
  } = useOperatorStatus();
  const { data: riskData, isLoading: riskLoading, error: riskError } = useRisk();

  if (operatorLoading && !operatorData && riskLoading && !riskData && !loadDiagnostics) {
    return <AnalyticsLoadingViz message="Loading operator health..." />;
  }
  if (isLoading && !operatorData && !riskData) {
    return <AnalyticsLoadingViz message="Loading model health..." />;
  }

  const coreStateThrough = riskData?.risk_engine?.core_state_through_date
    || riskData?.risk_engine?.factor_returns_latest_date
    || null;
  const coreRebuilt = riskData?.risk_engine?.core_rebuild_date
    || riskData?.risk_engine?.last_recompute_date
    || null;
  const estimationExposureAnchor = riskData?.risk_engine?.estimation_exposure_anchor_date || null;
  const currentLoadingsAsOf = riskData?.source_dates?.exposures_served_asof
    || riskData?.model_sanity?.served_loadings_asof
    || riskData?.model_sanity?.coverage_date
    || null;
  const latestSourceAsOf = operatorData?.source_dates?.exposures_latest_available_asof
    || operatorData?.source_dates?.exposures_asof
    || riskData?.model_sanity?.latest_loadings_available_asof
    || riskData?.model_sanity?.latest_available_date
    || latestSourceDate(operatorData?.source_dates);
  const lagDays = riskData?.risk_engine?.cross_section_min_age_days;
  const rSquared = riskData?.r_squared ?? riskData?.risk_engine?.latest_r2 ?? null;
  const allowedProfiles = new Set(operatorData?.runtime?.allowed_profiles ?? []);
  const onlyServeRefreshAllowed = allowedProfiles.size > 0 && allowedProfiles.size === 1 && allowedProfiles.has("serve-refresh");
  const neonAuthoritativeRebuilds = Boolean(operatorData?.runtime?.neon_authoritative_rebuilds);
  const coreDue = Boolean(operatorData?.core_due?.due);
  const servedLoadingsBehind = Boolean(riskData?.model_sanity?.update_available);
  const coreRefreshActionAvailable = coreDue && !onlyServeRefreshAllowed;
  const canRunRefreshAction = servedLoadingsBehind || coreRefreshActionAvailable;
  const updateAvailable = Boolean(
    !dismissUpdatePrompt
    && (servedLoadingsBehind || coreDue),
  );

  async function handleRefreshPrompt() {
    const proceed = window.confirm(
      `Run refresh now?\n\nLatest loadings source date: ${latestSourceAsOf || "n/a"}\nCore state through: ${coreStateThrough || "n/a"}`,
    );
    if (!proceed) return;
    setRefreshState("running");
    try {
      if (onlyServeRefreshAllowed) {
        await runServeRefreshAndRevalidate();
      } else {
        await triggerDailyMaintenanceRefresh();
      }
      setRefreshState("done");
    } catch {
      setRefreshState("failed");
    }
  }

  const operatorSection = (
    <OperatorStatusSection data={operatorData} error={operatorError} isLoading={operatorLoading} />
  );
  const modelSummary = (
    <div className="chart-card">
      <h3 style={{ marginBottom: 6 }}>Current Core Risk State</h3>
      <div className="section-subtitle">
        Live summary from the currently served risk payload. The stable core package changes only on core rebuild lanes; daily serving refreshes project against it without advancing it.
      </div>
      <div className="section-subtitle" style={{ marginBottom: 14 }}>
        {currentLoadingsAsOf
          ? `Daily serving loadings are currently ${fmtAsOfDate(currentLoadingsAsOf)}. The core package below remains frozen until the next core rebuild and uses the lagged estimation anchor shown here.`
          : "Daily serving loadings can refresh independently; the core package below remains frozen until the next core rebuild."}
      </div>
      <div className="kpi-row">
        <KpiCard
          label="R-Squared"
          value={typeof rSquared === "number" ? `${(rSquared * 100).toFixed(1)}%` : "—"}
          subtitle={riskError && !riskData
            ? "Current risk payload unavailable"
            : coreStateThrough
              ? `Current factor-return fit through ${fmtAsOfDate(coreStateThrough)}`
              : "Current factor-return fit"}
        />
        <KpiCard
          label="Core State Through"
          value={fmtAsOfDate(coreStateThrough)}
          subtitle={coreStateThrough ? "Latest return date covered by the stable core package" : "Latest return date covered by the stable core package"}
        />
        <KpiCard
          label="Estimation Anchor"
          value={fmtAsOfDate(estimationExposureAnchor)}
          subtitle={estimationExposureAnchor ? `Lagged exposure basis used to estimate returns (${lagDays ?? 7}d policy)` : `Lagged exposure basis used to estimate returns (${lagDays ?? 7}d policy)`}
        />
        <KpiCard
          label="Core Rebuilt"
          value={fmtAsOfDate(coreRebuilt)}
          subtitle={coreRebuilt ? "Most recent core-weekly / cold-core rebuild date" : "Most recent core-weekly / cold-core rebuild date"}
        />
        <KpiCard
          label="Lag Policy"
          value={`${lagDays ?? 7}d`}
          subtitle="Minimum age of the exposure snapshot used for core estimation"
        />
      </div>
    </div>
  );
  const updateBanner = updateAvailable ? (
    <div className="update-banner">
      <div className="update-banner-title">Update Available</div>
      <div className="update-banner-body">
        {servedLoadingsBehind ? (
          <>
            Newer {neonAuthoritativeRebuilds ? "authoritative Neon" : "source"} factor loadings exist for <strong>{fmtAsOfDate(latestSourceAsOf)}</strong>,
            while the currently served well-covered loadings are older. A serving refresh can publish them without advancing the stable core package.
          </>
        ) : coreDue ? (
          <>
            The core model is due for a rebuild{operatorData?.core_due?.reason ? ` (${operatorData.core_due.reason})` : ""}.
            The current stable core state runs through <strong>{fmtAsOfDate(coreStateThrough)}</strong>.
          </>
        ) : null}
        {coreDue && onlyServeRefreshAllowed && (
          <> Core rebuilds are not available from this runtime. Run `core-weekly` or `cold-core` from the maintenance environment.</>
        )}
        {!onlyServeRefreshAllowed && (
          <> The maintenance lane will sync local LSEG updates first, then rebuild core only if cadence or policy requires it.</>
        )}
      </div>
      <div className="update-banner-actions">
        {canRunRefreshAction && (
          <button
            className="btn-refresh"
            onClick={handleRefreshPrompt}
            disabled={refreshState === "running"}
          >
            {refreshState === "running"
              ? "Refreshing…"
              : servedLoadingsBehind && onlyServeRefreshAllowed
                ? "Run Serving Refresh"
                : "Run Maintenance Refresh"}
          </button>
        )}
        <button
          className="btn-dismiss"
          onClick={() => setDismissUpdatePrompt(true)}
        >
          Dismiss
        </button>
      </div>
      {refreshState === "done" && (
        <div className="update-banner-feedback success">
          {servedLoadingsBehind && onlyServeRefreshAllowed
            ? "Serving refresh completed."
            : "Refresh started in background."}
        </div>
      )}
      {refreshState === "failed" && (
        <div className="update-banner-feedback error">
          Could not start refresh from this page.
        </div>
      )}
    </div>
  ) : null;

  if (!loadDiagnostics) {
    return (
      <div className="health-wrap">
        {operatorSection}
        {updateBanner}
        {modelSummary}
        <div className="chart-card">
          <h3 style={{ margin: "0 0 4px" }}>Model Health Diagnostics</h3>
          <div className="health-load-prompt">
            <div className="section-subtitle">
              This page runs the heaviest diagnostic study in the app. Sections mount lazily as you scroll so routine dashboard use stays fast.
            </div>
            <div className="section-subtitle" style={{ marginBottom: 0 }}>
              Operator Status above is the live control-room truth. Diagnostics below are a deeper local maintenance study of this machine and may lag the Neon-served view.
            </div>
            <button className="health-load-btn" onClick={() => setLoadDiagnostics(true)}>
              Load Diagnostics
            </button>
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="health-wrap">
        {operatorSection}
        {updateBanner}
        {modelSummary}
        <ApiErrorState title="Health Diagnostics Not Ready" error={error} />
      </div>
    );
  }

  if (!data || data.status !== "ok") {
    const diagnosticsDeferred = data?.status === "deferred";
    return (
      <div className="health-wrap">
        {operatorSection}
        {updateBanner}
        {modelSummary}
        <div className="chart-card">
          <h3>Health Diagnostics</h3>
          <div className="detail-history-empty">
            {isLoading
              ? "Health diagnostics are still loading."
              : diagnosticsDeferred
                ? "Deep health diagnostics were deferred on the quick refresh path. Run core-weekly or cold-core to refresh them."
                : "No diagnostics payload is available yet. Run a core lane and reload this page."}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="health-wrap">
      {operatorSection}
      {updateBanner}
      {modelSummary}
      <HealthDiagnosticsRoot data={data} />
    </div>
  );
}
