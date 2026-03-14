"use client";

import dynamic from "next/dynamic";
import { useState } from "react";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import ApiErrorState from "@/components/ApiErrorState";
import KpiCard from "@/components/KpiCard";
import {
  triggerDailyMaintenanceRefresh,
  triggerServeRefresh,
  useHealthDiagnostics,
  useOperatorStatus,
  useRisk,
} from "@/hooks/useApi";
import OperatorStatusSection from "@/features/health/OperatorStatusSection";

const HealthDiagnosticsRoot = dynamic(() => import("@/features/health/HealthDiagnosticsRoot"), {
  ssr: false,
  loading: () => <AnalyticsLoadingViz message="Loading health diagnostics..." />,
});

function fmtAsOfDate(isoDate?: string | null): string {
  if (!isoDate) return "N/A";
  const d = new Date(`${isoDate}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return isoDate;
  return d.toLocaleDateString("en-US", { year: "numeric", month: "short", day: "2-digit" });
}

function latestSourceDate(dates: Record<string, string | null | undefined> | undefined): string {
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

  const modelAsOf = riskData?.risk_engine?.factor_returns_latest_date || riskData?.model_sanity?.coverage_date || null;
  const latestSourceAsOf = riskData?.model_sanity?.latest_available_date || latestSourceDate(operatorData?.source_dates);
  const lagDays = riskData?.risk_engine?.cross_section_min_age_days;
  const rSquared = riskData?.r_squared;
  const allowedProfiles = new Set(operatorData?.runtime?.allowed_profiles ?? []);
  const onlyServeRefreshAllowed = allowedProfiles.size > 0 && allowedProfiles.size === 1 && allowedProfiles.has("serve-refresh");
  const updateAvailable = Boolean(
    !dismissUpdatePrompt
    && (
      riskData?.model_sanity?.update_available
      || (modelAsOf && latestSourceAsOf && latestSourceAsOf > modelAsOf)
    ),
  );

  async function handleRefreshPrompt() {
    const proceed = window.confirm(
      `Run refresh now?\n\nLatest source date: ${latestSourceAsOf || "n/a"}\nModel as-of date: ${modelAsOf || "n/a"}`,
    );
    if (!proceed) return;
    setRefreshState("running");
    try {
      if (onlyServeRefreshAllowed) {
        await triggerServeRefresh();
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
      <h3 style={{ marginBottom: 6 }}>Current Model Quality</h3>
      <div className="section-subtitle">
        Live summary from the currently served risk payload. Load diagnostics below for the deeper regression, coverage, and bias studies.
      </div>
      <div className="kpi-row">
        <KpiCard
          label="R-Squared"
          value={typeof rSquared === "number" ? `${(rSquared * 100).toFixed(1)}%` : "—"}
          subtitle={riskError && !riskData
            ? "Current risk payload unavailable"
            : modelAsOf
              ? `Current factor-return fit as of ${fmtAsOfDate(modelAsOf)}`
              : "Current factor-return fit"}
        />
        <KpiCard
          label="Model As Of"
          value={fmtAsOfDate(modelAsOf)}
          subtitle={modelAsOf ? `Latest well-covered date (${lagDays ?? 7}d lag)` : "Latest well-covered date"}
        />
      </div>
    </div>
  );
  const updateBanner = updateAvailable ? (
    <div className="update-banner">
      <div className="update-banner-title">Update Available</div>
      <div className="update-banner-body">
        Newer source data exists for <strong>{latestSourceAsOf}</strong>, while the model currently uses the latest
        well-covered date <strong>{modelAsOf || "n/a"}</strong>.
      </div>
      <div className="update-banner-actions">
        <button
          className="btn-refresh"
          onClick={handleRefreshPrompt}
          disabled={refreshState === "running"}
        >
          {refreshState === "running" ? "Refreshing…" : "Run Refresh"}
        </button>
        <button
          className="btn-dismiss"
          onClick={() => setDismissUpdatePrompt(true)}
        >
          Dismiss
        </button>
      </div>
      {refreshState === "done" && (
        <div className="update-banner-feedback success">
          Refresh started in background.
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
              Operator Status above is the live control-room truth. Diagnostics below are a deeper local maintenance study and may lag the cloud-serving view.
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
              : "No diagnostics payload is available yet. Run refresh and reload this page."}
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
