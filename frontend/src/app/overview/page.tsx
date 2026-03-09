"use client";

import { useState } from "react";
import { triggerRefresh, usePortfolio, useRisk } from "@/hooks/useApi";
import KpiCard from "@/components/KpiCard";
import RiskDecompChart from "@/components/RiskDecompChart";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import TableRowToggle from "@/components/TableRowToggle";
import ApiErrorState from "@/components/ApiErrorState";

const COLLAPSED_ROWS = 10;

function fmt(n: number): string {
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
  return `$${n.toFixed(2)}`;
}

function fmtAsOfDate(isoDate?: string): string {
  if (!isoDate) return "N/A";
  const d = new Date(`${isoDate}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return isoDate;
  return d.toLocaleDateString("en-US", { year: "numeric", month: "short", day: "2-digit" });
}

export default function OverviewPage() {
  const { data: portfolio, isLoading: pLoading, error: pError } = usePortfolio();
  const { data: risk, isLoading: rLoading, error: rError } = useRisk();
  const [showAllHoldings, setShowAllHoldings] = useState(false);
  const [refreshState, setRefreshState] = useState<"idle" | "running" | "done" | "failed">("idle");
  const [dismissUpdatePrompt, setDismissUpdatePrompt] = useState(false);

  if (pLoading || rLoading) {
    return <AnalyticsLoadingViz message="Loading overview..." />;
  }
  if (pError || rError) {
    return <ApiErrorState title="Overview Data Not Ready" error={pError || rError} />;
  }

  const positions = portfolio?.positions ?? [];
  const totalValue = portfolio?.total_value ?? 0;
  const posCount = portfolio?.position_count ?? 0;
  const rSquared = risk?.r_squared ?? 0;
  const condNum = risk?.condition_number ?? 0;
  const riskShares = risk?.risk_shares ?? { country: 0, industry: 0, style: 0, idio: 100 };
  const modelAsOf = risk?.risk_engine?.factor_returns_latest_date;
  const lagDays = risk?.risk_engine?.cross_section_min_age_days;
  const latestSourceAsOf = String(
    portfolio?.source_dates?.exposures_asof
      || portfolio?.source_dates?.fundamentals_asof
      || "",
  );
  const updateAvailable = Boolean(
    !dismissUpdatePrompt
    && modelAsOf
    && latestSourceAsOf
    && latestSourceAsOf > String(modelAsOf),
  );

  const holdings = [...positions].sort((a, b) => b.market_value - a.market_value);
  const visibleHoldings = showAllHoldings ? holdings : holdings.slice(0, COLLAPSED_ROWS);

  async function handleRefreshPrompt() {
    const proceed = window.confirm(
      `Run refresh now?\n\nLatest source date: ${latestSourceAsOf}\nModel as-of date: ${modelAsOf}`,
    );
    if (!proceed) return;
    setRefreshState("running");
    try {
      await triggerRefresh("full");
      setRefreshState("done");
    } catch {
      setRefreshState("failed");
    }
  }

  return (
    <div>
      {updateAvailable && (
        <div className="chart-card mb-4" style={{ border: "1px solid rgba(255, 143, 42, 0.45)" }}>
          <h3 style={{ marginBottom: 8 }}>Update Available</h3>
          <div style={{ color: "rgba(232, 237, 249, 0.86)", fontSize: 13, lineHeight: 1.5 }}>
            Newer source data exists for <strong>{latestSourceAsOf}</strong>, while the model currently uses the latest
            well-covered date <strong>{modelAsOf}</strong>.
          </div>
          <div style={{ marginTop: 10, display: "flex", gap: 8, alignItems: "center" }}>
            <button
              className="btn btn-secondary"
              onClick={handleRefreshPrompt}
              disabled={refreshState === "running"}
            >
              {refreshState === "running" ? "Starting refresh..." : "Run Refresh"}
            </button>
            <button
              className="btn btn-secondary"
              onClick={() => setDismissUpdatePrompt(true)}
              style={{ opacity: 0.8 }}
            >
              Dismiss
            </button>
          </div>
          {refreshState === "done" && (
            <div style={{ marginTop: 8, color: "rgba(169,182,210,0.85)", fontSize: 12 }}>
              Refresh started in background.
            </div>
          )}
          {refreshState === "failed" && (
            <div style={{ marginTop: 8, color: "rgba(204,53,88,0.9)", fontSize: 12 }}>
              Could not start refresh from this page.
            </div>
          )}
        </div>
      )}

      <div className="kpi-row">
        <KpiCard label="Total Value" value={fmt(totalValue)} subtitle={`${posCount} positions`} />
        <KpiCard label="Positions" value={String(posCount)} />
        <KpiCard
          label="R-Squared"
          value={`${(rSquared * 100).toFixed(1)}%`}
          subtitle="Model explanatory power"
        />
        <KpiCard
          label="Data Age"
          value={fmtAsOfDate(modelAsOf)}
          subtitle={modelAsOf ? `Barra model as-of (${lagDays ?? 7}d lag)` : "Barra model as-of"}
        />
        <KpiCard
          label="Condition #"
          value={condNum > 1e6 ? condNum.toExponential(1) : condNum.toFixed(1)}
          subtitle="Factor covariance matrix"
        />
      </div>

      <div className="chart-card mb-4">
        <h3>Risk Decomposition</h3>
        <RiskDecompChart shares={riskShares} />
      </div>

      <div className="chart-card">
        <h3>Top Holdings</h3>
        <div className="dash-table">
          <table>
            <thead>
              <tr>
                <th>Ticker</th>
                <th className="text-right">Value</th>
                <th className="text-right">Weight</th>
                <th>Sector</th>
                <th className="text-right">Risk Contrib</th>
              </tr>
            </thead>
            <tbody>
              {visibleHoldings.map((pos) => (
                <tr key={pos.ticker}>
                  <td><strong>{pos.ticker}</strong></td>
                  <td className="text-right">{fmt(pos.market_value)}</td>
                  <td className="text-right">{(pos.weight * 100).toFixed(2)}%</td>
                  <td>{pos.trbc_economic_sector_short || "—"}</td>
                  <td className="text-right">{pos.risk_contrib_pct.toFixed(2)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
          <TableRowToggle
            totalRows={holdings.length}
            collapsedRows={COLLAPSED_ROWS}
            expanded={showAllHoldings}
            onToggle={() => setShowAllHoldings((prev) => !prev)}
            label="holdings"
          />
        </div>
      </div>
    </div>
  );
}
