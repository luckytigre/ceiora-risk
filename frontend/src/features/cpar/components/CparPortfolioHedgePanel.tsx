"use client";

import type { CparHedgeMode, CparPortfolioHedgeData } from "@/lib/types";
import { describeCparHedgeStatus, formatCparNumber, formatCparPercent } from "@/lib/cparTruth";
import CparPostHedgeTable from "./CparPostHedgeTable";

const MODES: { value: CparHedgeMode; label: string; detail: string }[] = [
  {
    value: "factor_neutral",
    label: "Factor Neutral",
    detail: "Use the aggregate thresholded ETF package across market, sector, and style legs.",
  },
  {
    value: "market_neutral",
    label: "Market Neutral",
    detail: "Use only the aggregate SPY trade-space leg when the portfolio beta is material.",
  },
];

function stat(label: string, value: string, detail?: string) {
  return (
    <div className="cpar-hedge-stat">
      <div className="cpar-package-label">{label}</div>
      <div className="cpar-package-value">{value}</div>
      {detail ? <div className="cpar-package-detail">{detail}</div> : null}
    </div>
  );
}

export default function CparPortfolioHedgePanel({
  data,
  mode,
  onModeChange,
}: {
  data: CparPortfolioHedgeData;
  mode: CparHedgeMode;
  onModeChange: (mode: CparHedgeMode) => void;
}) {
  const status = data.hedge_status ? describeCparHedgeStatus(data.hedge_status) : null;

  return (
    <section className="cpar-hedge-panel" data-testid="cpar-portfolio-hedge-panel">
      <div className="chart-card">
        <h3>Portfolio Hedge Preview</h3>
        <div className="section-subtitle">
          The workflow aggregates covered holdings rows into one active-package cPAR exposure vector, then applies the persisted covariance surface without any request-time fitting.
        </div>
        <div className="cpar-mode-toggle">
          {MODES.map((option) => (
            <button
              key={option.value}
              type="button"
              className={`cpar-mode-btn ${mode === option.value ? "active" : ""}`}
              onClick={() => onModeChange(option.value)}
              title={option.detail}
            >
              {option.label}
            </button>
          ))}
        </div>
        <div className="cpar-badge-row compact">
          {status ? (
            <span className={`cpar-badge ${status.tone}`} title={status.detail}>{status.label}</span>
          ) : (
            <span className="cpar-badge neutral">No Hedge</span>
          )}
          {data.hedge_reason ? <span className="cpar-detail-chip">{data.hedge_reason}</span> : null}
          <span className="cpar-detail-chip">{data.covered_positions_count} covered</span>
          {data.excluded_positions_count > 0 ? <span className="cpar-detail-chip">{data.excluded_positions_count} excluded</span> : null}
        </div>
        <div className="cpar-package-grid compact">
          {stat("Gross MV", formatCparNumber(data.gross_market_value, 2))}
          {stat("Net MV", formatCparNumber(data.net_market_value, 2))}
          {stat("Priced Coverage", formatCparPercent(data.coverage_ratio, 1))}
          {stat("Pre Var", formatCparNumber(data.pre_hedge_factor_variance_proxy, 3))}
          {stat("Post Var", formatCparNumber(data.post_hedge_factor_variance_proxy, 3))}
          {stat("Reduction", formatCparPercent(data.non_market_reduction_ratio, 1))}
        </div>
        <div className="dash-table">
          <table>
            <thead>
              <tr>
                <th>Leg</th>
                <th>Group</th>
                <th className="text-right">Weight</th>
              </tr>
            </thead>
            <tbody>
              {data.hedge_legs.length === 0 ? (
                <tr>
                  <td colSpan={3} className="cpar-empty-row">
                    No hedge legs were required for this portfolio mode.
                  </td>
                </tr>
              ) : (
                data.hedge_legs.map((row) => (
                  <tr key={row.factor_id}>
                    <td>
                      <strong>{row.label || row.factor_id}</strong>
                      <span className="cpar-table-sub">{row.factor_id}</span>
                    </td>
                    <td>{row.group || "—"}</td>
                    <td className="text-right cpar-number-cell">{formatCparNumber(row.weight, 3)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      <CparPostHedgeTable rows={data.post_hedge_exposures} />
    </section>
  );
}
