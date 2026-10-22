"use client";

import { useMemo } from "react";
import { compareNumber, compareText, useSortableRows } from "@/hooks/useSortableRows";
import type { CparHedgeMode, CparPortfolioHedgeData } from "@/lib/types/cpar";
import { describeCparHedgeStatus, formatCparNumber, formatCparPercent } from "@/lib/cparTruth";
import CparPostHedgeTable from "./CparPostHedgeTable";

type SortKey = "leg" | "group" | "weight";

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
  title = "Portfolio Hedge Preview",
  subtitle = "The workflow aggregates covered holdings rows into one active-package cPAR exposure vector, then applies the persisted covariance surface without any request-time fitting.",
  testId = "cpar-portfolio-hedge-panel",
}: {
  data: CparPortfolioHedgeData;
  mode: CparHedgeMode;
  onModeChange: (mode: CparHedgeMode) => void;
  title?: string;
  subtitle?: string;
  testId?: string;
}) {
  const status = data.hedge_status ? describeCparHedgeStatus(data.hedge_status) : null;
  const comparators = useMemo<Record<SortKey, (left: CparPortfolioHedgeData["hedge_legs"][number], right: CparPortfolioHedgeData["hedge_legs"][number]) => number>>(
    () => ({
      leg: (left, right) => compareText(left.label || left.factor_id, right.label || right.factor_id),
      group: (left, right) => compareText(left.group, right.group),
      weight: (left, right) => compareNumber(left.weight, right.weight),
    }),
    [],
  );
  const { sortedRows: sortedLegs, handleSort, arrow } = useSortableRows<
    CparPortfolioHedgeData["hedge_legs"][number],
    SortKey
  >({
    rows: data.hedge_legs,
    comparators,
  });

  return (
    <section className="cpar-hedge-panel" data-testid={testId}>
      <div className="chart-card">
        <h3>{title}</h3>
        <div className="section-subtitle">{subtitle}</div>
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
          {stat("Pre Var", formatCparNumber(data.pre_hedge_factor_variance_proxy, 3))}
          {stat("Post Var", formatCparNumber(data.post_hedge_factor_variance_proxy, 3))}
          {stat("Reduction", formatCparPercent(data.non_market_reduction_ratio, 1))}
          {stat("Gross Hedge", formatCparNumber(data.gross_hedge_notional, 3))}
          {stat("Net Hedge", formatCparNumber(data.net_hedge_notional, 3))}
        </div>
        <div className="dash-table">
          <table>
            <thead>
              <tr>
                <th className="sortable" onClick={() => handleSort("leg")}>Leg{arrow("leg")}</th>
                <th className="sortable" onClick={() => handleSort("group")}>Group{arrow("group")}</th>
                <th className="text-right sortable" onClick={() => handleSort("weight")}>Weight{arrow("weight")}</th>
              </tr>
            </thead>
            <tbody>
              {sortedLegs.length === 0 ? (
                <tr>
                  <td colSpan={3} className="cpar-empty-row">
                    No hedge legs were required for this portfolio mode.
                  </td>
                </tr>
              ) : (
                sortedLegs.map((row) => (
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
