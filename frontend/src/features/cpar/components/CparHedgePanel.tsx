"use client";

import { useState } from "react";
import { useCparHedge } from "@/hooks/useCparApi";
import {
  describeCparHedgeStatus,
  formatCparNumber,
  formatCparPercent,
  readCparError,
  sameCparPackageIdentity,
} from "@/lib/cparTruth";
import type { CparFitStatus, CparHedgeMode } from "@/lib/types/cpar";
import { CparInlineLoadingState } from "./CparLoadingState";
import CparPostHedgeTable from "./CparPostHedgeTable";

const MODES: { value: CparHedgeMode; label: string; detail: string }[] = [
  {
    value: "factor_neutral",
    label: "Factor Neutral",
    detail: "Use the thresholded raw ETF package across market, sector, and style legs.",
  },
  {
    value: "market_neutral",
    label: "Market Neutral",
    detail: "Use only the SPY leg when the persisted trade-space beta is material.",
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

export default function CparHedgePanel({
  ticker,
  ric,
  fitStatus,
  expectedPackageRunId,
  expectedPackageDate,
}: {
  ticker: string;
  ric: string;
  fitStatus: CparFitStatus;
  expectedPackageRunId: string;
  expectedPackageDate: string;
}) {
  const [mode, setMode] = useState<CparHedgeMode>("factor_neutral");
  const enabled = fitStatus !== "insufficient_history";
  const { data, error, isLoading } = useCparHedge(ticker, mode, ric, enabled);

  if (!enabled) {
    return (
      <section className="chart-card">
        <h3>Hedge Preview</h3>
        <div className="detail-history-empty compact">
          Hedge output is blocked when the persisted cPAR fit status is `insufficient_history`.
        </div>
      </section>
    );
  }

  const errorSummary = error ? readCparError(error) : null;
  const status = data ? describeCparHedgeStatus(data.hedge_status) : null;
  const packageMismatch = Boolean(
    data
    && !sameCparPackageIdentity(
      { package_run_id: expectedPackageRunId, package_date: expectedPackageDate },
      data,
    ),
  );

  return (
    <section className="cpar-hedge-panel" data-testid="cpar-hedge-panel">
      <div className="chart-card">
        <h3>Hedge Preview</h3>
        <div className="section-subtitle">
          Hedge output is derived from persisted thresholded loadings and persisted package covariance only.
        </div>
        <div className="cpar-mode-toggle">
          {MODES.map((option) => (
            <button
              key={option.value}
              type="button"
              className={`cpar-mode-btn ${mode === option.value ? "active" : ""}`}
              onClick={() => setMode(option.value)}
              title={option.detail}
            >
              {option.label}
            </button>
          ))}
        </div>

        {isLoading && !data ? (
          <CparInlineLoadingState message="Loading cPAR hedge preview..." />
        ) : errorSummary ? (
          <div className="cpar-inline-message warning">
            <strong>
              {errorSummary.kind === "not_ready" ? "Hedge package not ready." : "Hedge preview unavailable."}
            </strong>
            <span>{errorSummary.message}</span>
          </div>
        ) : packageMismatch ? (
          <div className="cpar-inline-message error" data-testid="cpar-hedge-package-mismatch">
            <strong>Hedge preview drifted to a different active package.</strong>
            <span>The persisted hedge response no longer matches the selected detail row.</span>
            <span>Reload the page before interpreting hedge output.</span>
          </div>
        ) : !data ? (
          <div className="detail-history-empty compact">No hedge preview is available for this instrument.</div>
        ) : (
          <>
            <div className="cpar-badge-row compact">
              <span className={`cpar-badge ${status?.tone || "neutral"}`} title={status?.detail}>
                {status?.label || "Hedge"}
              </span>
              {data.hedge_reason ? <span className="cpar-detail-chip">{data.hedge_reason}</span> : null}
            </div>
            <div className="cpar-package-grid compact">
              {stat("Pre Var", formatCparNumber(data.pre_hedge_factor_variance_proxy, 3))}
              {stat("Post Var", formatCparNumber(data.post_hedge_factor_variance_proxy, 3))}
              {stat("Reduction", formatCparPercent(data.non_market_reduction_ratio, 1))}
              {stat("Gross", formatCparNumber(data.gross_hedge_notional, 3))}
              {stat("Net", formatCparNumber(data.net_hedge_notional, 3))}
              {stat("Overlap", formatCparPercent(data.stability.leg_overlap_ratio, 1))}
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
                        No hedge legs were required for this mode.
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
          </>
        )}
      </div>

      {data && !packageMismatch ? <CparPostHedgeTable rows={data.post_hedge_exposures} /> : null}
    </section>
  );
}
