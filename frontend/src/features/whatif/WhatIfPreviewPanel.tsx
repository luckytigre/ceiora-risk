"use client";

import type { Ref } from "react";
import ExposureBarChart from "@/components/ExposureBarChart";
import { shortFactorLabel } from "@/lib/factorLabels";
import type { WhatIfPreviewData } from "@/lib/types";
import { formatAsOfDate } from "@/lib/analyticsTruth";
import { fmtQty, WHAT_IF_MODES, type WhatIfMode } from "@/features/whatif/whatIfUtils";

interface WhatIfPreviewPanelProps {
  currentModeFactorOrder: string[];
  mode: WhatIfMode;
  onModeChange: (mode: WhatIfMode) => void;
  onToggleResults: () => void;
  previewData: WhatIfPreviewData | null;
  showResults: boolean;
  toggleRef: Ref<HTMLDivElement>;
}

export default function WhatIfPreviewPanel({
  currentModeFactorOrder,
  mode,
  onModeChange,
  onToggleResults,
  previewData,
  showResults,
  toggleRef,
}: WhatIfPreviewPanelProps) {
  if (!previewData) {
    return (
      <div className="whatif-results-placeholder">
        Stage one or more trade deltas and run <strong>Preview</strong> to turn this page into the full current-versus-hypothetical portfolio analysis.
      </div>
    );
  }

  const truthSurface = String(previewData.truth_surface || "").trim();
  const servedModelPreview = truthSurface === "live_holdings_projected_through_current_served_model";
  const currentSideDescription = servedModelPreview
    ? "Current side = live holdings projected through the current served model snapshot"
    : "Current side = live holdings projected through current published loadings plus live risk-cache fallback";

  return (
    <>
      <div
        ref={toggleRef}
        role="button"
        tabIndex={0}
        className={`whatif-results-divider${showResults ? " open" : ""}`}
        onClick={onToggleResults}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            onToggleResults();
          }
        }}
      >
        <span className="whatif-results-divider-rule" />
        <span className="whatif-results-divider-label">
          Scenario Analysis
          <span className="whatif-results-divider-chevron" aria-hidden="true" />
        </span>
        <span className="whatif-results-divider-rule" />
      </div>

      {showResults && (
        <div className="whatif-results-body">
          <div className="section-subtitle" style={{ marginBottom: 12 }}>
            {currentSideDescription}
            {previewData.serving_snapshot?.snapshot_id ? ` ${previewData.serving_snapshot.snapshot_id}` : ""}.
            {servedModelPreview
              ? " This preview is exploratory and does not replace the dashboard’s published truth surface."
              : " Risk inputs are temporarily falling back to live cache because the current published snapshot predates the new durable risk payloads."}
            Dashboard pages only change after `RECALC` publishes a new snapshot.
            {previewData.source_dates?.exposures_served_asof
              ? ` Served exposures are as of ${formatAsOfDate(previewData.source_dates.exposures_served_asof)}.`
              : ""}
          </div>
          <div className="explore-mode-toggle">
            {WHAT_IF_MODES.map((entry) => (
              <button
                key={entry.key}
                type="button"
                className={`explore-mode-btn${mode === entry.key ? " active" : ""}`}
                onClick={() => onModeChange(entry.key)}
              >
                {entry.label}
              </button>
            ))}
          </div>

          <div className="explore-detail-grid">
            <div className="chart-card">
              <span className="explore-compare-label">Live Holdings Preview</span>
              <ExposureBarChart
                factors={previewData.current.exposure_modes[mode]}
                mode={mode}
                factorCatalog={previewData.current.factor_catalog}
              />
            </div>
            <div className="chart-card">
              <span className="explore-compare-label">Hypothetical Portfolio</span>
              <ExposureBarChart
                factors={previewData.hypothetical.exposure_modes[mode]}
                mode={mode}
                orderByFactors={currentModeFactorOrder}
                factorCatalog={previewData.hypothetical.factor_catalog}
              />
            </div>
          </div>

          <div className="explore-whatif-grid">
            <div className="dash-table">
              <h4 className="explore-whatif-table-title">Risk Share Delta</h4>
              <table>
                <thead>
                  <tr>
                    <th>Bucket</th>
                    <th className="text-right">Current</th>
                    <th className="text-right">Hypothetical</th>
                    <th className="text-right">Delta</th>
                  </tr>
                </thead>
                <tbody>
                  {(["market", "industry", "style", "idio"] as const).map((bucket) => (
                    <tr key={bucket}>
                      <td>{bucket}</td>
                      <td className="text-right">{previewData.current.risk_shares[bucket].toFixed(2)}%</td>
                      <td className="text-right">{previewData.hypothetical.risk_shares[bucket].toFixed(2)}%</td>
                      <td className={`text-right ${previewData.diff.risk_shares[bucket] >= 0 ? "positive" : "negative"}`.trim()}>
                        {previewData.diff.risk_shares[bucket] >= 0 ? "+" : ""}
                        {previewData.diff.risk_shares[bucket].toFixed(2)}%
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div className="dash-table">
              <h4 className="explore-whatif-table-title">Holding Delta</h4>
              <table>
                <thead>
                  <tr>
                    <th>Account</th>
                    <th>Ticker</th>
                    <th className="text-right">Current</th>
                    <th className="text-right">Hypothetical</th>
                    <th className="text-right">Delta</th>
                  </tr>
                </thead>
                <tbody>
                  {previewData.holding_deltas.length > 0 ? previewData.holding_deltas.map((row) => (
                    <tr key={`${row.account_id}:${row.ticker}`}>
                      <td>{row.account_id}</td>
                      <td>{row.ticker}</td>
                      <td className="text-right">{fmtQty(row.current_quantity)}</td>
                      <td className="text-right">{fmtQty(row.hypothetical_quantity)}</td>
                      <td className={`text-right ${row.delta_quantity >= 0 ? "positive" : "negative"}`.trim()}>
                        {row.delta_quantity >= 0 ? "+" : ""}
                        {fmtQty(row.delta_quantity)}
                      </td>
                    </tr>
                  )) : (
                    <tr>
                      <td colSpan={5} className="holdings-empty-row">No holding delta.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>

          <div className="dash-table">
            <h4 className="explore-whatif-table-title">Key Factor Differences</h4>
            <table>
              <thead>
                <tr>
                  <th>Factor</th>
                  <th className="text-right">Current</th>
                  <th className="text-right">Hypothetical</th>
                  <th className="text-right">Delta</th>
                </tr>
                </thead>
                <tbody>
                  {previewData.diff.factor_deltas[mode].map((row) => (
                  <tr key={row.factor_id}>
                    <td>{shortFactorLabel(row.factor_id, previewData.current.factor_catalog)}</td>
                    <td className="text-right">{row.current.toFixed(mode === "risk_contribution" ? 2 : 4)}{mode === "risk_contribution" ? "%" : ""}</td>
                    <td className="text-right">{row.hypothetical.toFixed(mode === "risk_contribution" ? 2 : 4)}{mode === "risk_contribution" ? "%" : ""}</td>
                    <td className={`text-right ${row.delta >= 0 ? "positive" : "negative"}`.trim()}>
                      {row.delta >= 0 ? "+" : ""}
                      {row.delta.toFixed(mode === "risk_contribution" ? 2 : 4)}
                      {mode === "risk_contribution" ? "%" : ""}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </>
  );
}
