"use client";

import type { Ref } from "react";
import CparExposureBarChart from "@/features/cpar/components/CparExposureBarChart";
import { shortFactorLabel } from "@/lib/factorLabels";
import { describeCparFitStatus, formatCparPackageDate } from "@/lib/cparTruth";
import type { FactorCatalogEntry, FactorExposure } from "@/lib/types/analytics";
import type { CparExploreWhatIfData, CparPortfolioPositionRow } from "@/lib/types/cpar";
import {
  CPAR_EXPLORE_MODES,
  fmtQty,
  type CparExploreMode,
} from "@/features/cpar/components/cparExploreUtils";

function methodLabel(row: CparPortfolioPositionRow | null | undefined): string {
  if (!row) return "—";
  if (row.coverage === "missing_price") return "Missing Price";
  if (row.coverage === "missing_cpar_fit") return "Missing cPAR Fit";
  if (row.coverage === "insufficient_history") return "Insufficient History";
  if (!row.fit_status) return "Package Fit";
  return describeCparFitStatus(row.fit_status).label;
}

function factorCatalog(side: CparExploreWhatIfData["current"]): FactorCatalogEntry[] {
  return (side.factor_catalog || []).map((factor) => ({
    factor_id: factor.factor_id,
    factor_name: factor.label,
    short_label: factor.label,
    family: factor.group === "market" ? "market" : factor.group === "sector" ? "industry" : "style",
    block: factor.group === "market" ? "Market" : factor.group === "sector" ? "Industry" : "Style",
    display_order: factor.display_order,
    active: true,
    method_version: factor.method_version,
  }));
}

function chartFactors(rows: CparExploreWhatIfData["current"]["exposure_modes"]["raw"]): FactorExposure[] {
  return rows.map((row) => ({
    factor_id: row.factor_id,
    value: row.value,
    factor_vol: row.factor_volatility,
    drilldown: row.drilldown.map((drilldown) => ({
      ticker: drilldown.ticker || drilldown.ric || "—",
      weight: drilldown.weight,
      exposure: drilldown.exposure,
      sensitivity: drilldown.sensitivity,
      contribution: drilldown.contribution,
      model_status: "core_estimated",
      exposure_origin: "native",
    })),
  }));
}

export default function CparWhatIfPreviewPanel({
  currentModeFactorOrder,
  mode,
  onModeChange,
  onToggleResults,
  previewData,
  showResults,
  toggleRef,
}: {
  currentModeFactorOrder: string[];
  mode: CparExploreMode;
  onModeChange: (mode: CparExploreMode) => void;
  onToggleResults: () => void;
  previewData: CparExploreWhatIfData | null;
  showResults: boolean;
  toggleRef: Ref<HTMLDivElement>;
}) {
  if (!previewData) {
    return (
      <div className="whatif-results-placeholder">
        Stage one or more trade deltas and run <strong>Preview</strong> to turn this page into the full current-versus-hypothetical cPAR analysis.
      </div>
    );
  }

  const currentCatalog = factorCatalog(previewData.current);
  const hypotheticalCatalog = factorCatalog(previewData.hypothetical);
  const currentFactors = chartFactors(previewData.current.exposure_modes[mode]);
  const hypotheticalFactors = chartFactors(previewData.hypothetical.exposure_modes[mode]);
  const methodByRic = new Map<string, string>();
  for (const row of [...previewData.current.positions, ...previewData.hypothetical.positions]) {
    methodByRic.set(row.ric, methodLabel(row));
  }

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
            Current and hypothetical sides are projected through the active cPAR package dated {formatCparPackageDate(previewData.package_date)}.
            Holdings stay aggregate across all accounts, while staged rows preserve their account-level edits.
          </div>
          <div className="explore-mode-toggle">
            {CPAR_EXPLORE_MODES.map((entry) => (
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
              <span className="explore-compare-label">Current Aggregate Book</span>
              <CparExposureBarChart
                factors={currentFactors}
                mode={mode}
                factorCatalog={currentCatalog}
              />
            </div>
            <div className="chart-card">
              <span className="explore-compare-label">Hypothetical Aggregate Book</span>
              <CparExposureBarChart
                factors={hypotheticalFactors}
                mode={mode}
                orderByFactors={currentModeFactorOrder}
                factorCatalog={hypotheticalCatalog}
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
                    <th>Method</th>
                    <th className="text-right">Current</th>
                    <th className="text-right">Hypothetical</th>
                    <th className="text-right">Delta</th>
                  </tr>
                </thead>
                <tbody>
                  {previewData.holding_deltas.length > 0 ? previewData.holding_deltas.map((row) => (
                    <tr key={`${row.account_id}:${row.ric}`}>
                      <td>{row.account_id}</td>
                      <td>{row.ticker || row.ric}</td>
                      <td>{methodByRic.get(row.ric) || "Package Fit"}</td>
                      <td className="text-right">{fmtQty(row.current_quantity)}</td>
                      <td className="text-right">{fmtQty(row.hypothetical_quantity)}</td>
                      <td className={`text-right ${row.delta_quantity >= 0 ? "positive" : "negative"}`.trim()}>
                        {row.delta_quantity >= 0 ? "+" : ""}
                        {fmtQty(row.delta_quantity)}
                      </td>
                    </tr>
                  )) : (
                    <tr>
                      <td colSpan={6} className="holdings-empty-row">No holding delta.</td>
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
                    <td>{shortFactorLabel(row.factor_id, currentCatalog)}</td>
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
