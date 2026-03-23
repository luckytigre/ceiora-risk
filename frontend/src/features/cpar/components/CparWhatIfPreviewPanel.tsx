"use client";

import { useMemo, type Ref } from "react";
import CparExposureBarChart from "@/features/cpar/components/CparExposureBarChart";
import MethodLabel, { type MethodLabelTone } from "@/components/MethodLabel";
import { compareNumber, compareText, useSortableRows } from "@/hooks/useSortableRows";
import { shortFactorLabel } from "@/lib/factorLabels";
import { describeCparPositionMethod, formatCparPackageDate } from "@/lib/cparTruth";
import type { FactorCatalogEntry, FactorExposure } from "@/lib/types/analytics";
import type { CparExploreWhatIfData } from "@/lib/types/cpar";
import {
  CPAR_EXPLORE_MODES,
  fmtQty,
  type CparExploreMode,
} from "@/features/cpar/components/cparExploreUtils";

type RiskShareSortKey = "bucket" | "current" | "hypothetical" | "delta";
type HoldingDeltaSortKey = "account" | "ticker" | "method" | "current" | "hypothetical" | "delta";
type FactorDeltaSortKey = "factor" | "current" | "hypothetical" | "delta";
type CparHoldingDeltaRow = CparExploreWhatIfData["holding_deltas"][number];
type CparFactorDeltaRow = CparExploreWhatIfData["diff"]["factor_deltas"][CparExploreMode][number];

function factorCatalog(side: CparExploreWhatIfData["current"]): FactorCatalogEntry[] {
  return (side.factor_catalog || []).map((factor) => ({
    factor_id: factor.factor_id,
    factor_name: factor.label,
    short_label: shortFactorLabel(factor.label),
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
  const currentExposureModes = previewData.current.display_exposure_modes ?? previewData.current.exposure_modes;
  const hypotheticalExposureModes = previewData.hypothetical.display_exposure_modes ?? previewData.hypothetical.exposure_modes;
  const factorDeltaModes = previewData.diff.display_factor_deltas ?? previewData.diff.factor_deltas;
  const currentFactors = chartFactors(currentExposureModes[mode]);
  const hypotheticalFactors = chartFactors(hypotheticalExposureModes[mode]);
  const methodByRic = new Map<string, { label: string; tone: MethodLabelTone }>();
  for (const row of [...previewData.current.positions, ...previewData.hypothetical.positions]) {
    const descriptor = describeCparPositionMethod(row.coverage, row.fit_status);
    methodByRic.set(row.ric, { label: descriptor.label, tone: descriptor.tone });
  }
  const riskShareRows = useMemo(
    () => (["market", "industry", "style", "idio"] as const).map((bucket) => ({
      bucket,
      current: previewData.current.risk_shares[bucket],
      hypothetical: previewData.hypothetical.risk_shares[bucket],
      delta: previewData.diff.risk_shares[bucket],
    })),
    [previewData.current.risk_shares, previewData.diff.risk_shares, previewData.hypothetical.risk_shares],
  );
  const riskShareComparators = useMemo<Record<RiskShareSortKey, (left: (typeof riskShareRows)[number], right: (typeof riskShareRows)[number]) => number>>(
    () => ({
      bucket: (left, right) => compareText(left.bucket, right.bucket),
      current: (left, right) => compareNumber(left.current, right.current),
      hypothetical: (left, right) => compareNumber(left.hypothetical, right.hypothetical),
      delta: (left, right) => compareNumber(left.delta, right.delta),
    }),
    [],
  );
  const holdingDeltaComparators = useMemo<Record<HoldingDeltaSortKey, (left: CparHoldingDeltaRow, right: CparHoldingDeltaRow) => number>>(
    () => ({
      account: (left, right) => compareText(left.account_id, right.account_id),
      ticker: (left, right) => compareText(left.ticker || left.ric, right.ticker || right.ric),
      method: (left, right) => compareText(methodByRic.get(left.ric)?.label || "Package Fit", methodByRic.get(right.ric)?.label || "Package Fit"),
      current: (left, right) => compareNumber(left.current_quantity, right.current_quantity),
      hypothetical: (left, right) => compareNumber(left.hypothetical_quantity, right.hypothetical_quantity),
      delta: (left, right) => compareNumber(left.delta_quantity, right.delta_quantity),
    }),
    [methodByRic, previewData.holding_deltas],
  );
  const factorDeltaComparators = useMemo<Record<FactorDeltaSortKey, (left: CparFactorDeltaRow, right: CparFactorDeltaRow) => number>>(
    () => ({
      factor: (left, right) => compareText(shortFactorLabel(left.factor_id, currentCatalog), shortFactorLabel(right.factor_id, currentCatalog)),
      current: (left, right) => compareNumber(left.current, right.current),
      hypothetical: (left, right) => compareNumber(left.hypothetical, right.hypothetical),
      delta: (left, right) => compareNumber(left.delta, right.delta),
    }),
    [currentCatalog, factorDeltaModes, mode],
  );
  const { sortedRows: sortedRiskShareRows, handleSort: handleRiskShareSort, arrow: riskShareArrow } = useSortableRows<
    (typeof riskShareRows)[number],
    RiskShareSortKey
  >({
    rows: riskShareRows,
    comparators: riskShareComparators,
  });
  const { sortedRows: sortedHoldingDeltas, handleSort: handleHoldingDeltaSort, arrow: holdingDeltaArrow } = useSortableRows<
    CparHoldingDeltaRow,
    HoldingDeltaSortKey
  >({
    rows: previewData.holding_deltas,
    comparators: holdingDeltaComparators,
  });
  const { sortedRows: sortedFactorDeltas, handleSort: handleFactorDeltaSort, arrow: factorDeltaArrow } = useSortableRows<
    CparFactorDeltaRow,
    FactorDeltaSortKey
  >({
    rows: factorDeltaModes[mode],
    comparators: factorDeltaComparators,
  });

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
                    <th onClick={() => handleRiskShareSort("bucket")}>Bucket{riskShareArrow("bucket")}</th>
                    <th className="text-right" onClick={() => handleRiskShareSort("current")}>Current{riskShareArrow("current")}</th>
                    <th className="text-right" onClick={() => handleRiskShareSort("hypothetical")}>Hypothetical{riskShareArrow("hypothetical")}</th>
                    <th className="text-right" onClick={() => handleRiskShareSort("delta")}>Delta{riskShareArrow("delta")}</th>
                  </tr>
                </thead>
                <tbody>
                  {sortedRiskShareRows.map((row) => (
                    <tr key={row.bucket}>
                      <td>{row.bucket}</td>
                      <td className="text-right">{row.current.toFixed(2)}%</td>
                      <td className="text-right">{row.hypothetical.toFixed(2)}%</td>
                      <td className={`text-right ${row.delta >= 0 ? "positive" : "negative"}`.trim()}>
                        {row.delta >= 0 ? "+" : ""}
                        {row.delta.toFixed(2)}%
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
                    <th onClick={() => handleHoldingDeltaSort("account")}>Account{holdingDeltaArrow("account")}</th>
                    <th onClick={() => handleHoldingDeltaSort("ticker")}>Ticker{holdingDeltaArrow("ticker")}</th>
                    <th onClick={() => handleHoldingDeltaSort("method")}>Method{holdingDeltaArrow("method")}</th>
                    <th className="text-right" onClick={() => handleHoldingDeltaSort("current")}>Current{holdingDeltaArrow("current")}</th>
                    <th className="text-right" onClick={() => handleHoldingDeltaSort("hypothetical")}>Hypothetical{holdingDeltaArrow("hypothetical")}</th>
                    <th className="text-right" onClick={() => handleHoldingDeltaSort("delta")}>Delta{holdingDeltaArrow("delta")}</th>
                  </tr>
                </thead>
                <tbody>
                  {sortedHoldingDeltas.length > 0 ? sortedHoldingDeltas.map((row) => (
                    <tr key={`${row.account_id}:${row.ric}`}>
                      <td>{row.account_id}</td>
                      <td>{row.ticker || row.ric}</td>
                      <td>
                        <MethodLabel
                          label={methodByRic.get(row.ric)?.label || "Package Fit"}
                          tone={methodByRic.get(row.ric)?.tone || "success"}
                        />
                      </td>
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
                  <th onClick={() => handleFactorDeltaSort("factor")}>Factor{factorDeltaArrow("factor")}</th>
                  <th className="text-right" onClick={() => handleFactorDeltaSort("current")}>Current{factorDeltaArrow("current")}</th>
                  <th className="text-right" onClick={() => handleFactorDeltaSort("hypothetical")}>Hypothetical{factorDeltaArrow("hypothetical")}</th>
                  <th className="text-right" onClick={() => handleFactorDeltaSort("delta")}>Delta{factorDeltaArrow("delta")}</th>
                </tr>
              </thead>
              <tbody>
                {sortedFactorDeltas.map((row) => (
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
