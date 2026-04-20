"use client";

import { useMemo, type Ref } from "react";
import ExposureBarChart from "@/features/cuse4/components/ExposureBarChart";
import MethodLabel, { type MethodLabelTone } from "@/components/MethodLabel";
import { compareNumber, compareText, useSortableRows } from "@/hooks/useSortableRows";
import { shortFactorLabel } from "@/lib/factorLabels";
import type { WhatIfPreviewData } from "@/lib/types/cuse4";
import { formatAsOfDate } from "@/lib/cuse4Truth";
import { exposureMethodDisplayLabel, exposureMethodTone } from "@/lib/exposureOrigin";
import { fmtQty, WHAT_IF_MODES, type WhatIfMode } from "@/features/whatif/whatIfUtils";

type RiskShareSortKey = "bucket" | "current" | "hypothetical" | "delta";
type HoldingDeltaSortKey = "account" | "ticker" | "method" | "current" | "hypothetical" | "delta";
type FactorDeltaSortKey = "factor" | "current" | "hypothetical" | "delta";
type HoldingDeltaRow = WhatIfPreviewData["holding_deltas"][number];
type FactorDeltaRow = WhatIfPreviewData["diff"]["factor_deltas"][WhatIfMode][number];

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
  const previewScope = previewData.preview_scope;
  const previewAccountIds = previewScope?.account_ids ?? [];
  const previewScopeLabel = previewAccountIds.length <= 1 ? "staged account" : "staged accounts";
  const modeLabel = WHAT_IF_MODES.find((entry) => entry.key === mode)?.label ?? mode;
  const currentSideDescription = servedModelPreview
    ? `Current side = live holdings for the ${previewScopeLabel} projected through the current served model snapshot`
    : `Current side = live holdings for the ${previewScopeLabel} projected through current published loadings plus live risk-cache fallback`;
  const methodByAccountTicker = new Map<string, { label: string; tone: MethodLabelTone }>();
  const methodByTicker = new Map<string, { label: string; tone: MethodLabelTone }>();
  const collectMethod = (
    accountId: string | null | undefined,
    ticker: string | null | undefined,
    method: { label: string; tone: MethodLabelTone },
  ) => {
    const tickerKey = String(ticker || "").trim().toUpperCase();
    if (!tickerKey || method.label === "\u2014") return;
    const accountKey = String(accountId || "").trim().toUpperCase();
    methodByTicker.set(tickerKey, methodByTicker.get(tickerKey) || method);
    if (accountKey) {
      methodByAccountTicker.set(`${accountKey}:${tickerKey}`, method);
    }
  };
  for (const pos of [...previewData.current.positions, ...previewData.hypothetical.positions]) {
    const method = {
      label: exposureMethodDisplayLabel(pos.exposure_origin, pos.model_status),
      tone: exposureMethodTone(pos.exposure_origin, pos.model_status),
    };
    collectMethod(pos.account, pos.ticker, method);
  }
  const methodForHoldingDelta = (accountId: string, ticker: string) => {
    const accountKey = String(accountId || "").trim().toUpperCase();
    const tickerKey = String(ticker || "").trim().toUpperCase();
    return (
      methodByAccountTicker.get(`${accountKey}:${tickerKey}`)
      || methodByTicker.get(tickerKey)
      || { label: "\u2014", tone: "neutral" as const }
    );
  };
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
  const holdingDeltaComparators = useMemo<Record<HoldingDeltaSortKey, (left: HoldingDeltaRow, right: HoldingDeltaRow) => number>>(
    () => ({
      account: (left, right) => compareText(left.account_id, right.account_id),
      ticker: (left, right) => compareText(left.ticker, right.ticker),
      method: (left, right) => compareText(methodForHoldingDelta(left.account_id, left.ticker).label, methodForHoldingDelta(right.account_id, right.ticker).label),
      current: (left, right) => compareNumber(left.current_quantity, right.current_quantity),
      hypothetical: (left, right) => compareNumber(left.hypothetical_quantity, right.hypothetical_quantity),
      delta: (left, right) => compareNumber(left.delta_quantity, right.delta_quantity),
    }),
    [previewData.holding_deltas],
  );
  const factorDeltaComparators = useMemo<Record<FactorDeltaSortKey, (left: FactorDeltaRow, right: FactorDeltaRow) => number>>(
    () => ({
      factor: (left, right) => compareText(shortFactorLabel(left.factor_id, previewData.current.factor_catalog), shortFactorLabel(right.factor_id, previewData.current.factor_catalog)),
      current: (left, right) => compareNumber(left.current, right.current),
      hypothetical: (left, right) => compareNumber(left.hypothetical, right.hypothetical),
      delta: (left, right) => compareNumber(left.delta, right.delta),
    }),
    [mode, previewData.current.factor_catalog, previewData.diff.factor_deltas],
  );
  const { sortedRows: sortedRiskShareRows, handleSort: handleRiskShareSort, arrow: riskShareArrow } = useSortableRows<
    (typeof riskShareRows)[number],
    RiskShareSortKey
  >({
    rows: riskShareRows,
    comparators: riskShareComparators,
  });
  const { sortedRows: sortedHoldingDeltas, handleSort: handleHoldingDeltaSort, arrow: holdingDeltaArrow } = useSortableRows<
    HoldingDeltaRow,
    HoldingDeltaSortKey
  >({
    rows: previewData.holding_deltas,
    comparators: holdingDeltaComparators,
  });
  const { sortedRows: sortedFactorDeltas, handleSort: handleFactorDeltaSort, arrow: factorDeltaArrow } = useSortableRows<
    FactorDeltaRow,
    FactorDeltaSortKey
  >({
    rows: previewData.diff.factor_deltas[mode],
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
            {currentSideDescription}
            {previewData.serving_snapshot?.snapshot_id ? ` ${previewData.serving_snapshot.snapshot_id}` : ""}.
            {previewAccountIds.length > 0
              ? ` Preview scope: ${previewAccountIds.join(", ")}.`
              : ""}
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
              <span className="explore-compare-label">Current Staged-Account Book ({modeLabel})</span>
              <ExposureBarChart
                factors={previewData.current.exposure_modes[mode]}
                mode={mode}
                factorCatalog={previewData.current.factor_catalog}
              />
            </div>
            <div className="chart-card">
              <span className="explore-compare-label">Hypothetical Staged-Account Book ({modeLabel})</span>
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
              <h4 className="explore-whatif-table-title">Risk Share Delta (% of total risk)</h4>
              <table>
                <thead>
                  <tr>
                    <th onClick={() => handleRiskShareSort("bucket")}>Bucket{riskShareArrow("bucket")}</th>
                    <th className="text-right" onClick={() => handleRiskShareSort("current")}>Current Share{riskShareArrow("current")}</th>
                    <th className="text-right" onClick={() => handleRiskShareSort("hypothetical")}>Hypothetical Share{riskShareArrow("hypothetical")}</th>
                    <th className="text-right" onClick={() => handleRiskShareSort("delta")}>Share Delta{riskShareArrow("delta")}</th>
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
                    <tr key={`${row.account_id}:${row.ticker}`}>
                      <td>{row.account_id}</td>
                      <td>{row.ticker}</td>
                      <td>
                        <MethodLabel
                          label={methodForHoldingDelta(row.account_id, row.ticker).label}
                          tone={methodForHoldingDelta(row.account_id, row.ticker).tone}
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
