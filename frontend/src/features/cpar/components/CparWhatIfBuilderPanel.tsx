"use client";

import type { KeyboardEvent } from "react";
import InlineShareDraftEditor from "@/features/holdings/components/InlineShareDraftEditor";
import { canNavigateCparSearchResult, describeCparFitStatus } from "@/lib/cparTruth";
import type { HoldingsAccount } from "@/lib/types/holdings";
import type { CparSearchItem } from "@/lib/types/cpar";
import {
  fmtMarketValue,
  normalizeTicker,
  parseQty,
  type CparExplorePositionSummary,
  type CparExploreScenarioDraftRow,
} from "@/features/cpar/components/cparExploreUtils";

function highlightMatch(text: string, query: string) {
  if (!query) return text;
  const idx = text.toLowerCase().indexOf(query.toLowerCase());
  if (idx === -1) return text;
  return (
    <>
      {text.slice(0, idx)}
      <mark className="explore-highlight">{text.slice(idx, idx + query.length)}</mark>
      {text.slice(idx + query.length)}
    </>
  );
}

interface CparWhatIfBuilderPanelProps {
  accountId: string;
  accountOptions: HoldingsAccount[];
  activeIndex: number;
  applyReady: boolean;
  busy: boolean;
  builderStatus: string;
  controlsBusy: boolean;
  discardReady: boolean;
  dropdownOpen: boolean;
  entryMv: number | null;
  entryPrice: number | null;
  errorMessage: string;
  onAccountIdChange: (value: string) => void;
  onApply: () => void;
  onDiscard: () => void;
  onPreview: () => void;
  onQuantityTextChange: (value: string) => void;
  onSearchQueryChange: (value: string) => void;
  onSetActiveIndex: (value: number) => void;
  onStage: () => void;
  onTickerBlur: (relatedTarget: EventTarget | null) => void;
  onTickerFocus: () => void;
  onTickerHover: (item: CparSearchItem) => void;
  onTickerKeyDown: (e: KeyboardEvent<HTMLInputElement>) => void;
  onTickerSelect: (item: CparSearchItem) => void;
  positionMap: Map<string, CparExplorePositionSummary>;
  previewNeedsAttention: boolean;
  previewReady: boolean;
  priceMap: Map<string, number>;
  quantityText: string;
  resultMessage: string;
  scenarioRows: CparExploreScenarioDraftRow[];
  searchQuery: string;
  searchLoading: boolean;
  searchSettled: boolean;
  searchResults: CparSearchItem[];
  stageReady: boolean;
  updateScenarioRow: (key: string, value: string) => void;
  adjustScenarioRow: (key: string, delta: number) => void;
  removeScenarioRow: (key: string) => void;
}

export default function CparWhatIfBuilderPanel({
  accountId,
  accountOptions,
  activeIndex,
  applyReady,
  busy,
  builderStatus,
  controlsBusy,
  discardReady,
  dropdownOpen,
  entryMv,
  entryPrice,
  errorMessage,
  onAccountIdChange,
  onApply,
  onDiscard,
  onPreview,
  onQuantityTextChange,
  onSearchQueryChange,
  onSetActiveIndex,
  onStage,
  onTickerBlur,
  onTickerFocus,
  onTickerHover,
  onTickerKeyDown,
  onTickerSelect,
  positionMap,
  previewNeedsAttention,
  previewReady,
  priceMap,
  quantityText,
  resultMessage,
  scenarioRows,
  searchQuery,
  searchLoading,
  searchSettled,
  searchResults,
  stageReady,
  updateScenarioRow,
  adjustScenarioRow,
  removeScenarioRow,
}: CparWhatIfBuilderPanelProps) {
  return (
    <>
      <div className="whatif-builder-header">
        <div>
          <div className="whatif-builder-kicker">Scenario Lab</div>
          <h2 className="whatif-builder-title">Position What-If</h2>
          <div className="whatif-builder-subtitle">
            Stage trade deltas, preview the hypothetical aggregate cPAR book, and apply holdings updates from the
            same explore surface.
          </div>
        </div>
        <div className="whatif-builder-status">{builderStatus}</div>
      </div>

      <div className="whatif-builder-entry">
        <div className="whatif-builder-ticker-wrap">
          <input
            id="cpar-explore-entry-ticker"
            className="explore-input whatif-entry-field whatif-entry-ticker"
            value={searchQuery}
            onChange={(e) => onSearchQueryChange(e.target.value)}
            onKeyDown={onTickerKeyDown}
            onFocus={onTickerFocus}
            onBlur={(e) => onTickerBlur(e.relatedTarget)}
            placeholder="Ticker, RIC, or name"
            disabled={controlsBusy}
            spellCheck={false}
            autoComplete="off"
            title="Search the cPAR registry and active package"
          />
          {dropdownOpen && searchQuery.trim().length > 0 && (
            <div className="explore-typeahead whatif-typeahead">
              {searchSettled && searchResults.length > 0 ? searchResults.map((row, index) => {
                const pos = row.ticker ? positionMap.get(normalizeTicker(row.ticker)) : undefined;
                const fit = describeCparFitStatus(row.fit_status);
                const disabled = !canNavigateCparSearchResult(row);
                const tierLabel = row.risk_tier_label || fit.label;
                const contextLabel = row.quote_source_label || row.hq_country_code || row.ric;
                return (
                  <button
                    key={`${row.ric}:${row.ticker || "ric"}`}
                    className={`explore-typeahead-item${index === activeIndex ? " active" : ""}${pos ? " held" : ""}${disabled ? " disabled" : ""}`}
                    onMouseEnter={() => {
                      onSetActiveIndex(index);
                      onTickerHover(row);
                    }}
                    onClick={() => onTickerSelect(row)}
                    type="button"
                    disabled={disabled}
                    title={
                      disabled
                        ? "Ticker required for cPAR explore quote selection"
                        : (row.risk_tier_detail || row.scenario_stage_detail || undefined)
                    }
                  >
                    <span className="ticker">{highlightMatch(row.ticker || row.ric, searchQuery)}</span>
                    <span className="name">{highlightMatch(row.display_name || row.ric, searchQuery)}</span>
                    <span className="explore-typeahead-classifications">
                      <span>{tierLabel}</span>
                      <span className="explore-typeahead-ig">{contextLabel}</span>
                    </span>
                    {pos && (
                      <span className="explore-typeahead-held">
                        <span>{pos.shares.toLocaleString()} qty</span>
                        <span>{(pos.weight * 100).toFixed(1)}% wt</span>
                      </span>
                    )}
                    <span className="risk">
                      {row.scenario_stage_supported === false ? "quote only" : row.ric}
                    </span>
                  </button>
                );
              }) : (
                <div className="explore-typeahead-item disabled" aria-live="polite">
                  {searchLoading
                    ? "Searching cPAR registry and active package…"
                    : !searchSettled
                      ? "Waiting for current search input…"
                      : "No cPAR registry matches yet."}
                </div>
              )}
            </div>
          )}
        </div>

        <input
          id="cpar-explore-entry-qty"
          className="explore-input whatif-entry-field whatif-entry-qty"
          value={quantityText}
          onChange={(e) => onQuantityTextChange(e.target.value)}
          onFocus={() => onTickerBlur(null)}
          inputMode="decimal"
          placeholder="Trade Δ"
          disabled={controlsBusy}
          title="Share delta for the explore what-if scenario"
        />

        <select
          id="cpar-explore-entry-account"
          className="explore-input whatif-entry-field whatif-entry-account"
          value={accountId}
          onChange={(e) => onAccountIdChange(e.target.value.toLowerCase())}
          onFocus={() => onTickerBlur(null)}
          disabled={controlsBusy}
          title="Account"
        >
          {accountOptions.length === 0 ? <option value="">No accounts</option> : null}
          {accountOptions.map((account) => (
            <option key={account.account_id} value={account.account_id}>
              {account.account_name || account.account_id}
            </option>
          ))}
        </select>

        <div className="whatif-builder-sep" />

        <div className="whatif-builder-readout">
          <div className="whatif-builder-readout-item">
            <span className="whatif-builder-readout-label">Price</span>
            <span className="whatif-builder-readout-value">
              {entryPrice != null ? `$${entryPrice.toFixed(2)}` : "—"}
            </span>
          </div>
          <div className="whatif-builder-readout-item">
            <span className="whatif-builder-readout-label">Mkt Val</span>
            <span className={`whatif-builder-readout-value${entryMv != null ? (entryMv >= 0 ? " positive" : " negative") : ""}`}>
              {entryMv != null ? fmtMarketValue(entryMv) : "—"}
            </span>
          </div>
        </div>

        <div className="whatif-builder-sep" />

        <div className="whatif-builder-actions">
          <button
            className={`btn-action${stageReady ? " ready ready-stage" : ""}`}
            onClick={onStage}
            disabled={controlsBusy}
            type="button"
            title="Add this ticker + trade delta as a scenario row for comparison"
          >
            Stage
          </button>
          <button
            className={`btn-action primary${previewNeedsAttention ? " ready ready-preview" : ""}`}
            onClick={onPreview}
            disabled={!previewReady}
            type="button"
            title="Preview risk impact of all staged scenario rows without committing changes"
          >
            {busy ? "..." : "Preview"}
          </button>
          <button
            className={`btn-action apply${applyReady ? " ready ready-apply" : ""}`}
            onClick={onApply}
            disabled={!applyReady}
            type="button"
            title="Write staged rows to your holdings"
          >
            Apply
          </button>
          {scenarioRows.length > 0 && (
            <button
              className={`btn-action subtle${discardReady ? " ready ready-discard" : ""}`}
              onClick={onDiscard}
              disabled={controlsBusy}
              type="button"
              title="Clear all staged scenario rows and preview data"
            >
              Discard
            </button>
          )}
        </div>
      </div>

      {scenarioRows.length > 0 && (
        <div className="whatif-builder-queue">
          <span className="whatif-builder-queue-label">Staged</span>
          {scenarioRows.map((row) => {
            const rowPrice = priceMap.get(normalizeTicker(row.ticker));
            const rowQty = parseQty(row.quantity_text);
            const rowMv = rowPrice != null && rowQty != null ? rowQty * rowPrice : null;
            return (
              <div key={row.key} className="whatif-builder-pill">
                <div className="whatif-builder-pill-meta">
                  <span className="whatif-builder-pill-ticker">{row.ticker}</span>
                  <span className="whatif-builder-pill-account">{row.account_id}</span>
                </div>
                <InlineShareDraftEditor
                  quantityText={row.quantity_text}
                  disabled={controlsBusy}
                  draftActive
                  invalid={rowQty === null}
                  titleBase={`${row.ticker} ${row.account_id}`}
                  onQuantityTextChange={(value) => updateScenarioRow(row.key, value)}
                  onStep={(delta) => adjustScenarioRow(row.key, delta)}
                />
                {rowMv != null && (
                  <span className="whatif-builder-pill-mv">{fmtMarketValue(rowMv)}</span>
                )}
                <button
                  className="whatif-builder-pill-remove"
                  onClick={() => removeScenarioRow(row.key)}
                  type="button"
                  disabled={controlsBusy}
                  title="Remove this scenario row"
                >
                  &times;
                </button>
              </div>
            );
          })}
        </div>
      )}

      {(resultMessage || errorMessage) && (
        <div className={`whatif-builder-feedback${errorMessage ? " error" : ""}`}>
          {errorMessage || resultMessage}
        </div>
      )}
    </>
  );
}
