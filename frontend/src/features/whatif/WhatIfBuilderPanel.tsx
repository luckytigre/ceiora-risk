"use client";

import type { KeyboardEvent } from "react";
import InlineShareDraftEditor from "@/features/holdings/components/InlineShareDraftEditor";
import type { HoldingsAccount, UniverseSearchItem } from "@/lib/types/cuse4";
import {
  fmtMarketValue,
  normalizeTicker,
  parseQty,
  type ExplorePositionSummary,
  type ScenarioDraftRow,
} from "@/features/whatif/whatIfUtils";

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

interface WhatIfBuilderPanelProps {
  accountId: string;
  accountOptions: HoldingsAccount[];
  activeIndex: number;
  applyReady: boolean;
  awaitingRefresh: boolean;
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
  onTickerHover: (ticker: string) => void;
  onTickerKeyDown: (e: KeyboardEvent<HTMLInputElement>) => void;
  onTickerSelect: (ticker: string) => void;
  positionMap: Map<string, ExplorePositionSummary>;
  previewNeedsAttention: boolean;
  previewReady: boolean;
  priceMap: Map<string, number>;
  quantityText: string;
  resultMessage: string;
  scenarioRows: ScenarioDraftRow[];
  searchQuery: string;
  searchLoading: boolean;
  searchSettled: boolean;
  searchResults: UniverseSearchItem[];
  stageReady: boolean;
  updateScenarioRow: (key: string, value: string) => void;
  adjustScenarioRow: (key: string, delta: number) => void;
  removeScenarioRow: (key: string) => void;
}

export default function WhatIfBuilderPanel({
  accountId,
  accountOptions,
  activeIndex,
  applyReady,
  awaitingRefresh,
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
}: WhatIfBuilderPanelProps) {
  return (
    <>
      <div className="whatif-builder-header">
        <div>
          <div className="whatif-builder-kicker">Scenario Lab</div>
          <h2 className="whatif-builder-title">Position What-If</h2>
          <div className="whatif-builder-subtitle">
            Stage trade deltas, preview the hypothetical portfolio, and apply when you want the scenario written through.
          </div>
        </div>
        <div className="whatif-builder-status">{builderStatus}</div>
      </div>

      <div className="whatif-builder-entry">
        <div className="whatif-builder-ticker-wrap">
          <input
            id="whatif-entry-ticker"
            className="explore-input whatif-entry-field whatif-entry-ticker"
            value={searchQuery}
            onChange={(e) => onSearchQueryChange(e.target.value)}
            onKeyDown={onTickerKeyDown}
            onFocus={onTickerFocus}
            onBlur={(e) => onTickerBlur(e.relatedTarget)}
            placeholder="Ticker"
            disabled={controlsBusy}
            title="Search for a ticker — results appear as you type"
          />
          {dropdownOpen && searchQuery.trim().length > 0 && (
            <div className="explore-typeahead whatif-typeahead">
              {searchSettled && searchResults.length > 0 ? searchResults.map((row, index) => {
                const pos = positionMap.get(row.ticker.toUpperCase());
                const tierLabel = row.risk_tier_label || row.model_status || "Unknown Tier";
                const contextLabel = row.quote_source_label || row.trbc_economic_sector_short_abbr || row.trbc_economic_sector_short || "—";
                const whatIfReady = row.whatif_ready !== false;
                return (
                  <button
                    key={row.ric || row.ticker}
                    className={`explore-typeahead-item${index === activeIndex ? " active" : ""}${pos ? " held" : ""}`}
                    onMouseEnter={() => {
                      onSetActiveIndex(index);
                      onTickerHover(row.ticker);
                    }}
                    onClick={() => {
                      if (whatIfReady) onTickerSelect(row.ticker);
                    }}
                    title={row.whatif_ready_detail || row.risk_tier_detail || undefined}
                    disabled={!whatIfReady}
                  >
                    <span className="ticker">{highlightMatch(row.ticker, searchQuery)}</span>
                    <span className="name">{highlightMatch(row.name, searchQuery)}</span>
                    <span className="explore-typeahead-classifications">
                      <span>{tierLabel}</span>
                      {!whatIfReady && (
                        <span className="explore-typeahead-ig">
                          {row.whatif_ready_label || "Not Preview Ready"}
                        </span>
                      )}
                      {(row.quote_source_label || row.trbc_industry_group || contextLabel) && (
                        <span className="explore-typeahead-ig">
                          {row.trbc_industry_group || contextLabel}
                        </span>
                      )}
                    </span>
                    {pos && (
                      <span className="explore-typeahead-held">
                        <span>{pos.shares.toLocaleString()} qty</span>
                        <span>{(pos.weight * 100).toFixed(1)}% wt</span>
                      </span>
                    )}
                    <span className="risk">
                      {typeof row.risk_loading === "number"
                        ? row.risk_loading.toFixed(4)
                        : (row.quote_source_label || "registry")}
                    </span>
                  </button>
                );
              }) : (
                <div className="explore-typeahead-item disabled" aria-live="polite">
                  {searchLoading
                    ? "Searching universe…"
                    : !searchSettled
                      ? "Waiting for current search input…"
                      : "No universe matches yet."}
                </div>
              )}
            </div>
          )}
        </div>

        <input
          id="whatif-entry-qty"
          className="explore-input whatif-entry-field whatif-entry-qty"
          value={quantityText}
          onChange={(e) => onQuantityTextChange(e.target.value)}
          onFocus={() => onTickerBlur(null)}
          inputMode="decimal"
          placeholder="Trade Δ"
          disabled={controlsBusy}
          title="Share delta for the what-if scenario"
        />

        <select
          id="whatif-entry-account"
          className="explore-input whatif-entry-field whatif-entry-account"
          value={accountId}
          onChange={(e) => onAccountIdChange(e.target.value.toLowerCase())}
          onFocus={() => onTickerBlur(null)}
          disabled={controlsBusy}
          title="Account"
        >
          {accountOptions.length === 0 ? (
            <option value="">No accounts</option>
          ) : null}
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
            title="Add this ticker + trade delta as a scenario row for comparison"
          >
            Stage
          </button>
          <button
            className={`btn-action primary${previewNeedsAttention ? " ready ready-preview" : ""}`}
            onClick={onPreview}
            disabled={!previewReady}
            title="Preview risk impact of all staged scenario rows without committing changes"
          >
            {awaitingRefresh ? "RECALC..." : busy ? "..." : "Preview"}
          </button>
          <button
            className={`btn-action apply${applyReady ? " ready ready-apply" : ""}`}
            onClick={onApply}
            disabled={!applyReady}
            title="Write staged rows to holdings, then trigger a full portfolio recalc"
          >
            Apply
          </button>
          {scenarioRows.length > 0 && (
            <button
              className={`btn-action subtle${discardReady ? " ready ready-discard" : ""}`}
              onClick={onDiscard}
              disabled={controlsBusy}
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
                <button className="whatif-builder-pill-remove" onClick={() => removeScenarioRow(row.key)} type="button" disabled={controlsBusy} title="Remove this scenario row">
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
