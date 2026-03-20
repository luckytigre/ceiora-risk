"use client";

import { useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import InlineShareDraftEditor from "@/features/holdings/components/InlineShareDraftEditor";
import { useDebouncedValue } from "@/hooks/useDebouncedValue";
import { useCparSearch } from "@/hooks/useCparApi";
import {
  canNavigateCparSearchResult,
  describeCparFitStatus,
  readCparError,
} from "@/lib/cparTruth";
import type { CparSearchItem } from "@/lib/types/cpar";

export interface CparDraftScenarioRow {
  key: string;
  ric: string;
  ticker: string | null;
  display_name: string | null;
  fit_status: CparSearchItem["fit_status"];
  hq_country_code: string | null;
  quantity_text: string;
}

function parseQuantityDelta(value: string): number | null {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || Math.abs(parsed) <= 1e-12) return null;
  return parsed;
}

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

export default function CparPortfolioWhatIfBuilder({
  resetKey,
  scenarioRows,
  hasInvalidScenarioRows,
  onStageRow,
  onUpdateScenarioRow,
  onAdjustScenarioRow,
  onRemoveScenarioRow,
  onClearScenarioRows,
}: {
  resetKey: string;
  scenarioRows: CparDraftScenarioRow[];
  hasInvalidScenarioRows: boolean;
  onStageRow: (item: CparSearchItem, quantityDelta: number) => string | null;
  onUpdateScenarioRow: (ric: string, quantityText: string) => void;
  onAdjustScenarioRow: (ric: string, delta: number) => void;
  onRemoveScenarioRow: (ric: string) => void;
  onClearScenarioRows: () => void;
}) {
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedScenarioItem, setSelectedScenarioItem] = useState<CparSearchItem | null>(null);
  const [quantityDeltaInput, setQuantityDeltaInput] = useState("10");
  const [builderMessage, setBuilderMessage] = useState<string | null>(null);
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);
  const [tickerFocused, setTickerFocused] = useState(false);
  const searchWrapRef = useRef<HTMLDivElement>(null);
  const debouncedSearchQuery = useDebouncedValue(searchQuery, 220);

  const { data: searchData, error: searchError, isLoading: searchLoading } = useCparSearch(debouncedSearchQuery, 8);
  const searchState = searchError ? readCparError(searchError) : null;
  const searchResults = searchData?.results ?? [];
  const selectableCount = useMemo(
    () => searchResults.filter((item) => canNavigateCparSearchResult(item)).length,
    [searchResults],
  );
  const parsedQuantityDelta = parseQuantityDelta(quantityDeltaInput);
  const builderStatus = scenarioRows.length > 0 ? `${scenarioRows.length} staged` : "Preview-only";

  useEffect(() => {
    setSearchQuery("");
    setSelectedScenarioItem(null);
    setQuantityDeltaInput("10");
    setBuilderMessage(null);
    setDropdownOpen(false);
    setActiveIndex(-1);
    setTickerFocused(false);
  }, [resetKey]);

  useEffect(() => {
    if (tickerFocused && searchQuery.trim().length > 0 && searchResults.length > 0) {
      setDropdownOpen(true);
      setActiveIndex(-1);
    } else {
      setDropdownOpen(false);
    }
  }, [tickerFocused, searchQuery, searchResults.length]);

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (searchWrapRef.current && !searchWrapRef.current.contains(event.target as Node)) {
        setDropdownOpen(false);
        setActiveIndex(-1);
        setTickerFocused(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  function selectSearchResult(item: CparSearchItem) {
    if (!canNavigateCparSearchResult(item)) return;
    setSelectedScenarioItem(item);
    setSearchQuery(item.ticker || item.ric);
    setDropdownOpen(false);
    setTickerFocused(false);
    setActiveIndex(-1);
    setBuilderMessage(null);
  }

  function handleSearchKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (!dropdownOpen || searchResults.length === 0) {
      return;
    }
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setActiveIndex((current) => (current < searchResults.length - 1 ? current + 1 : 0));
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      setActiveIndex((current) => (current > 0 ? current - 1 : searchResults.length - 1));
    } else if (event.key === "Enter") {
      event.preventDefault();
      if (activeIndex >= 0 && activeIndex < searchResults.length) {
        selectSearchResult(searchResults[activeIndex]);
      }
    } else if (event.key === "Escape") {
      setDropdownOpen(false);
      setActiveIndex(-1);
    }
  }

  function handleStageRow() {
    if (!selectedScenarioItem || !canNavigateCparSearchResult(selectedScenarioItem)) {
      setBuilderMessage("Choose an active-package search hit with a ticker before staging a what-if row.");
      return;
    }
    if (parsedQuantityDelta == null) {
      setBuilderMessage("Enter a non-zero finite share delta before staging a what-if row.");
      return;
    }

    const stageError = onStageRow(selectedScenarioItem, parsedQuantityDelta);
    if (stageError) {
      setBuilderMessage(stageError);
      return;
    }

    setQuantityDeltaInput("10");
    setSelectedScenarioItem(null);
    setSearchQuery("");
    setBuilderMessage(null);
    setDropdownOpen(false);
    setActiveIndex(-1);
  }

  return (
    <section className="chart-card" data-testid="cpar-portfolio-whatif-builder">
      <div className="whatif-builder" ref={searchWrapRef}>
        <div className="whatif-builder-header">
          <div>
            <div className="whatif-builder-kicker">Scenario Lab</div>
            <h2 className="whatif-builder-title">Account What-If</h2>
            <div className="whatif-builder-subtitle">
              Stage package-scoped share deltas, preview the hypothetical account hedge, and keep the flow read-only.
              This uses the active cPAR package plus the selected live holdings account without inheriting cUSE4
              apply semantics.
            </div>
          </div>
          <div className="whatif-builder-status">{builderStatus}</div>
        </div>

        <div className="whatif-builder-entry">
          <div className="whatif-builder-ticker-wrap">
            <input
              id="cpar-whatif-search"
              className="explore-input whatif-entry-field whatif-entry-ticker"
              data-testid="cpar-search-input"
              value={searchQuery}
              onChange={(event) => {
                setSearchQuery(event.target.value);
                setSelectedScenarioItem(null);
                setBuilderMessage(null);
              }}
              onKeyDown={handleSearchKeyDown}
              onFocus={() => {
                setTickerFocused(true);
                if (searchQuery.trim().length > 0 && searchResults.length > 0) {
                  setDropdownOpen(true);
                }
              }}
              onBlur={(event) => {
                if (event.relatedTarget && searchWrapRef.current?.contains(event.relatedTarget as Node)) return;
                setTickerFocused(false);
                setDropdownOpen(false);
                setActiveIndex(-1);
              }}
              placeholder="Ticker, RIC, or name"
              autoComplete="off"
              spellCheck={false}
            />
            {dropdownOpen && searchResults.length > 0 ? (
              <div className="explore-typeahead whatif-typeahead" data-testid="cpar-search-results">
                {searchResults.map((item, index) => {
                  const disabled = !canNavigateCparSearchResult(item);
                  const fit = describeCparFitStatus(item.fit_status);
                  return (
                    <button
                      key={item.ric}
                      type="button"
                      className={`explore-typeahead-item${index === activeIndex ? " active" : ""}${disabled ? " disabled" : ""}`}
                      onMouseEnter={() => setActiveIndex(index)}
                      onMouseDown={(event) => event.preventDefault()}
                      onClick={() => selectSearchResult(item)}
                      disabled={disabled}
                      title={disabled ? "Ticker required for account what-if staging" : undefined}
                    >
                      <span className="ticker">{highlightMatch(item.ticker || item.ric, searchQuery)}</span>
                      <span className="name">{highlightMatch(item.display_name || item.ric, searchQuery)}</span>
                      <span className="explore-typeahead-classifications">
                        <span>{fit.label}</span>
                        <span className="explore-typeahead-ig">
                          {disabled ? "Ticker required" : item.hq_country_code || "—"}
                        </span>
                      </span>
                      <span className="risk">{item.ric}</span>
                    </button>
                  );
                })}
              </div>
            ) : null}
          </div>

          <input
            id="cpar-whatif-qty"
            className="explore-input whatif-entry-field whatif-entry-qty"
            data-testid="cpar-whatif-quantity-input"
            type="number"
            step="0.01"
            value={quantityDeltaInput}
            onChange={(event) => {
              setQuantityDeltaInput(event.target.value);
              setBuilderMessage(null);
            }}
            onFocus={() => {
              setDropdownOpen(false);
              setActiveIndex(-1);
            }}
            placeholder="Trade Δ"
          />

          <div className="whatif-builder-sep" />

          <div className="whatif-builder-readout">
            <div className="whatif-builder-readout-item">
              <span className="whatif-builder-readout-label">Selected</span>
              <span className="whatif-builder-readout-value">
                {selectedScenarioItem?.ticker || selectedScenarioItem?.ric || "—"}
              </span>
            </div>
            <div className="whatif-builder-readout-item">
              <span className="whatif-builder-readout-label">RIC</span>
              <span className="whatif-builder-readout-value">{selectedScenarioItem?.ric || "—"}</span>
            </div>
            <div className="whatif-builder-readout-item">
              <span className="whatif-builder-readout-label">Fit</span>
              <span className="whatif-builder-readout-value">
                {selectedScenarioItem ? describeCparFitStatus(selectedScenarioItem.fit_status).label : "—"}
              </span>
            </div>
          </div>

          <div className="whatif-builder-sep" />

          <div className="whatif-builder-actions">
            <button
              type="button"
              className={`btn-action${selectedScenarioItem && parsedQuantityDelta != null ? " ready ready-stage" : ""}`}
              data-testid="cpar-whatif-add-btn"
              onClick={handleStageRow}
            >
              Stage
            </button>
            {scenarioRows.length > 0 ? (
              <button
                type="button"
                className="btn-action subtle"
                onClick={() => {
                  onClearScenarioRows();
                  setBuilderMessage(null);
                }}
              >
                Clear
              </button>
            ) : null}
          </div>
        </div>

        {scenarioRows.length > 0 ? (
          <div className="whatif-builder-queue">
            <span className="whatif-builder-queue-label">Staged</span>
            {scenarioRows.map((row) => (
              <div key={row.key} className="whatif-builder-pill">
                <div className="whatif-builder-pill-meta">
                  <span className="whatif-builder-pill-ticker">{row.ticker || row.ric}</span>
                  <span className="whatif-builder-pill-account">{row.hq_country_code || row.ric}</span>
                </div>
                <InlineShareDraftEditor
                  quantityText={row.quantity_text}
                  draftActive
                  invalid={parseQuantityDelta(row.quantity_text) == null}
                  titleBase={row.ticker || row.ric}
                  onQuantityTextChange={(value) => onUpdateScenarioRow(row.ric, value)}
                  onStep={(delta) => onAdjustScenarioRow(row.ric, delta)}
                />
                <span className="whatif-builder-pill-mv">{describeCparFitStatus(row.fit_status).label}</span>
                <button
                  className="whatif-builder-pill-remove"
                  onClick={() => onRemoveScenarioRow(row.ric)}
                  type="button"
                  title={`Remove ${row.ticker || row.ric}`}
                >
                  &times;
                </button>
              </div>
            ))}
          </div>
        ) : null}

        {searchQuery.trim().length > 0 && searchState ? (
          <div className="whatif-builder-feedback error">
            {searchState.kind === "not_ready" ? "Package not ready." : "Search unavailable."} {searchState.message}
          </div>
        ) : searchQuery.trim().length > 0 && searchLoading && !searchData ? (
          <div className="whatif-builder-feedback">Searching the active cPAR package…</div>
        ) : searchQuery.trim().length > 0 && searchResults.length === 0 && !searchState ? (
          <div className="whatif-builder-feedback">No active-package results matched this search.</div>
        ) : searchQuery.trim().length > 0 && searchResults.length > 0 && selectableCount === 0 && !searchState ? (
          <div className="whatif-builder-feedback">Active-package matches found, but ticker is required to stage rows.</div>
        ) : null}

        <div className="whatif-builder-feedback">
          Positive values add shares, negative values reduce shares, and the preview remains read-only.
        </div>
        {builderMessage ? (
          <div className="whatif-builder-feedback error">{builderMessage}</div>
        ) : hasInvalidScenarioRows ? (
          <div className="whatif-builder-feedback error">
            One or more staged rows have an invalid or zero share delta. Fix the draft before the hypothetical
            preview can recompute.
          </div>
        ) : null}
      </div>
    </section>
  );
}
