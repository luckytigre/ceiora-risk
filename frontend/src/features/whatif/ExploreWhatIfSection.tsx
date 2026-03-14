"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { mutate } from "swr";
import ExposureBarChart from "@/components/ExposureBarChart";
import InlineShareDraftEditor from "@/features/holdings/components/InlineShareDraftEditor";
import {
  applyPortfolioWhatIf,
  previewPortfolioWhatIf,
  triggerServeRefresh,
  useHoldingsAccounts,
  useHoldingsPositions,
} from "@/hooks/useApi";
import { ApiError, apiPath } from "@/lib/api";
import type {
  UniverseSearchItem,
  UniverseTickerItem,
  WhatIfPreviewData,
  WhatIfScenarioRow,
} from "@/lib/types";
import { factorTier, shortFactorLabel } from "@/lib/factorLabels";

type WhatIfMode = "raw" | "sensitivity" | "risk_contribution";

interface ScenarioDraftRow {
  key: string;
  account_id: string;
  ticker: string;
  quantity_text: string;
  source: string;
}

const MODES: Array<{ key: WhatIfMode; label: string }> = [
  { key: "raw", label: "Exposure" },
  { key: "sensitivity", label: "Sensitivity" },
  { key: "risk_contribution", label: "Risk Contrib" },
];

const STRICT_QTY_RE = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/;

function normalizeAccountId(raw: string | null | undefined): string {
  return String(raw || "").trim().toLowerCase();
}

function normalizeTicker(raw: string | null | undefined): string {
  return String(raw || "").trim().toUpperCase();
}

function scenarioKey(accountId: string, ticker: string): string {
  return `${normalizeAccountId(accountId)}::${normalizeTicker(ticker)}`;
}

function parseQty(raw: string): number | null {
  const clean = String(raw || "").trim().replaceAll(",", "");
  if (!clean) return null;
  if (!STRICT_QTY_RE.test(clean)) return null;
  const out = Number.parseFloat(clean);
  return Number.isFinite(out) ? out : null;
}

function fmtQty(n: number): string {
  if (!Number.isFinite(n)) return "—";
  const rounded = Number(n.toFixed(6));
  if (Number.isInteger(rounded)) return `${rounded}`;
  return `${rounded}`;
}

function fmtMarketValue(n: number): string {
  if (!Number.isFinite(n)) return "—";
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
  return `$${n.toFixed(2)}`;
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

export default function ExploreWhatIfSection({
  item,
  priceMap,
  searchQuery,
  onSearchQueryChange,
  searchResults,
  searchError,
  onSelectTicker,
  positionMap,
  isLoadingTicker,
  tickerError,
}: {
  item: UniverseTickerItem | null | undefined;
  priceMap: Map<string, number>;
  searchQuery: string;
  onSearchQueryChange: (q: string) => void;
  searchResults: UniverseSearchItem[];
  searchError: unknown;
  onSelectTicker: (ticker: string) => void;
  positionMap: Map<string, { shares: number; weight: number; market_value: number; long_short: string }>;
  isLoadingTicker: boolean;
  tickerError: unknown;
}) {
  const { data: accountsData } = useHoldingsAccounts();
  const { data: holdingsData } = useHoldingsPositions(null);

  const [mode, setMode] = useState<WhatIfMode>("raw");
  const [accountId, setAccountId] = useState("");
  const [quantityText, setQuantityText] = useState("");
  const [busy, setBusy] = useState(false);
  const [previewData, setPreviewData] = useState<WhatIfPreviewData | null>(null);
  const [showResults, setShowResults] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");
  const [resultMessage, setResultMessage] = useState("");
  const [scenarioDrafts, setScenarioDrafts] = useState<Record<string, ScenarioDraftRow>>({});
  const toggleRef = useRef<HTMLDivElement>(null);

  // Typeahead state
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);
  const [tickerFocused, setTickerFocused] = useState(false);
  const wrapRef = useRef<HTMLDivElement>(null);

  const accountOptions = accountsData?.accounts ?? [];
  const validAccountIds = useMemo(
    () => new Set(accountOptions.map((account) => normalizeAccountId(account.account_id))),
    [accountOptions],
  );
  const holdingsRows = holdingsData?.positions ?? [];
  const selectedTicker = normalizeTicker(item?.ticker);
  const scenarioTicker = normalizeTicker(searchQuery) || selectedTicker;

  // Price + computed market value for current entry
  const entryPrice = priceMap.get(scenarioTicker) ?? null;
  const entryQty = parseQty(quantityText);
  const entryMv = entryPrice != null && entryQty != null ? entryQty * entryPrice : null;

  // Typeahead: show dropdown when query has results
  useEffect(() => {
    if (tickerFocused && searchQuery.trim().length > 0 && searchResults.length > 0) {
      setDropdownOpen(true);
      setActiveIndex(-1);
    } else {
      setDropdownOpen(false);
    }
  }, [tickerFocused, searchQuery, searchResults.length]);

  // Close dropdown on outside click
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) {
        setDropdownOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const selectFromTypeahead = useCallback(
    (ticker: string) => {
      onSelectTicker(ticker);
      setTickerFocused(false);
      setDropdownOpen(false);
      setActiveIndex(-1);
    },
    [onSelectTicker],
  );

  const handleTickerKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (!dropdownOpen || searchResults.length === 0) {
        if (e.key === "Enter") {
          const direct = searchQuery.trim().toUpperCase();
          if (direct) selectFromTypeahead(direct);
        }
        return;
      }
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setActiveIndex((prev) => (prev < searchResults.length - 1 ? prev + 1 : 0));
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        setActiveIndex((prev) => (prev > 0 ? prev - 1 : searchResults.length - 1));
      } else if (e.key === "Enter") {
        e.preventDefault();
        if (activeIndex >= 0 && activeIndex < searchResults.length) {
          selectFromTypeahead(searchResults[activeIndex].ticker);
        } else {
          const direct = searchQuery.trim().toUpperCase();
          if (direct) selectFromTypeahead(direct);
        }
      } else if (e.key === "Escape") {
        setDropdownOpen(false);
      }
    },
    [dropdownOpen, searchResults, activeIndex, searchQuery, selectFromTypeahead],
  );

  useEffect(() => {
    if (!accountId && accountOptions.length > 0) {
      setAccountId(accountOptions[0].account_id);
    }
  }, [accountId, accountOptions]);

  useEffect(() => {
    setPreviewData(null);
    setErrorMessage("");
    setResultMessage("");
  }, [selectedTicker]);

  const liveQuantityByScenarioKey = useMemo(() => {
    const out = new Map<string, number>();
    for (const row of holdingsRows) {
      const key = scenarioKey(row.account_id, row.ticker);
      out.set(key, Number(out.get(key) || 0) + Number(row.quantity || 0));
    }
    return out;
  }, [holdingsRows]);

  useEffect(() => {
    if (!accountId) return;
    const key = scenarioKey(accountId, scenarioTicker);
    const staged = scenarioDrafts[key];
    if (staged) {
      setQuantityText(staged.quantity_text);
      return;
    }
    setQuantityText("");
  }, [accountId, scenarioTicker, scenarioDrafts]);

  const scenarioRows = useMemo(
    () =>
      Object.values(scenarioDrafts).sort((a, b) => {
        const byTicker = normalizeTicker(a.ticker).localeCompare(normalizeTicker(b.ticker));
        if (byTicker !== 0) return byTicker;
        return normalizeAccountId(a.account_id).localeCompare(normalizeAccountId(b.account_id));
      }),
    [scenarioDrafts],
  );

  const currentModeFactorOrder = useMemo(() => {
    const currentFactors = previewData?.current.exposure_modes[mode] ?? [];
    return [...currentFactors]
      .sort((a, b) => {
        const tierDiff = factorTier(a.factor) - factorTier(b.factor);
        if (tierDiff !== 0) return tierDiff;
        const byMagnitude = Math.abs(Number(b.value || 0)) - Math.abs(Number(a.value || 0));
        if (byMagnitude !== 0) return byMagnitude;
        return a.factor.localeCompare(b.factor);
      })
      .map((factor) => factor.factor);
  }, [previewData, mode]);

  function clearMessages() {
    setErrorMessage("");
    setResultMessage("");
  }

  function stageSelectedTicker() {
    clearMessages();
    setPreviewData(null);
    const account = normalizeAccountId(accountId);
    const ticker = scenarioTicker;
    const qty = parseQty(quantityText);
    if (!account) {
      setErrorMessage("Select an account for the what-if row.");
      return;
    }
    if (validAccountIds.size > 0 && !validAccountIds.has(account)) {
      setErrorMessage("Choose an existing account from the list before staging the what-if row.");
      return;
    }
    if (!ticker) {
      setErrorMessage("Enter a ticker for the what-if row.");
      return;
    }
    if (qty === null) {
      setErrorMessage("Quantity must be numeric.");
      return;
    }
    const key = scenarioKey(account, ticker);
    setScenarioDrafts((prev) => ({
      ...prev,
      [key]: {
        key,
        account_id: account,
        ticker,
        quantity_text: quantityText.trim(),
        source: "what_if",
      },
    }));
    setResultMessage(`Staged trade delta for ${ticker} in ${account}.`);
  }

  function updateScenarioRow(key: string, quantity_value: string) {
    setPreviewData(null);
    setScenarioDrafts((prev) => {
      const existing = prev[key];
      if (!existing) return prev;
      return {
        ...prev,
        [key]: {
          ...existing,
          quantity_text: quantity_value,
        },
      };
    });
  }

  function adjustScenarioRow(key: string, delta: number) {
    const existing = scenarioDrafts[key];
    if (!existing) return;
    const currentQty = parseQty(existing.quantity_text);
    if (currentQty === null) {
      setErrorMessage(`Fix quantity for ${existing.ticker} before stepping it.`);
      return;
    }
    updateScenarioRow(key, fmtQty(currentQty + delta));
  }

  function removeScenarioRow(key: string) {
    setPreviewData(null);
    clearMessages();
    setScenarioDrafts((prev) => {
      const next = { ...prev };
      delete next[key];
      return next;
    });
  }

  async function runPreview() {
    clearMessages();
    const payloadRows: WhatIfScenarioRow[] = [];
    for (const row of scenarioRows) {
      const qty = parseQty(row.quantity_text);
      if (qty === null) {
        setErrorMessage(`Fix quantity for ${row.ticker} before previewing.`);
        return;
      }
      payloadRows.push({
        account_id: row.account_id,
        ticker: row.ticker,
        quantity: qty,
        source: row.source,
      });
    }
    try {
      setBusy(true);
      const out = await previewPortfolioWhatIf({ scenario_rows: payloadRows });
      setPreviewData(out);
      setShowResults(true);
      setResultMessage(`Preview refreshed for ${payloadRows.length} scenario row${payloadRows.length === 1 ? "" : "s"}.`);
      requestAnimationFrame(() => {
        toggleRef.current?.scrollIntoView({ behavior: "smooth", block: "nearest" });
      });
    } catch (err) {
      if (err instanceof ApiError) {
        setErrorMessage(typeof err.detail === "string" ? err.detail : err.message);
      } else if (err instanceof Error) {
        setErrorMessage(err.message);
      } else {
        setErrorMessage("What-if preview failed.");
      }
    } finally {
      setBusy(false);
    }
  }

  async function applyScenario() {
    clearMessages();
    if (scenarioRows.length === 0) {
      setErrorMessage("Stage at least one scenario row first.");
      return;
    }
    const payloadRows: WhatIfScenarioRow[] = [];
    for (const row of scenarioRows) {
      const qty = parseQty(row.quantity_text);
      if (qty === null) {
        setErrorMessage(`Fix quantity for ${row.ticker} before applying.`);
        return;
      }
      if (validAccountIds.size > 0 && !validAccountIds.has(normalizeAccountId(row.account_id))) {
        setErrorMessage(`Choose an existing account for ${row.ticker} before applying.`);
        return;
      }
      payloadRows.push({
        account_id: row.account_id,
        ticker: row.ticker,
        quantity: qty,
        source: row.source || "what_if",
      });
    }
    const hasFullRemovalFromDelta = scenarioRows.some((row) => {
      const qty = parseQty(row.quantity_text);
      if (qty === null) return false;
      const liveQty = Number(liveQuantityByScenarioKey.get(scenarioKey(row.account_id, row.ticker)) || 0);
      return Math.abs(liveQty) > 1e-12 && Math.abs(liveQty + qty) <= 1e-12;
    });
    if (hasFullRemovalFromDelta && !window.confirm("This trade delta fully closes one or more positions. Apply these changes and run RECALC?")) {
      return;
    }

    try {
      setBusy(true);
      const out = await applyPortfolioWhatIf({
        scenario_rows: payloadRows,
        default_source: "what_if",
      });
      if (out.status !== "ok") {
        const rejected = out.rejected?.[0];
        const warning = out.warnings?.[0];
        setErrorMessage(
          rejected?.message ||
            warning ||
            "What-if apply was rejected. Review the staged rows and try again.",
        );
        return;
      }
      if (out.rejected_rows > 0) {
        const rejected = out.rejected?.[0];
        setErrorMessage(rejected?.message || "One or more scenario rows were rejected.");
        return;
      }

      let refreshMessage = "Applied what-if scenario.";
      try {
        await triggerServeRefresh();
        refreshMessage = `Applied ${scenarioRows.length} what-if trade${scenarioRows.length === 1 ? "" : "s"} and started RECALC.`;
      } catch (refreshErr) {
        if (refreshErr instanceof ApiError) {
          refreshMessage = `What-if changes were applied, but RECALC failed: ${typeof refreshErr.detail === "string" ? refreshErr.detail : refreshErr.message}`;
        } else if (refreshErr instanceof Error) {
          refreshMessage = `What-if changes were applied, but RECALC failed: ${refreshErr.message}`;
        } else {
          refreshMessage = "What-if changes were applied, but RECALC failed.";
        }
      }

      await Promise.all([
        mutate(apiPath.holdingsAccounts()),
        mutate(apiPath.holdingsPositions(null)),
        mutate(apiPath.portfolio()),
        mutate(apiPath.risk()),
        mutate(apiPath.exposures("raw")),
        mutate(apiPath.exposures("sensitivity")),
        mutate(apiPath.exposures("risk_contribution")),
        mutate(apiPath.operatorStatus()),
      ]);
      setScenarioDrafts({});
      setPreviewData(null);
      const warningText = out.warnings.length > 0 ? ` ${out.warnings[0]}` : "";
      setResultMessage(`${refreshMessage}${warningText}`);
    } catch (err) {
      if (err instanceof ApiError) {
        setErrorMessage(typeof err.detail === "string" ? err.detail : err.message);
      } else if (err instanceof Error) {
        setErrorMessage(err.message);
      } else {
        setErrorMessage("Could not apply what-if scenario.");
      }
    } finally {
      setBusy(false);
    }
  }

  function discardScenario() {
    setScenarioDrafts({});
    setPreviewData(null);
    setShowResults(false);
    clearMessages();
    setResultMessage("Discarded what-if scenario rows.");
  }

  const builderStatus = previewData
    ? "Preview ready"
    : scenarioRows.length > 0
      ? `${scenarioRows.length} staged`
      : selectedTicker
        ? `${selectedTicker} selected`
        : "Ready";
  const normalizedAccountId = normalizeAccountId(accountId);
  const hasValidAccount = Boolean(normalizedAccountId) && (validAccountIds.size === 0 || validAccountIds.has(normalizedAccountId));
  const hasEntryTicker = Boolean(scenarioTicker);
  const hasValidEntryQty = entryQty !== null;
  const stageReady = !busy && hasValidAccount && hasEntryTicker && hasValidEntryQty;
  const previewReady = !busy && scenarioRows.length > 0;
  const previewNeedsAttention = previewReady && !previewData;
  const applyReady = !busy && scenarioRows.length > 0;
  const discardReady = !busy && scenarioRows.length > 0;

  return (
    <div className="whatif-builder" ref={wrapRef}>
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
        {/* Ticker with typeahead */}
        <div className="whatif-builder-ticker-wrap">
          <input
            id="whatif-entry-ticker"
            className="explore-input whatif-entry-field whatif-entry-ticker"
            value={searchQuery}
            onChange={(e) => onSearchQueryChange(e.target.value)}
            onKeyDown={handleTickerKeyDown}
            onFocus={() => {
              setTickerFocused(true);
              if (searchQuery.trim().length > 0 && searchResults.length > 0) setDropdownOpen(true);
            }}
            onBlur={(e) => {
              const next = e.relatedTarget;
              if (next && wrapRef.current?.contains(next as Node)) return;
              setTickerFocused(false);
              setDropdownOpen(false);
              setActiveIndex(-1);
            }}
            placeholder="Ticker"
            disabled={busy}
            title="Search for a ticker — results appear as you type"
          />
          {dropdownOpen && searchResults.length > 0 && (
            <div className="explore-typeahead whatif-typeahead">
              {searchResults.map((r, i) => {
                const pos = positionMap.get(r.ticker.toUpperCase());
                return (
                  <button
                    key={r.ticker}
                    className={`explore-typeahead-item${i === activeIndex ? " active" : ""}${pos ? " held" : ""}`}
                    onMouseEnter={() => setActiveIndex(i)}
                    onClick={() => selectFromTypeahead(r.ticker)}
                  >
                    <span className="ticker">{highlightMatch(r.ticker, searchQuery)}</span>
                    <span className="name">{highlightMatch(r.name, searchQuery)}</span>
                    <span className="explore-typeahead-classifications">
                      <span>{r.trbc_economic_sector_short_abbr || r.trbc_economic_sector_short || "—"}</span>
                      {r.trbc_industry_group && r.trbc_industry_group !== r.trbc_economic_sector_short && (
                        <span className="explore-typeahead-ig">{r.trbc_industry_group}</span>
                      )}
                    </span>
                    {pos && (
                      <span className="explore-typeahead-held">
                        <span>{pos.shares.toLocaleString()} qty</span>
                        <span>{(pos.weight * 100).toFixed(1)}% wt</span>
                      </span>
                    )}
                    <span className="risk">
                      {typeof r.risk_loading === "number" ? r.risk_loading.toFixed(4) : "N/A"}
                    </span>
                  </button>
                );
              })}
            </div>
          )}
        </div>

        {/* Qty */}
        <input
          id="whatif-entry-qty"
          className="explore-input whatif-entry-field whatif-entry-qty"
          value={quantityText}
          onChange={(e) => setQuantityText(e.target.value)}
          onFocus={() => {
            setTickerFocused(false);
            setDropdownOpen(false);
          }}
          inputMode="decimal"
          placeholder="Trade Δ"
          disabled={busy}
          title="Share delta for the what-if scenario"
        />

        {/* Account */}
        <select
          id="whatif-entry-account"
          className="explore-input whatif-entry-field whatif-entry-account"
          value={accountId}
          onChange={(e) => setAccountId(e.target.value.toLowerCase())}
          onFocus={() => {
            setTickerFocused(false);
            setDropdownOpen(false);
          }}
          disabled={busy}
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

        {/* Live price + computed mkt val */}
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

        {/* Actions */}
        <div className="whatif-builder-actions">
          <button
            className={`btn-action${stageReady ? " ready ready-stage" : ""}`}
            onClick={stageSelectedTicker}
            disabled={busy}
            title="Add this ticker + trade delta as a scenario row for comparison"
          >
            Stage
          </button>
          <button
            className={`btn-action primary${previewNeedsAttention ? " ready ready-preview" : ""}`}
            onClick={() => void runPreview()}
            disabled={!previewReady}
            title="Preview risk impact of all staged scenario rows without committing changes"
          >
            {busy ? "..." : "Preview"}
          </button>
          <button
            className={`btn-action apply${applyReady ? " ready ready-apply" : ""}`}
            onClick={() => void applyScenario()}
            disabled={!applyReady}
            title="Write staged rows to holdings, then trigger a full portfolio recalc"
          >
            Apply
          </button>
          {scenarioRows.length > 0 && (
            <button
              className={`btn-action subtle${discardReady ? " ready ready-discard" : ""}`}
              onClick={discardScenario}
              disabled={busy}
              title="Clear all staged scenario rows and preview data"
            >
              Discard
            </button>
          )}
        </div>
      </div>

      {/* Inline staging queue */}
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
                  disabled={busy}
                  draftActive
                  invalid={rowQty === null}
                  titleBase={`${row.ticker} ${row.account_id}`}
                  onQuantityTextChange={(value) => updateScenarioRow(row.key, value)}
                  onStep={(delta) => adjustScenarioRow(row.key, delta)}
                />
                {rowMv != null && (
                  <span className="whatif-builder-pill-mv">{fmtMarketValue(rowMv)}</span>
                )}
                <button className="whatif-builder-pill-remove" onClick={() => removeScenarioRow(row.key)} type="button" disabled={busy} title="Remove this scenario row">
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

      {previewData ? (
        <>
          <div
            ref={toggleRef}
            role="button"
            tabIndex={0}
            className={`whatif-results-divider${showResults ? " open" : ""}`}
            onClick={() => setShowResults((prev) => !prev)}
            onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); setShowResults((prev) => !prev); } }}
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
              <div className="explore-mode-toggle">
                {MODES.map((entry) => (
                  <button
                    key={entry.key}
                    type="button"
                    className={`explore-mode-btn${mode === entry.key ? " active" : ""}`}
                    onClick={() => setMode(entry.key)}
                  >
                    {entry.label}
                  </button>
                ))}
              </div>

              <div className="explore-detail-grid">
                <div className="chart-card">
                  <span className="explore-compare-label">Current Portfolio</span>
                  <ExposureBarChart factors={previewData.current.exposure_modes[mode]} mode={mode} />
                </div>
                <div className="chart-card">
                  <span className="explore-compare-label">Hypothetical Portfolio</span>
                  <ExposureBarChart
                    factors={previewData.hypothetical.exposure_modes[mode]}
                    mode={mode}
                    orderByFactors={currentModeFactorOrder}
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
                      {(["country", "industry", "style", "idio"] as const).map((bucket) => (
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
                      <tr key={row.factor}>
                        <td>{shortFactorLabel(row.factor)}</td>
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
      ) : (
        <div className="whatif-results-placeholder">
          Stage one or more trade deltas and run <strong>Preview</strong> to turn this page into the full current-versus-hypothetical portfolio analysis.
        </div>
      )}
    </div>
  );
}
