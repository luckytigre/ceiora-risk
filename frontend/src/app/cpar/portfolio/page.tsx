"use client";

import { Suspense, useEffect, useMemo, useRef, useState, type KeyboardEvent } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import InlineShareDraftEditor from "@/features/holdings/components/InlineShareDraftEditor";
import CparLoadingsTable from "@/features/cpar/components/CparLoadingsTable";
import CparPortfolioCoverageTable from "@/features/cpar/components/CparPortfolioCoverageTable";
import CparPortfolioHedgePanel from "@/features/cpar/components/CparPortfolioHedgePanel";
import { useDebouncedValue } from "@/hooks/useDebouncedValue";
import { ApiError } from "@/lib/api";
import { useCparMeta, useCparPortfolioHedge, useCparPortfolioWhatIf, useCparSearch, useHoldingsAccounts } from "@/hooks/useApi";
import {
  canNavigateCparSearchResult,
  describeCparFitStatus,
  formatCparNumber,
  formatCparPercent,
  readCparError,
  sameCparPackageIdentity,
} from "@/lib/cparTruth";
import type { CparHedgeMode, CparSearchItem } from "@/lib/types";

interface CparDraftScenarioRow {
  key: string;
  ric: string;
  ticker: string | null;
  display_name: string | null;
  fit_status: CparSearchItem["fit_status"];
  hq_country_code: string | null;
  quantity_text: string;
}

function genericErrorMessage(error: unknown): string {
  if (error instanceof ApiError) return error.message;
  if (error instanceof Error) return error.message;
  return "Unknown holdings/account error.";
}

function parseQuantityDelta(value: string): number | null {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || Math.abs(parsed) <= 1e-12) return null;
  return parsed;
}

function formatScenarioQuantity(value: number): string {
  if (!Number.isFinite(value)) return "";
  return String(Number(value.toFixed(4)));
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

function CparPortfolioPageInner() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const selectedAccountParam = searchParams?.get("account_id")?.trim() || null;
  const [mode, setMode] = useState<CparHedgeMode>("factor_neutral");
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedScenarioItem, setSelectedScenarioItem] = useState<CparSearchItem | null>(null);
  const [quantityDeltaInput, setQuantityDeltaInput] = useState("10");
  const [scenarioRows, setScenarioRows] = useState<CparDraftScenarioRow[]>([]);
  const [scenarioMessage, setScenarioMessage] = useState<string | null>(null);
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);
  const [tickerFocused, setTickerFocused] = useState(false);
  const searchWrapRef = useRef<HTMLDivElement>(null);
  const debouncedSearchQuery = useDebouncedValue(searchQuery, 220);

  const { data: meta, error: metaError, isLoading: metaLoading } = useCparMeta();
  const metaState = metaError ? readCparError(metaError) : null;
  const { data: accountsData, error: accountsError, isLoading: accountsLoading } = useHoldingsAccounts();
  const { data: searchData, error: searchError, isLoading: searchLoading } = useCparSearch(debouncedSearchQuery, 8);

  const defaultAccountId = useMemo(() => {
    const accounts = accountsData?.accounts ?? [];
    return (
      accounts.find((row) => row.is_active && row.positions_count > 0)?.account_id
      || accounts.find((row) => row.positions_count > 0)?.account_id
      || accounts[0]?.account_id
      || null
    );
  }, [accountsData?.accounts]);

  useEffect(() => {
    if (selectedAccountParam || !defaultAccountId) return;
    const params = new URLSearchParams(searchParams?.toString() || "");
    params.set("account_id", defaultAccountId);
    router.replace(`/cpar/risk?${params.toString()}`);
  }, [defaultAccountId, router, searchParams, selectedAccountParam]);

  const selectedAccountId = selectedAccountParam || defaultAccountId;

  useEffect(() => {
    setScenarioRows([]);
    setSelectedScenarioItem(null);
    setScenarioMessage(null);
    setSearchQuery("");
    setDropdownOpen(false);
    setActiveIndex(-1);
  }, [selectedAccountId, meta?.package_run_id]);

  const searchState = searchError ? readCparError(searchError) : null;
  const searchResults = useMemo(
    () => (searchData?.results ?? []).filter((item) => canNavigateCparSearchResult(item)),
    [searchData?.results],
  );

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

  const {
    data: portfolio,
    error: portfolioError,
    isLoading: portfolioLoading,
  } = useCparPortfolioHedge(selectedAccountId, mode, Boolean(selectedAccountId) && Boolean(meta) && !metaState);
  const portfolioState = portfolioError ? readCparError(portfolioError) : null;
  const packageMismatch = Boolean(meta && portfolio && !sameCparPackageIdentity(meta, portfolio));
  const parsedScenarioRows = useMemo(() => {
    const rows = scenarioRows.map((row) => {
      const quantityDelta = parseQuantityDelta(row.quantity_text);
      if (quantityDelta == null) return null;
      return {
        ric: row.ric,
        ticker: row.ticker,
        quantity_delta: quantityDelta,
      };
    });
    return rows.every((row) => row !== null)
      ? rows as Array<{ ric: string; ticker: string | null; quantity_delta: number }>
      : null;
  }, [scenarioRows]);
  const hasInvalidScenarioRows = scenarioRows.length > 0 && parsedScenarioRows === null;
  const {
    data: whatIf,
    error: whatIfError,
    isLoading: whatIfLoading,
  } = useCparPortfolioWhatIf(
    selectedAccountId,
    mode,
    parsedScenarioRows ?? [],
    Boolean(selectedAccountId)
      && Boolean(meta)
      && !metaState
      && Boolean(portfolio)
      && !portfolioState
      && !packageMismatch
      && Boolean(parsedScenarioRows?.length),
  );

  if (metaLoading && !meta) {
    return <AnalyticsLoadingViz message="Loading cPAR portfolio hedge workflow..." />;
  }

  const whatIfState = whatIfError ? readCparError(whatIfError) : null;
  const whatIfPackageMismatch = Boolean(
    (meta && whatIf && !sameCparPackageIdentity(meta, whatIf))
    || (whatIf && !sameCparPackageIdentity(whatIf, whatIf.current))
    || (whatIf && !sameCparPackageIdentity(whatIf, whatIf.hypothetical)),
  );
  const selectedAccount = (accountsData?.accounts || []).find((row) => row.account_id === selectedAccountId) || null;
  const parsedQuantityDelta = parseQuantityDelta(quantityDeltaInput);
  const builderStatus = scenarioRows.length > 0 ? `${scenarioRows.length} staged` : "Preview-only";

  function selectSearchResult(item: CparSearchItem) {
    setSelectedScenarioItem(item);
    setSearchQuery(item.ticker || item.ric);
    setDropdownOpen(false);
    setTickerFocused(false);
    setActiveIndex(-1);
    setScenarioMessage(null);
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

  function addScenarioRow() {
    if (!selectedScenarioItem || !canNavigateCparSearchResult(selectedScenarioItem)) {
      setScenarioMessage("Choose an active-package search hit with a ticker before staging a what-if row.");
      return;
    }
    if (parsedQuantityDelta == null) {
      setScenarioMessage("Enter a non-zero finite share delta before staging a what-if row.");
      return;
    }
    if (scenarioRows.some((row) => row.ric === selectedScenarioItem.ric)) {
      setScenarioMessage(`RIC ${selectedScenarioItem.ric} is already staged.`);
      return;
    }
    setScenarioRows((current) => [
      ...current,
      {
        key: selectedScenarioItem.ric,
        ric: selectedScenarioItem.ric,
        ticker: selectedScenarioItem.ticker,
        display_name: selectedScenarioItem.display_name,
        fit_status: selectedScenarioItem.fit_status,
        hq_country_code: selectedScenarioItem.hq_country_code || null,
        quantity_text: formatScenarioQuantity(parsedQuantityDelta),
      },
    ]);
    setQuantityDeltaInput("10");
    setSelectedScenarioItem(null);
    setSearchQuery("");
    setScenarioMessage(null);
    setDropdownOpen(false);
    setActiveIndex(-1);
  }

  function updateScenarioRow(ric: string, quantityText: string) {
    setScenarioRows((current) => current.map((row) => (
      row.ric === ric
        ? { ...row, quantity_text: quantityText }
        : row
    )));
    setScenarioMessage(null);
  }

  function adjustScenarioRow(ric: string, delta: number) {
    setScenarioRows((current) => current.map((row) => {
      if (row.ric !== ric) return row;
      const currentQuantity = parseQuantityDelta(row.quantity_text) ?? 0;
      return {
        ...row,
        quantity_text: formatScenarioQuantity(currentQuantity + delta),
      };
    }));
    setScenarioMessage(null);
  }

  function removeScenarioRow(ric: string) {
    setScenarioRows((current) => current.filter((row) => row.ric !== ric));
    setScenarioMessage(null);
  }

  return (
    <div className="cpar-page">
      {metaState ? (
        <section className="chart-card cpar-alert-card" data-testid="cpar-portfolio-not-ready">
          <h3>{metaState.kind === "not_ready" ? "cPAR Risk Not Ready" : "cPAR Risk Unavailable"}</h3>
          <div className="section-subtitle">{metaState.message}</div>
          <div className="detail-history-empty compact">
            This workflow is package-based and read-only. Publish a durable cPAR package first, then reload.
          </div>
        </section>
      ) : null}

      <div className="cpar-two-column">
        <section className="chart-card" data-testid="cpar-portfolio-account-panel">
          <h3>Account Scope</h3>
          <div className="section-subtitle">
            Reused infrastructure: live holdings accounts and positions. Not reused: cUSE4 portfolio or what-if payload semantics.
          </div>
          {accountsLoading && !accountsData ? (
            <AnalyticsLoadingViz message="Loading holdings accounts..." />
          ) : accountsError ? (
            <div className="cpar-inline-message error">
              <strong>Holdings accounts unavailable.</strong>
              <span>{genericErrorMessage(accountsError)}</span>
            </div>
          ) : !(accountsData?.accounts.length) ? (
            <div className="detail-history-empty compact">No holdings accounts are available yet.</div>
          ) : (
            <>
              <label className="cpar-package-label" htmlFor="cpar-account-select">Selected account</label>
              <div className="cpar-search-row">
                <select
                  id="cpar-account-select"
                  className="explore-input whatif-entry-field whatif-entry-account"
                  data-testid="cpar-portfolio-account-select"
                  value={selectedAccountId || ""}
                  onChange={(event) => {
                    const nextAccountId = event.target.value;
                    const params = new URLSearchParams(searchParams?.toString() || "");
                    params.set("account_id", nextAccountId);
                    router.push(`/cpar/risk?${params.toString()}`);
                  }}
                >
                  {(accountsData?.accounts || []).map((account) => (
                    <option key={account.account_id} value={account.account_id}>
                      {account.account_id} · {account.account_name} [{account.positions_count}]
                    </option>
                  ))}
                </select>
              </div>
              {selectedAccount ? (
                <div className="cpar-package-grid compact">
                  <div className="cpar-package-metric">
                    <div className="cpar-package-label">Account</div>
                    <div className="cpar-package-value">{selectedAccount.account_id}</div>
                    <div className="cpar-package-detail">{selectedAccount.account_name}</div>
                  </div>
                  <div className="cpar-package-metric">
                    <div className="cpar-package-label">Positions</div>
                    <div className="cpar-package-value">{selectedAccount.positions_count}</div>
                    <div className="cpar-package-detail">Current holdings rows</div>
                  </div>
                  <div className="cpar-package-metric">
                    <div className="cpar-package-label">Last Update</div>
                    <div className="cpar-package-value">{selectedAccount.last_position_updated_at ? "Live" : "—"}</div>
                    <div className="cpar-package-detail">{selectedAccount.last_position_updated_at || "No positions yet"}</div>
                  </div>
                </div>
              ) : null}
            </>
          )}
        </section>

        <section className="chart-card" data-testid="cpar-portfolio-summary">
          <h3>Workflow Scope</h3>
          <div className="section-subtitle">
            The account workflow prices current holdings at the latest shared-source price on or before the active package date, then aggregates only covered persisted cPAR loadings into one hedge vector.
          </div>
          <div className="cpar-inline-message neutral">
            <strong>Narrow by design.</strong>
            <span>This is a narrow cPAR what-if preview, not a portfolio mutation tool, not a broad analytics engine, and not a cPAR-vs-cUSE4 comparison layer.</span>
            <div className="cpar-badge-row compact">
              <Link href="/cpar/hedge" className="cpar-detail-chip" prefetch={false}>Instrument Hedge</Link>
              <Link href="/cpar/explore" className="cpar-detail-chip" prefetch={false}>Instrument Explore</Link>
            </div>
          </div>
        </section>
      </div>

      {!selectedAccountId && !accountsLoading ? (
        <section className="chart-card">
          <h3>Account Hedge Preview</h3>
          <div className="detail-history-empty compact">Choose a holdings account to open the read-only cPAR portfolio hedge workflow.</div>
        </section>
      ) : metaState || accountsError ? null : portfolioLoading && !portfolio ? (
        <section className="chart-card" data-testid="cpar-portfolio-loading">
          <h3>Account Hedge Preview</h3>
          <AnalyticsLoadingViz message={`Loading cPAR portfolio hedge for ${selectedAccountId}...`} />
        </section>
      ) : portfolioState ? (
        <section className="chart-card" data-testid="cpar-portfolio-error">
          <h3>Account Hedge Preview</h3>
          <div className={`cpar-inline-message ${portfolioState.kind === "missing" ? "warning" : "error"}`}>
            <strong>
              {portfolioState.kind === "missing"
                ? "Account not found."
                : portfolioState.kind === "not_ready"
                  ? "Risk package not ready."
                  : "Risk preview unavailable."}
            </strong>
            <span>{portfolioState.message}</span>
          </div>
        </section>
      ) : packageMismatch ? (
        <section className="chart-card" data-testid="cpar-portfolio-package-mismatch">
            <h3>Account Hedge Preview</h3>
            <div className="cpar-inline-message error">
              <strong>Active package changed during read.</strong>
              <span>The risk workflow no longer matches the active package metadata.</span>
              <span>Reload the page to pin one cPAR package before using the account hedge preview.</span>
            </div>
          </section>
      ) : portfolio ? (
        <>
          <section className="chart-card" data-testid="cpar-portfolio-overview">
            <h3>Account Coverage Summary</h3>
            <div className="cpar-badge-row compact">
              <span className={`cpar-badge ${
                portfolio.portfolio_status === "ok"
                  ? "success"
                  : portfolio.portfolio_status === "partial"
                    ? "warning"
                    : "error"
              }`}>
                {portfolio.portfolio_status === "ok"
                  ? "Coverage OK"
                  : portfolio.portfolio_status === "partial"
                    ? "Partial Coverage"
                    : portfolio.portfolio_status === "empty"
                      ? "Empty Account"
                      : "Coverage Unavailable"}
              </span>
              <span className="cpar-detail-chip">{portfolio.covered_positions_count} covered</span>
              <span className="cpar-detail-chip">{portfolio.excluded_positions_count} excluded</span>
              <span className="cpar-detail-chip">Priced coverage {formatCparPercent(portfolio.coverage_ratio, 1)}</span>
            </div>
            <div className="cpar-package-grid compact">
              <div className="cpar-package-metric">
                <div className="cpar-package-label">Gross MV</div>
                <div className="cpar-package-value">{formatCparNumber(portfolio.gross_market_value, 2)}</div>
                <div className="cpar-package-detail">Priced holdings rows only</div>
              </div>
              <div className="cpar-package-metric">
                <div className="cpar-package-label">Covered Gross</div>
                <div className="cpar-package-value">{formatCparNumber(portfolio.covered_gross_market_value, 2)}</div>
                <div className="cpar-package-detail">Rows included in the hedge vector</div>
              </div>
              <div className="cpar-package-metric">
                <div className="cpar-package-label">Net MV</div>
                <div className="cpar-package-value">{formatCparNumber(portfolio.net_market_value, 2)}</div>
                <div className="cpar-package-detail">Covered rows only</div>
              </div>
            </div>
            {portfolio.portfolio_reason ? (
              <div className="cpar-inline-message warning">
                <strong>Workflow note.</strong>
                <span>{portfolio.portfolio_reason}</span>
              </div>
            ) : null}
          </section>

          {portfolio.aggregate_thresholded_loadings.length > 0 ? (
            <div className="cpar-two-column">
              <CparLoadingsTable
                title="Aggregate Thresholded Loadings"
                rows={portfolio.aggregate_thresholded_loadings}
                emptyText="No covered holdings rows contributed to the aggregate thresholded portfolio vector."
              />
              <section className="chart-card">
                <h3>Scope Notes</h3>
                <div className="section-subtitle">
                  Aggregate loadings are weighted by signed market value over covered gross market value, using the current holdings quantities and latest shared-source prices on or before the package date. The displayed coverage ratio is covered gross divided by priced gross.
                </div>
                <div className="cpar-inline-message neutral">
                  <strong>No mutation path.</strong>
                  <span>The account workflow is read-only. Change holdings elsewhere, publish the active package if needed, then reload this page.</span>
                </div>
              </section>
            </div>
          ) : null}

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
                      setScenarioMessage(null);
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
                        const fit = describeCparFitStatus(item.fit_status);
                        return (
                          <button
                            key={item.ric}
                            type="button"
                            className={`explore-typeahead-item${index === activeIndex ? " active" : ""}`}
                            onMouseEnter={() => setActiveIndex(index)}
                            onMouseDown={(event) => event.preventDefault()}
                            onClick={() => selectSearchResult(item)}
                          >
                            <span className="ticker">{highlightMatch(item.ticker || item.ric, searchQuery)}</span>
                            <span className="name">{highlightMatch(item.display_name || item.ric, searchQuery)}</span>
                            <span className="explore-typeahead-classifications">
                              <span>{fit.label}</span>
                              {item.hq_country_code ? <span className="explore-typeahead-ig">{item.hq_country_code}</span> : null}
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
                    setScenarioMessage(null);
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
                    onClick={addScenarioRow}
                  >
                    Stage
                  </button>
                  {scenarioRows.length > 0 ? (
                    <button
                      type="button"
                      className="btn-action subtle"
                      onClick={() => {
                        setScenarioRows([]);
                        setScenarioMessage(null);
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
                        onQuantityTextChange={(value) => updateScenarioRow(row.ric, value)}
                        onStep={(delta) => adjustScenarioRow(row.ric, delta)}
                      />
                      <span className="whatif-builder-pill-mv">{describeCparFitStatus(row.fit_status).label}</span>
                      <button
                        className="whatif-builder-pill-remove"
                        onClick={() => removeScenarioRow(row.ric)}
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
              ) : searchQuery.trim().length > 0 && !searchResults.length && !searchState ? (
                <div className="whatif-builder-feedback">No active-package ticker results matched this search.</div>
              ) : null}

              <div className="whatif-builder-feedback">
                Positive values add shares, negative values reduce shares, and the preview remains read-only.
              </div>
              {scenarioMessage ? (
                <div className="whatif-builder-feedback error">{scenarioMessage}</div>
              ) : hasInvalidScenarioRows ? (
                <div className="whatif-builder-feedback error">
                  One or more staged rows have an invalid or zero share delta. Fix the draft before the hypothetical
                  preview can recompute.
                </div>
              ) : null}
            </div>
          </section>

          {scenarioRows.length === 0 ? (
            portfolio.portfolio_status === "ok" || portfolio.portfolio_status === "partial" ? (
              <CparPortfolioHedgePanel
                data={portfolio}
                mode={mode}
                onModeChange={setMode}
              />
            ) : null
          ) : hasInvalidScenarioRows ? (
            <section className="chart-card" data-testid="cpar-portfolio-whatif-invalid">
              <h3>What-If Preview</h3>
              <div className="cpar-inline-message warning">
                <strong>Draft rows need attention.</strong>
                <span>At least one staged row has an invalid or zero share delta.</span>
                <span>Update the staged queue above before the hypothetical preview can recompute.</span>
              </div>
            </section>
          ) : whatIfLoading && !whatIf ? (
            <section className="chart-card" data-testid="cpar-portfolio-whatif-loading">
              <h3>What-If Preview</h3>
              <AnalyticsLoadingViz message={`Loading cPAR what-if preview for ${selectedAccountId}...`} />
            </section>
          ) : whatIfState ? (
            <section className="chart-card" data-testid="cpar-portfolio-whatif-error">
              <h3>What-If Preview</h3>
              <div className={`cpar-inline-message ${whatIfState.kind === "missing" ? "warning" : "error"}`}>
                <strong>
                  {whatIfState.kind === "missing"
                    ? "Account not found."
                    : whatIfState.kind === "not_ready"
                      ? "What-if package not ready."
                      : "What-if preview unavailable."}
                </strong>
                <span>{whatIfState.message}</span>
              </div>
            </section>
          ) : whatIfPackageMismatch ? (
            <section className="chart-card" data-testid="cpar-portfolio-whatif-package-mismatch">
              <h3>What-If Preview</h3>
              <div className="cpar-inline-message error">
                <strong>Active package changed during what-if read.</strong>
                <span>The what-if preview no longer matches the active package metadata.</span>
                <span>Reload the page to pin one cPAR package before comparing current and hypothetical hedges.</span>
              </div>
            </section>
          ) : whatIf ? (
            <>
              <section className="chart-card" data-testid="cpar-portfolio-whatif-scenarios">
                <h3>Scenario Preview Rows</h3>
                <div className="section-subtitle">
                  Each row is previewed against the active package only. Coverage and fit warnings remain explicit, and no holdings mutation occurs.
                </div>
                <div className="dash-table">
                  <table>
                    <thead>
                      <tr>
                        <th>Instrument</th>
                        <th className="text-right">Current Qty</th>
                        <th className="text-right">Delta</th>
                        <th className="text-right">Hyp Qty</th>
                        <th className="text-right">MV Delta</th>
                        <th>Coverage</th>
                      </tr>
                    </thead>
                    <tbody>
                      {whatIf.scenario_rows.map((row) => (
                        <tr key={row.ric}>
                          <td>
                            <strong>{row.ticker || row.ric}</strong>
                            <span className="cpar-table-sub">{row.display_name || row.ric}</span>
                          </td>
                          <td className="text-right cpar-number-cell">{formatCparNumber(row.current_quantity, 2)}</td>
                          <td className="text-right cpar-number-cell">{formatCparNumber(row.quantity_delta, 2)}</td>
                          <td className="text-right cpar-number-cell">{formatCparNumber(row.hypothetical_quantity, 2)}</td>
                          <td className="text-right cpar-number-cell">{formatCparNumber(row.market_value_delta, 2)}</td>
                          <td>{row.coverage_reason || row.coverage}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </section>

              <div className="cpar-two-column">
                <CparPortfolioHedgePanel
                  data={whatIf.current}
                  mode={mode}
                  onModeChange={setMode}
                  title="Current Account Hedge"
                  subtitle="This is the live covered account vector under the active cPAR package, before staged share deltas are applied."
                  testId="cpar-portfolio-current-hedge-panel"
                />
                <CparPortfolioHedgePanel
                  data={whatIf.hypothetical}
                  mode={mode}
                  onModeChange={setMode}
                  title="Hypothetical Account Hedge"
                  subtitle="This is the same account after applying the staged cPAR what-if deltas, still using the same active package and persisted covariance surface."
                  testId="cpar-portfolio-hypothetical-hedge-panel"
                />
              </div>
            </>
          ) : null}

          <CparPortfolioCoverageTable rows={portfolio.positions} />
        </>
      ) : null}
    </div>
  );
}

export default function CparPortfolioPage() {
  return (
    <Suspense fallback={<AnalyticsLoadingViz message="Loading cPAR portfolio hedge workflow..." />}>
      <CparPortfolioPageInner />
    </Suspense>
  );
}
