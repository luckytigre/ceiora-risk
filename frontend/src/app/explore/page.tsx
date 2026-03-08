"use client";

import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from "react";
import ExposureBarChart from "@/components/ExposureBarChart";
import FactorRadarChart from "@/components/FactorRadarChart";
import TickerWeeklyPriceChart from "@/components/TickerWeeklyPriceChart";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import ApiErrorState from "@/components/ApiErrorState";
import {
  usePortfolio,
  useUniverseFactors,
  useUniverseSearch,
  useUniverseTicker,
  useUniverseTickerHistory,
} from "@/hooks/useApi";
import { shortFactorLabel, factorTier } from "@/lib/factorLabels";
import type { FactorExposure } from "@/lib/types";

interface ExploreRow {
  factor: string;
  loading: number;
  factorVol: number;
  sensitivity: number;
  tier: number;
}

const TIER_NAMES: Record<number, string> = { 1: "Country", 2: "Industry", 3: "Style" };

function fmtPct(value: number): string {
  return `${(value * 100).toFixed(2)}%`;
}

function highlightMatch(text: string, query: string) {
  if (!query) return text;
  const idx = text.toLowerCase().indexOf(query.toLowerCase());
  if (idx === -1) return text;
  return (
    <>
      {text.slice(0, idx)}
      <mark style={{ background: "rgba(215, 87, 186, 0.3)", color: "inherit" }}>
        {text.slice(idx, idx + query.length)}
      </mark>
      {text.slice(idx + query.length)}
    </>
  );
}

export default function ExplorePage() {
  const [query, setQuery] = useState("");
  const [selectedTicker, setSelectedTicker] = useState<string | null>(null);
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(-1);
  const wrapRef = useRef<HTMLDivElement>(null);

  const { data: searchData, error: searchError } = useUniverseSearch(query, 10);
  const { data: tickerData, isLoading, error: tickerError } = useUniverseTicker(selectedTicker);
  const {
    data: historyData,
    isLoading: historyLoading,
    error: historyError,
  } = useUniverseTickerHistory(selectedTicker, 5);
  const { data: factorsData, error: factorsError } = useUniverseFactors();
  const { data: portfolioData, error: portfolioError } = usePortfolio();

  const item = tickerData?.item;
  const factorVols = factorsData?.factor_vols ?? {};
  const results = searchData?.results ?? [];
  const historyPoints = historyData?.points ?? [];

  const historySummary = useMemo(() => {
    if (historyPoints.length === 0) return null;
    const first = Number(historyPoints[0]?.close ?? 0);
    const latest = Number(historyPoints[historyPoints.length - 1]?.close ?? 0);
    const totalReturnPct = first > 0 ? ((latest / first) - 1) * 100 : null;
    return {
      latest,
      totalReturnPct,
      isPositive: (totalReturnPct ?? 0) >= 0,
    };
  }, [historyPoints]);

  // Build a quick lookup of held positions by ticker
  const positionMap = useMemo(() => {
    const map = new Map<string, { shares: number; weight: number; market_value: number; long_short: string }>();
    for (const p of portfolioData?.positions ?? []) {
      map.set(p.ticker.toUpperCase(), {
        shares: p.shares,
        weight: p.weight,
        market_value: p.market_value,
        long_short: p.long_short,
      });
    }
    return map;
  }, [portfolioData]);

  const selectedPosition = selectedTicker ? positionMap.get(selectedTicker.toUpperCase()) : undefined;

  // Show dropdown when there's a query with results
  useEffect(() => {
    if (query.trim().length > 0 && results.length > 0) {
      setDropdownOpen(true);
      setActiveIndex(-1);
    } else {
      setDropdownOpen(false);
    }
  }, [query, results.length]);

  // Close on outside click
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) {
        setDropdownOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const selectTicker = useCallback((ticker: string) => {
    setSelectedTicker(ticker);
    setQuery(ticker);
    setDropdownOpen(false);
    setActiveIndex(-1);
  }, []);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (!dropdownOpen || results.length === 0) {
        if (e.key === "Enter") {
          const direct = query.trim().toUpperCase();
          if (direct) selectTicker(direct);
        }
        return;
      }
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setActiveIndex((prev) => (prev < results.length - 1 ? prev + 1 : 0));
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        setActiveIndex((prev) => (prev > 0 ? prev - 1 : results.length - 1));
      } else if (e.key === "Enter") {
        e.preventDefault();
        if (activeIndex >= 0 && activeIndex < results.length) {
          selectTicker(results[activeIndex].ticker);
        } else {
          const direct = query.trim().toUpperCase();
          if (direct) selectTicker(direct);
        }
      } else if (e.key === "Escape") {
        setDropdownOpen(false);
      }
    },
    [dropdownOpen, results, activeIndex, query, selectTicker],
  );

  const chartFactors: FactorExposure[] = useMemo(() => {
    if (!item) return [];
    const exposures = item.exposures ?? {};
    const sensitivities = item.sensitivities ?? {};
    return Object.entries(exposures).map(([factor, rawVal]) => {
      const loading = Number(rawVal) || 0;
      const fv = Number(factorVols[factor] ?? 0) || 0;
      return {
        factor,
        value: loading,
        factor_vol: fv,
        drilldown: [
          {
            ticker: item.ticker,
            weight: loading >= 0 ? 1 : -1,
            exposure: loading,
            sensitivity: Number(sensitivities[factor] ?? loading * fv) || 0,
            contribution: loading,
          },
        ],
      };
    });
  }, [item, factorVols]);

  const rows: ExploreRow[] = useMemo(() => {
    if (!item) return [];
    const exposures = item.exposures ?? {};
    const sensitivities = item.sensitivities ?? {};
    return Object.entries(exposures)
      .map(([factor, rawVal]) => {
        const loading = Number(rawVal) || 0;
        const fv = Number(factorVols[factor] ?? 0) || 0;
        const sensitivity = Number(sensitivities[factor] ?? loading * fv) || 0;
        const tier = factorTier(factor);
        return { factor, loading, factorVol: fv, sensitivity, tier };
      })
      .sort((a, b) => {
        const tierDiff = a.tier - b.tier;
        if (tierDiff !== 0) return tierDiff;
        return Math.abs(b.sensitivity) - Math.abs(a.sensitivity);
      });
  }, [item, factorVols]);

  // Group rows by tier for rendering section headers
  const groupedRows = useMemo(() => {
    const groups: { tier: number; rows: ExploreRow[] }[] = [];
    let currentTier = -1;
    for (const row of rows) {
      if (row.tier !== currentTier) {
        currentTier = row.tier;
        groups.push({ tier: currentTier, rows: [] });
      }
      groups[groups.length - 1].rows.push(row);
    }
    return groups;
  }, [rows]);

  const weeklyHistoryCard = item ? (
    <div className="chart-card">
      <h3 style={{ marginBottom: 25 }}>{item.ticker} Weekly Price</h3>
      <div className="detail-history" style={{ marginTop: 0, marginBottom: 0 }}>
        <div className="detail-history-header">
          <h5>5Y Weekly Close</h5>
          {!historyLoading && !historyError && historySummary && (
            <div className="detail-history-stats">
              {historySummary.totalReturnPct != null && (
                <span
                  className="detail-history-stat"
                  style={{
                    color: historySummary.isPositive
                      ? "rgba(107, 207, 154, 0.85)"
                      : "rgba(224, 87, 127, 0.85)",
                  }}
                >
                  {historySummary.totalReturnPct >= 0 ? "+" : ""}
                  {historySummary.totalReturnPct.toFixed(1)}%
                </span>
              )}
              <span className="detail-history-stat muted">
                ${historySummary.latest.toFixed(2)}
              </span>
            </div>
          )}
        </div>
        {historyLoading
          ? <div className="detail-history-empty loading-pulse">Loading weekly history...</div>
          : historyError
            ? (
              <div className="detail-history-empty">
                Weekly history is temporarily unavailable for {item.ticker}.
              </div>
            )
            : <TickerWeeklyPriceChart ticker={item.ticker} points={historyPoints} />}
      </div>
    </div>
  ) : null;

  if (factorsError || portfolioError) {
    return <ApiErrorState title="Universe Data Not Ready" error={factorsError || portfolioError} />;
  }

  return (
    <div>
      {/* Search */}
      <div className="chart-card mb-4">
        <h3>Explore Universe</h3>
        <div className="explore-search-wrap" ref={wrapRef}>
          <div className="explore-search-row">
            <input
              className="explore-input"
              type="text"
              placeholder="Search ticker or company"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={handleKeyDown}
              onFocus={() => {
                if (query.trim().length > 0 && results.length > 0) setDropdownOpen(true);
              }}
            />
            <button
              className="explore-search-btn"
              onClick={() => {
                const direct = query.trim().toUpperCase();
                if (direct) selectTicker(direct);
              }}
            >
              Lookup
            </button>
          </div>

          {dropdownOpen && results.length > 0 && (
            <div className="explore-typeahead">
              {results.map((r, i) => {
                const pos = positionMap.get(r.ticker.toUpperCase());
                return (
                  <button
                    key={r.ticker}
                    className={`explore-typeahead-item${i === activeIndex ? " active" : ""}${pos ? " held" : ""}`}
                    onMouseEnter={() => setActiveIndex(i)}
                    onClick={() => selectTicker(r.ticker)}
                  >
                    <span className="ticker">{highlightMatch(r.ticker, query)}</span>
                    <span className="name">{highlightMatch(r.name, query)}</span>
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

        {!isLoading && selectedTicker && tickerError && (
          <div className="explore-error">
            Unable to load {selectedTicker}. Check universe cache and try refresh.
          </div>
        )}
        {searchError && query.trim().length > 0 && (
          <div className="explore-error">
            Universe search is temporarily unavailable.
          </div>
        )}
      </div>

      {isLoading && selectedTicker && <AnalyticsLoadingViz message={`Loading ${selectedTicker}...`} />}

      {item && !isLoading && (
        <>
          {/* Hero Card */}
          <div className="chart-card mb-4">
            <div className="explore-hero">
              <div className="explore-hero-title">
                <span className="ticker">{item.ticker}</span>
                <span className="name">{item.name}</span>
              </div>
              <div className="explore-hero-badges">
                <span className="explore-badge">{item.trbc_economic_sector_short || "Unclassified"}</span>
                {item.trbc_industry_group && item.trbc_industry_group !== item.trbc_economic_sector_short && (
                  <span className="explore-badge">{item.trbc_industry_group}</span>
                )}
              </div>
              <div className="explore-hero-stats">
                <div className="explore-hero-stat">
                  <span className="label">Price</span>
                  <span className="value">${item.price.toFixed(2)}</span>
                </div>
                <div className="explore-hero-stat">
                  <span className="label">Mkt Cap</span>
                  <span className="value">
                    {item.market_cap ? `$${(item.market_cap / 1e9).toFixed(2)}B` : "—"}
                  </span>
                </div>
                <div className="explore-hero-stat">
                  <span className="label">Beta</span>
                  <span className="value">
                    —
                  </span>
                </div>
                <div className="explore-hero-stat">
                  <span className="label">P/E</span>
                  <span className="value">—</span>
                </div>
                <div className="explore-hero-stat">
                  <span className="label">Risk Loading</span>
                  <span className="value">
                    {typeof item.risk_loading === "number" ? item.risk_loading.toFixed(4) : "N/A"}
                  </span>
                </div>
              </div>
              {selectedPosition && (
                <div className="explore-hero-position">
                  <span className="explore-hero-position-label">Portfolio Position</span>
                  <div className="explore-hero-stats">
                    <div className="explore-hero-stat">
                      <span className="label">Shares</span>
                      <span className="value">{selectedPosition.shares.toLocaleString()}</span>
                    </div>
                    <div className="explore-hero-stat">
                      <span className="label">Mkt Val</span>
                      <span className="value">
                        ${selectedPosition.market_value >= 1e6
                          ? `${(selectedPosition.market_value / 1e6).toFixed(2)}M`
                          : selectedPosition.market_value.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                      </span>
                    </div>
                    <div className="explore-hero-stat">
                      <span className="label">Weight</span>
                      <span className="value">{(selectedPosition.weight * 100).toFixed(2)}%</span>
                    </div>
                    <div className="explore-hero-stat">
                      <span className="label">Side</span>
                      <span className="value">{selectedPosition.long_short}</span>
                    </div>
                  </div>
                </div>
              )}
            </div>
            {item.eligible_for_model === false && (
              <div
                style={{
                  marginTop: "10px",
                  padding: "10px 12px",
                  border: "1px solid rgba(180,180,180,0.35)",
                  background: "rgba(120,120,120,0.08)",
                  color: "var(--text-secondary)",
                }}
              >
                {item.model_warning || "Ticker is not eligible for strict equity-model analytics."}
              </div>
            )}
          </div>

          {/* Two-column: Radar + Table */}
          {item.eligible_for_model !== false ? (
            <>
              <div className="explore-detail-grid">
                <div className="chart-card">
                  <h3>Style Factor Profile</h3>
                  <FactorRadarChart exposures={item.exposures ?? {}} />
                </div>

                <div className="chart-card">
                  <h3>{item.ticker} Factor Exposures</h3>
                  <ExposureBarChart factors={chartFactors} />
                </div>
              </div>

              <div className="explore-detail-grid">
                {weeklyHistoryCard}

                {/* Right: Factor Loadings Table */}
                <div className="chart-card">
                  <h3>Factor Loadings</h3>
                  <div className="dash-table" style={{ maxHeight: 360, overflowY: "auto" }}>
                    <table>
                      <thead>
                        <tr>
                          <th>Factor</th>
                          <th className="text-right">Loading</th>
                          <th className="text-right">Factor Vol</th>
                          <th className="text-right">Sensitivity</th>
                        </tr>
                      </thead>
                      <tbody>
                        {groupedRows.map((group) => (
                          <Fragment key={`tier-${group.tier}`}>
                            <tr className="explore-tier-header">
                              <td colSpan={4}>{TIER_NAMES[group.tier] ?? "Other"}</td>
                            </tr>
                            {group.rows.map((row) => (
                              <tr key={row.factor}>
                                <td>{shortFactorLabel(row.factor)}</td>
                                <td
                                  className={`text-right ${row.loading >= 0 ? "positive" : "negative"}`}
                                >
                                  {row.loading >= 0 ? "+" : ""}
                                  {row.loading.toFixed(4)}
                                </td>
                                <td className="text-right">{fmtPct(row.factorVol)}</td>
                                <td
                                  className={`text-right ${row.sensitivity >= 0 ? "positive" : "negative"}`}
                                >
                                  {row.sensitivity >= 0 ? "+" : ""}
                                  {row.sensitivity.toFixed(4)}
                                </td>
                              </tr>
                            ))}
                          </Fragment>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            </>
          ) : (
            <>
              {weeklyHistoryCard}

              <div className="chart-card mb-4">
                <h3>Model Analytics</h3>
                <div style={{ color: "var(--text-secondary)" }}>
                  N/A for this ticker under strict equity eligibility rules.
                </div>
              </div>
            </>
          )}
        </>
      )}
    </div>
  );
}
