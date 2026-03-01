"use client";

import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from "react";
import ExposureBarChart from "@/components/ExposureBarChart";
import FactorRadarChart from "@/components/FactorRadarChart";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import { useUniverseFactors, useUniverseSearch, useUniverseTicker } from "@/hooks/useApi";
import { shortFactorLabel, factorTier } from "@/lib/factorLabels";
import type { FactorExposure } from "@/lib/types";

interface ExploreRow {
  factor: string;
  loading: number;
  factorVol: number;
  sensitivity: number;
  tier: number;
}

const TIER_NAMES: Record<number, string> = { 1: "Industry", 2: "Style" };

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

  const { data: searchData } = useUniverseSearch(query, 10);
  const { data: tickerData, isLoading, error: tickerError } = useUniverseTicker(selectedTicker);
  const { data: factorsData } = useUniverseFactors();

  const item = tickerData?.item;
  const factorVols = factorsData?.factor_vols ?? {};
  const results = searchData?.results ?? [];

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
              {results.map((r, i) => (
                <button
                  key={r.ticker}
                  className={`explore-typeahead-item${i === activeIndex ? " active" : ""}`}
                  onMouseEnter={() => setActiveIndex(i)}
                  onClick={() => selectTicker(r.ticker)}
                >
                  <span className="ticker">{highlightMatch(r.ticker, query)}</span>
                  <span className="name">{highlightMatch(r.name, query)}</span>
                  <span className="explore-badge" style={{ flexShrink: 0 }}>
                    {r.trbc_sector_abbr || r.trbc_sector || "—"}
                  </span>
                  <span className="risk">{r.risk_loading.toFixed(4)}</span>
                </button>
              ))}
            </div>
          )}
        </div>

        {!isLoading && selectedTicker && tickerError && (
          <div className="explore-error">
            No cached universe loadings found for {selectedTicker}.
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
                <span className="explore-badge">{item.trbc_sector || "Unclassified"}</span>
                {item.trbc_industry_group && item.trbc_industry_group !== item.trbc_sector && (
                  <span className="explore-badge">{item.trbc_industry_group}</span>
                )}
              </div>
              <div className="explore-hero-stats">
                <div className="explore-hero-stat">
                  <span className="label">Price</span>
                  <span className="value">${item.price.toFixed(2)}</span>
                </div>
                <div className="explore-hero-stat">
                  <span className="label">Market Cap</span>
                  <span className="value">
                    {item.market_cap ? `$${(item.market_cap / 1e9).toFixed(2)}B` : "—"}
                  </span>
                </div>
                <div className="explore-hero-stat">
                  <span className="label">Risk Loading</span>
                  <span className="value">{item.risk_loading.toFixed(4)}</span>
                </div>
              </div>
            </div>
          </div>

          {/* Two-column: Radar + Table */}
          <div className="explore-detail-grid">
            {/* Left: Radar Chart */}
            <div className="chart-card">
              <h3>Style Factor Profile</h3>
              <FactorRadarChart exposures={item.exposures ?? {}} />
            </div>

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

          {/* Full exposure bar chart */}
          <div className="chart-card mb-4">
            <h3>{item.ticker} Factor Profile</h3>
            <ExposureBarChart factors={chartFactors} />
          </div>
        </>
      )}
    </div>
  );
}
