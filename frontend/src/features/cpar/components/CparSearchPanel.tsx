"use client";

import { useEffect, useState } from "react";
import { useDebouncedValue } from "@/hooks/useDebouncedValue";
import { useCparSearch } from "@/hooks/useCparApi";
import { canNavigateCparSearchResult, readCparError } from "@/lib/cparTruth";
import type { CparSearchItem } from "@/lib/types/cpar";
import CparWarningsBar from "./CparWarningsBar";

export default function CparSearchPanel({
  initialQuery = "",
  selectedRic,
  title = "Find A cPAR Instrument",
  helperText = "Search the active package by ticker, RIC, or display name.",
  onSelectResult,
}: {
  initialQuery?: string;
  selectedRic?: string | null;
  title?: string;
  helperText?: string;
  onSelectResult: (item: CparSearchItem) => void;
}) {
  const [query, setQuery] = useState(initialQuery);
  const debouncedQuery = useDebouncedValue(query, 220);
  const { data, error, isLoading } = useCparSearch(debouncedQuery, 12);

  useEffect(() => {
    setQuery(initialQuery);
  }, [initialQuery]);

  const errorSummary = error ? readCparError(error) : null;

  return (
    <section className="chart-card cpar-search-panel" data-testid="cpar-search-panel">
      <h3>{title}</h3>
      <div className="section-subtitle">{helperText}</div>
      <div className="cpar-search-row">
        <input
          data-testid="cpar-search-input"
          className="cpar-search-input"
          type="search"
          value={query}
          placeholder="AAPL, AAPL.OQ, Apple..."
          onChange={(event) => setQuery(event.target.value)}
          autoComplete="off"
          spellCheck={false}
        />
      </div>
      {query.trim().length === 0 ? (
        <div className="detail-history-empty compact">
          Start typing to query the active cPAR package.
        </div>
      ) : isLoading && !data ? (
        <div className="detail-history-empty compact">Searching active package…</div>
      ) : errorSummary ? (
        <div className="cpar-inline-message warning">
          <strong>{errorSummary.kind === "not_ready" ? "Package not ready." : "Search unavailable."}</strong>
          <span>{errorSummary.message}</span>
        </div>
      ) : data && data.results.length === 0 ? (
        <div className="detail-history-empty compact">
          No active-package results matched “{data.query}”.
        </div>
      ) : (
        <>
          <div className="cpar-search-summary">
            {data ? `${Math.min(data.results.length, data.limit)} of ${data.total} active-package matches` : ""}
          </div>
          <div className="cpar-search-results" data-testid="cpar-search-results">
            {(data?.results || []).map((item) => {
              const active = selectedRic && item.ric === selectedRic;
              const disabled = !canNavigateCparSearchResult(item);
              return (
                <button
                  key={item.ric}
                  type="button"
                  className={`cpar-search-result ${active ? "active" : ""} ${disabled ? "disabled" : ""}`}
                  onClick={() => {
                    if (disabled) return;
                    onSelectResult(item);
                  }}
                  disabled={disabled}
                >
                  <div className="cpar-search-main">
                    <div className="cpar-search-title">
                      <strong>{item.ticker || item.ric}</strong>
                      <span>{item.display_name || item.ric}</span>
                    </div>
                    <div className="cpar-search-ric">{item.ric}</div>
                  </div>
                  <div className="cpar-search-meta">
                    <CparWarningsBar fitStatus={item.fit_status} warnings={item.warnings} compact />
                    <span className="cpar-search-country">
                      {disabled ? "Ticker required" : item.hq_country_code || "—"}
                    </span>
                  </div>
                </button>
              );
            })}
          </div>
        </>
      )}
    </section>
  );
}
