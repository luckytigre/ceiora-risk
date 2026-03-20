"use client";

import { useEffect, useMemo, useState, type KeyboardEvent, type ReactNode } from "react";
import { useDebouncedValue } from "@/hooks/useDebouncedValue";
import { useCparSearch } from "@/hooks/useCparApi";
import { canNavigateCparSearchResult, readCparError } from "@/lib/cparTruth";
import type { CparSearchItem } from "@/lib/types/cpar";
import CparWarningsBar from "./CparWarningsBar";

function highlightMatch(text: string, query: string): ReactNode {
  if (!query) return text;
  const index = text.toLowerCase().indexOf(query.toLowerCase());
  if (index === -1) return text;
  return (
    <>
      {text.slice(0, index)}
      <mark className="explore-highlight">{text.slice(index, index + query.length)}</mark>
      {text.slice(index + query.length)}
    </>
  );
}

function findSelectableIndex(
  results: CparSearchItem[],
  startIndex: number,
  direction: 1 | -1,
): number {
  if (results.length === 0) return -1;
  let nextIndex = startIndex;
  for (let attempts = 0; attempts < results.length; attempts += 1) {
    nextIndex = (nextIndex + direction + results.length) % results.length;
    if (canNavigateCparSearchResult(results[nextIndex])) return nextIndex;
  }
  return startIndex;
}

function firstSelectableIndex(results: CparSearchItem[]): number {
  const index = results.findIndex((item) => canNavigateCparSearchResult(item));
  return index >= 0 ? index : 0;
}

export default function CparExploreSearchModule({
  initialQuery = "",
  selectedRic,
  onSelectResult,
}: {
  initialQuery?: string;
  selectedRic?: string | null;
  onSelectResult: (item: CparSearchItem) => void;
}) {
  const [query, setQuery] = useState(initialQuery);
  const [activeIndex, setActiveIndex] = useState(0);
  const debouncedQuery = useDebouncedValue(query, 220);
  const { data, error, isLoading } = useCparSearch(debouncedQuery, 12);

  useEffect(() => {
    setQuery(initialQuery);
  }, [initialQuery]);

  useEffect(() => {
    setActiveIndex(firstSelectableIndex(data?.results ?? []));
  }, [debouncedQuery, data?.results.length]);

  const errorSummary = error ? readCparError(error) : null;
  const results = data?.results ?? [];
  const resultSummary = useMemo(() => {
    if (!data) return "Active package";
    return `${Math.min(data.results.length, data.limit)} of ${data.total} matches`;
  }, [data]);

  const selectIndex = (index: number) => {
    const item = results[index];
    if (!item || !canNavigateCparSearchResult(item)) return;
    onSelectResult(item);
  };

  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (!results.length) return;
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setActiveIndex((current) => findSelectableIndex(results, current, 1));
      return;
    }
    if (event.key === "ArrowUp") {
      event.preventDefault();
      setActiveIndex((current) => findSelectableIndex(results, current, -1));
      return;
    }
    if (event.key === "Enter") {
      event.preventDefault();
      const index = canNavigateCparSearchResult(results[activeIndex]) ? activeIndex : firstSelectableIndex(results);
      selectIndex(index);
    }
  };

  return (
    <section className="chart-card cpar-explore-search-module" data-testid="cpar-search-panel">
      <div className="cpar-explore-module-header">
        <div>
          <div className="cpar-explore-kicker">Explore</div>
          <h3 className="cpar-explore-module-title">Search The Active Package</h3>
          <div className="cpar-explore-module-subtitle">
            Search the active package by ticker, RIC, or display name, then pin one persisted fit row before reading
            loadings or opening the hedge workflow.
          </div>
        </div>
        <div className="cpar-explore-module-status">{resultSummary}</div>
      </div>

      <div className="cpar-explore-search-wrap explore-search-wrap">
        <input
          data-testid="cpar-search-input"
          className="explore-input cpar-explore-search-input"
          type="search"
          value={query}
          placeholder="AAPL, AAPL.OQ, Apple..."
          onChange={(event) => setQuery(event.target.value)}
          onKeyDown={onKeyDown}
          autoComplete="off"
          spellCheck={false}
        />

        {query.trim().length > 0 && results.length > 0 ? (
          <div className="explore-typeahead cpar-explore-typeahead" data-testid="cpar-search-results">
            {results.map((item, index) => {
              const active = (selectedRic && item.ric === selectedRic) || index === activeIndex;
              const disabled = !canNavigateCparSearchResult(item);
              return (
                <button
                  key={item.ric}
                  type="button"
                  className={`explore-typeahead-item cpar-explore-typeahead-item${active ? " active" : ""}${disabled ? " disabled" : ""}`}
                  onMouseEnter={() => setActiveIndex(index)}
                  onClick={() => {
                    if (disabled) return;
                    onSelectResult(item);
                  }}
                  disabled={disabled}
                >
                  <span className="ticker">{highlightMatch(item.ticker || item.ric, query)}</span>
                  <span className="cpar-explore-typeahead-copy">
                    <span className="name">{highlightMatch(item.display_name || item.ric, query)}</span>
                    <span className="ric-hint">{item.ric}</span>
                  </span>
                  <span className="cpar-explore-typeahead-meta">
                    <CparWarningsBar fitStatus={item.fit_status} warnings={item.warnings} compact />
                    <span className={`cpar-explore-typeahead-country${disabled ? " muted" : ""}`}>
                      {disabled ? "Ticker required" : item.hq_country_code || "—"}
                    </span>
                  </span>
                </button>
              );
            })}
          </div>
        ) : null}
      </div>

      {query.trim().length === 0 ? (
        <div className="detail-history-empty compact">
          Start typing to query the active cPAR package and choose one persisted row.
        </div>
      ) : isLoading && !data ? (
        <div className="detail-history-empty compact">Searching the active package…</div>
      ) : errorSummary ? (
        <div className="cpar-inline-message warning">
          <strong>{errorSummary.kind === "not_ready" ? "Package not ready." : "Search unavailable."}</strong>
          <span>{errorSummary.message}</span>
        </div>
      ) : data && data.results.length === 0 ? (
        <div className="detail-history-empty compact">No active-package results matched “{data.query}”.</div>
      ) : (
        <div className="cpar-explore-search-note">
          Disabled rows stay visible when the active package search hit has no ticker symbol, because the current
          detail and hedge routes remain ticker-keyed in this slice.
        </div>
      )}
    </section>
  );
}
