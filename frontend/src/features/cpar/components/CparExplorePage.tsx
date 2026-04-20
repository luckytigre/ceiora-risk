"use client";

import { useCallback, useMemo, useState } from "react";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import CparTickerQuoteCard from "@/features/cpar/components/CparTickerQuoteCard";
import CparExploreWhatIfSection from "@/features/cpar/components/CparExploreWhatIfSection";
import {
  preloadCparTickerBundle,
  useCparExploreContext,
  useCparSearch,
  useCparTicker,
  useCparTickerHistory,
} from "@/hooks/useCparApi";
import { useDebouncedValue } from "@/hooks/useDebouncedValue";
import { readCparError } from "@/lib/cparTruth";
import type { CparSearchItem } from "@/lib/types/cpar";
import { normalizeTicker, type CparExplorePositionSummary } from "@/features/cpar/components/cparExploreUtils";

export default function CparExplorePage() {
  const [query, setQuery] = useState("");
  const [selectedInstrument, setSelectedInstrument] = useState<CparSearchItem | null>(null);
  const debouncedQuery = useDebouncedValue(query, 140);

  const { data: searchData, error: searchError, isLoading: searchLoading, isValidating: searchValidating } = useCparSearch(debouncedQuery, 10);
  const { data: exploreContextData, error: exploreContextError } = useCparExploreContext();
  const { data: tickerData, isLoading, error: tickerError } = useCparTicker(
    selectedInstrument?.ticker || null,
    selectedInstrument?.ric || null,
  );
  const {
    data: historyData,
    isLoading: historyLoading,
    error: historyError,
  } = useCparTickerHistory(
    selectedInstrument?.ticker || null,
    5,
    selectedInstrument?.ric || null,
  );

  const item = tickerData;
  const results = searchData?.results ?? [];

  const positionMap = useMemo(() => {
    const map = new Map<string, CparExplorePositionSummary>();
    for (const row of exploreContextData?.held_positions ?? []) {
      const key = String(row.ticker || row.ric || "").trim().toUpperCase();
      if (!key) continue;
      map.set(key, {
        shares: Number(row.quantity || 0),
        weight: Number(row.portfolio_weight || 0),
        market_value: Number(row.market_value || 0),
        long_short: row.long_short,
      });
    }
    return map;
  }, [exploreContextData]);

  const priceMap = useMemo(() => {
    const map = new Map<string, number>();
    for (const row of exploreContextData?.held_positions ?? []) {
      const key = String(row.ticker || row.ric || "").trim().toUpperCase();
      if (!key || row.price == null) continue;
      map.set(key, Number(row.price));
    }
    if (item?.ticker) {
      const key = String(item.ticker || item.ric || "").trim().toUpperCase();
      const price = Number(item.source_context.latest_price_context?.price || 0);
        if (key && price > 0) map.set(key, price);
    }
    return map;
  }, [exploreContextData, item]);

  const selectedPosition = selectedInstrument
    ? positionMap.get(String(selectedInstrument.ticker || "").toUpperCase())
      ?? positionMap.get(String(selectedInstrument.ric || "").toUpperCase())
    : undefined;

  const selectInstrument = useCallback((nextItem: CparSearchItem) => {
    preloadCparTickerBundle(nextItem.ticker || nextItem.ric, nextItem.ric, 5);
    setSelectedInstrument(nextItem);
    setQuery(nextItem.ticker || nextItem.ric);
  }, []);

  const handleSearchQueryChange = useCallback((nextQuery: string) => {
    setQuery(nextQuery);
    if (
      selectedInstrument
      && normalizeTicker(nextQuery) !== normalizeTicker(selectedInstrument.ticker || selectedInstrument.ric)
    ) {
      setSelectedInstrument(null);
    }
  }, [selectedInstrument]);

  const exploreContextState = exploreContextError ? readCparError(exploreContextError) : null;
  const searchState = searchError ? readCparError(searchError) : null;
  const tickerState = tickerError ? readCparError(tickerError) : null;
  const searchPending = query.trim() !== debouncedQuery.trim();
  const settledQuery = debouncedQuery.trim().toUpperCase();
  const searchRequestSettled = !searchPending && !searchLoading && !searchValidating;
  const searchResultsCurrent =
    settledQuery.length === 0
      || (
        searchRequestSettled
        && (searchData?.query ?? "").trim().toUpperCase() === settledQuery
      );
  const visibleSearchResults = searchResultsCurrent ? results : [];

  return (
    <div className="explore-page-stack" data-testid="cpar-explore-page">
      {exploreContextState && (
        <div className="explore-error">
          {exploreContextState.message}
        </div>
      )}
      {!isLoading && selectedInstrument && tickerState && (
        <div className="explore-error">
          {tickerState.message}
        </div>
      )}
      {searchState && query.trim().length > 0 && (
        <div className="explore-error">
          {searchState.message}
        </div>
      )}

      {isLoading && selectedInstrument && (
        <div className="analytics-stage-overlay" aria-hidden="true">
          <AnalyticsLoadingViz
            message={`Loading ${selectedInstrument.ticker || selectedInstrument.ric}...`}
            className="analytics-stage-inline"
          />
        </div>
      )}

      {item && !isLoading && (
        <CparTickerQuoteCard
          item={item}
          selectedPosition={selectedPosition}
          historyPoints={historyData?.points ?? []}
          historyLoading={historyLoading}
          historyError={historyError}
        />
      )}

      <CparExploreWhatIfSection
        priceMap={priceMap}
        selectedInstrument={selectedInstrument}
        searchQuery={query}
        searchLoading={searchPending || searchLoading || searchValidating}
        searchSettled={searchRequestSettled}
        onSearchQueryChange={handleSearchQueryChange}
        searchResults={visibleSearchResults}
        onSelectInstrument={selectInstrument}
        positionMap={positionMap}
        onPreviewInstrument={(item) => preloadCparTickerBundle(item.ticker || item.ric, item.ric, 5)}
      />
    </div>
  );
}
