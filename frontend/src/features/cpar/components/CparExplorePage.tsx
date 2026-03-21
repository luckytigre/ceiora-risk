"use client";

import { useCallback, useMemo, useState } from "react";
import CparTickerQuoteCard from "@/features/cpar/components/CparTickerQuoteCard";
import CparExploreWhatIfSection from "@/features/cpar/components/CparExploreWhatIfSection";
import { useCparRisk, useCparSearch, useCparTicker, useCparTickerHistory } from "@/hooks/useCparApi";
import { readCparError } from "@/lib/cparTruth";
import type { CparSearchItem } from "@/lib/types/cpar";
import { normalizeTicker, type CparExplorePositionSummary } from "@/features/cpar/components/cparExploreUtils";

export default function CparExplorePage() {
  const [query, setQuery] = useState("");
  const [selectedInstrument, setSelectedInstrument] = useState<CparSearchItem | null>(null);

  const { data: searchData, error: searchError, isLoading: searchLoading, isValidating: searchValidating } = useCparSearch(query, 10);
  const { data: riskData, error: riskError } = useCparRisk();
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
    for (const row of riskData?.positions ?? []) {
      const key = String(row.ticker || row.ric || "").trim().toUpperCase();
      if (!key) continue;
      map.set(key, {
        shares: Number(row.quantity || 0),
        weight: Number(row.portfolio_weight || 0),
        market_value: Number(row.market_value || 0),
        long_short: Number(row.quantity || 0) >= 0 ? "LONG" : "SHORT",
      });
    }
    return map;
  }, [riskData]);

  const priceMap = useMemo(() => {
    const map = new Map<string, number>();
    for (const row of riskData?.positions ?? []) {
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
  }, [item, riskData]);

  const selectedPosition = selectedInstrument?.ticker
    ? positionMap.get(String(selectedInstrument.ticker).toUpperCase())
    : undefined;

  const selectInstrument = useCallback((nextItem: CparSearchItem) => {
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

  const riskState = riskError ? readCparError(riskError) : null;
  const searchState = searchError ? readCparError(searchError) : null;
  const tickerState = tickerError ? readCparError(tickerError) : null;

  return (
    <div className="explore-page-stack" data-testid="cpar-explore-page">
      {riskState && (
        <div className="explore-error">
          {riskState.message}
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
        <div className="cpar-explore-loading-chip" role="status" aria-live="polite">
          Loading {selectedInstrument.ticker || selectedInstrument.ric}...
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
        searchLoading={searchLoading || searchValidating}
        onSearchQueryChange={handleSearchQueryChange}
        searchResults={results}
        onSelectInstrument={selectInstrument}
        positionMap={positionMap}
      />
    </div>
  );
}
