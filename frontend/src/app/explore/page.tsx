"use client";

import { useCallback, useMemo, useState } from "react";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import ApiErrorState from "@/components/ApiErrorState";
import TickerQuoteCard from "@/features/explore/components/TickerQuoteCard";
import ExploreWhatIfSection from "@/features/whatif/ExploreWhatIfSection";
import {
  usePortfolio,
  useUniverseFactors,
  useUniverseSearch,
  useUniverseTicker,
  useUniverseTickerHistory,
} from "@/hooks/useApi";
import { useDebouncedValue } from "@/hooks/useDebouncedValue";
import type { FactorExposure } from "@/lib/types";

export default function ExplorePage() {
  const [query, setQuery] = useState("");
  const [selectedTicker, setSelectedTicker] = useState<string | null>(null);

  const debouncedQuery = useDebouncedValue(query, 220);
  const { data: searchData, error: searchError } = useUniverseSearch(debouncedQuery, 10);
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
  const priceMap = useMemo(() => {
    const map = new Map<string, number>();
    for (const p of portfolioData?.positions ?? []) {
      map.set(p.ticker.toUpperCase(), Number(p.price || 0));
    }
    if (item?.ticker) {
      map.set(item.ticker.toUpperCase(), Number(item.price || 0));
    }
    return map;
  }, [portfolioData, item]);

  const selectTicker = useCallback((ticker: string) => {
    setSelectedTicker(ticker);
    setQuery(ticker);
  }, []);

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

  if (factorsError || portfolioError) {
    return <ApiErrorState title="Universe Data Not Ready" error={factorsError || portfolioError} />;
  }

  return (
    <div className="explore-page-stack">
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

      {isLoading && selectedTicker && <AnalyticsLoadingViz message={`Loading ${selectedTicker}...`} />}

      {item && !isLoading && (
        <>
          <TickerQuoteCard
            item={item}
            selectedPosition={selectedPosition}
            historyPoints={historyPoints}
            historyLoading={historyLoading}
            historyError={historyError}
            chartFactors={chartFactors}
          />
        </>
      )}

      {/* What-if builder — entry form, staging queue, expandable results */}
      <ExploreWhatIfSection
        item={item}
        priceMap={priceMap}
        searchQuery={query}
        onSearchQueryChange={setQuery}
        searchResults={results}
        searchError={searchError}
        onSelectTicker={selectTicker}
        positionMap={positionMap}
        isLoadingTicker={isLoading}
        tickerError={tickerError}
      />
    </div>
  );
}
