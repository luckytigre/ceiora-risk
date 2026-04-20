"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import TickerQuoteCard from "@/features/explore/components/TickerQuoteCard";
import ExploreWhatIfSection from "@/features/whatif/ExploreWhatIfSection";
import {
  preloadUniverseFactors,
  preloadUniverseTickerDetail,
  preloadUniverseTickerHistory,
  useCuseExploreContext,
  useUniverseFactors,
  useUniverseSearch,
  useUniverseTicker,
  useUniverseTickerHistory,
} from "@/hooks/useCuse4Api";
import { useDebouncedValue } from "@/hooks/useDebouncedValue";
import type { FactorExposure } from "@/lib/types/cuse4";

export default function ExplorePage() {
  const [query, setQuery] = useState("");
  const [selectedTicker, setSelectedTicker] = useState<string | null>(null);
  const [quoteExpanded, setQuoteExpanded] = useState(false);

  const debouncedQuery = useDebouncedValue(query, 140);
  const {
    data: searchData,
    error: searchError,
    isLoading: searchLoading,
    isValidating: searchValidating,
  } = useUniverseSearch(debouncedQuery, 10);
  const { data: tickerData, isLoading, error: tickerError } = useUniverseTicker(selectedTicker);
  const {
    data: historyData,
    isLoading: historyLoading,
    error: historyError,
  } = useUniverseTickerHistory(selectedTicker, 5, quoteExpanded);
  const {
    data: factorsData,
    error: factorsError,
    isLoading: factorsLoading,
  } = useUniverseFactors(quoteExpanded && Boolean(selectedTicker));
  const { data: exploreContextData, error: exploreContextError } = useCuseExploreContext();

  const item = tickerData?.item;
  const factorVols = factorsData?.factor_vols ?? {};
  const factorCatalog = factorsData?.factor_catalog ?? [];
  const results = searchData?.results ?? [];
  const historyPoints = historyData?.points ?? [];
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

  // Build a quick lookup of held positions by ticker
  const positionMap = useMemo(() => {
    const map = new Map<string, { shares: number; weight: number; market_value: number; long_short: string }>();
    for (const p of exploreContextData?.held_positions ?? []) {
      map.set(p.ticker.toUpperCase(), {
        shares: p.shares,
        weight: p.weight,
        market_value: p.market_value,
        long_short: p.long_short,
      });
    }
    return map;
  }, [exploreContextData]);

  const selectedPosition = selectedTicker ? positionMap.get(selectedTicker.toUpperCase()) : undefined;
  const priceMap = useMemo(() => {
    const map = new Map<string, number>();
    for (const p of exploreContextData?.held_positions ?? []) {
      map.set(p.ticker.toUpperCase(), Number(p.price || 0));
    }
    if (item?.ticker) {
      map.set(item.ticker.toUpperCase(), Number(item.price || 0));
    }
    return map;
  }, [exploreContextData, item]);

  const selectTicker = useCallback((ticker: string) => {
    preloadUniverseTickerDetail(ticker);
    setQuoteExpanded(false);
    setSelectedTicker(ticker);
    setQuery(ticker);
  }, []);

  const handleSearchQueryChange = useCallback((nextQuery: string) => {
    setQuery(nextQuery);
    if (selectedTicker && nextQuery.trim().toUpperCase() !== selectedTicker.toUpperCase()) {
      setQuoteExpanded(false);
      setSelectedTicker(null);
    }
  }, [selectedTicker]);

  const chartFactors: FactorExposure[] = useMemo(() => {
    if (!item) return [];
    const exposures = item.exposures ?? {};
    const sensitivities = item.sensitivities ?? {};
    return Object.entries(exposures).map(([factorId, rawVal]) => {
      const loading = Number(rawVal) || 0;
      const fv = Number(factorVols[factorId] ?? 0) || 0;
      return {
        factor_id: factorId,
        value: loading,
        factor_vol: fv,
        drilldown: [
          {
            ticker: item.ticker,
            weight: loading >= 0 ? 1 : -1,
            exposure: loading,
            sensitivity: Number(sensitivities[factorId] ?? loading * fv) || 0,
            contribution: loading,
            model_status: item.model_status,
            exposure_origin: item.exposure_origin,
          },
        ],
      };
    });
  }, [item, factorVols]);

  useEffect(() => {
    if (!selectedTicker) return;
    let timeoutId: ReturnType<typeof setTimeout> | null = null;
    let idleId: number | null = null;
    const supportsIdleCallback =
      typeof window !== "undefined" && typeof window.requestIdleCallback === "function";

    const warmDeferredQuoteData = () => {
      preloadUniverseTickerHistory(selectedTicker, 5);
      preloadUniverseFactors();
    };

    if (supportsIdleCallback) {
      idleId = window.requestIdleCallback(() => {
        warmDeferredQuoteData();
      }, { timeout: 250 });
    } else {
      timeoutId = globalThis.setTimeout(() => {
        warmDeferredQuoteData();
      }, 180);
    }

    return () => {
      if (idleId != null && typeof window !== "undefined" && typeof window.cancelIdleCallback === "function") {
        window.cancelIdleCallback(idleId);
      }
      if (timeoutId != null) {
        globalThis.clearTimeout(timeoutId);
      }
    };
  }, [selectedTicker]);

  return (
    <div className="explore-page-stack">
      {exploreContextError && (
        <div className="explore-error">
          Held-position context is temporarily unavailable. Explore remains available, but current holding badges may be incomplete.
        </div>
      )}
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

      {isLoading && selectedTicker && (
        <div className="analytics-stage-overlay" aria-hidden="true">
          <AnalyticsLoadingViz message={`Loading ${selectedTicker}...`} className="analytics-stage-inline" />
        </div>
      )}

      {item && !isLoading && (
        <>
          <TickerQuoteCard
            item={item}
            expanded={quoteExpanded}
            onExpandedChange={setQuoteExpanded}
            historyRequested={quoteExpanded}
            selectedPosition={selectedPosition}
            historyPoints={historyPoints}
            historyLoading={historyLoading}
            historyError={historyError}
            chartFactors={chartFactors}
            factorCatalog={factorCatalog}
            factorVisualsLoading={factorsLoading}
            factorVisualsUnavailable={Boolean(factorsError)}
          />
        </>
      )}

      {/* What-if builder — entry form, staging queue, expandable results */}
      <ExploreWhatIfSection
        item={item}
        priceMap={priceMap}
        searchQuery={query}
        searchLoading={searchPending || searchLoading || searchValidating}
        searchSettled={searchRequestSettled}
        onSearchQueryChange={handleSearchQueryChange}
        searchResults={visibleSearchResults}
        onSelectTicker={selectTicker}
        positionMap={positionMap}
        onPreviewTicker={preloadUniverseTickerDetail}
      />
    </div>
  );
}
