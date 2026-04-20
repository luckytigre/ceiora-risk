"use client";

// cPAR-only hook barrel for the namespaced cPAR frontend surfaces.
// Prefer this over generic frontend API-hook alias layers in cPAR-owned frontend code.

import useSWR, { preload } from "swr";
import { ApiError, apiFetch } from "@/lib/apiTransport";
import { cparApiPath } from "@/lib/cparApi";
import type {
  CparExploreContextData,
  CparExploreWhatIfData,
  CparFactorHistoryData,
  CparFactorHistoryMode,
  CparHedgeMode,
  CparMetaData,
  CparPortfolioHedgeData,
  CparPortfolioWhatIfData,
  CparRiskData,
  CparSearchData,
  CparTickerDetailData,
  CparTickerHistoryData,
} from "@/lib/types/cpar";

export { ApiError };

const SWR_OPTS = {
  revalidateOnFocus: false,
  revalidateOnReconnect: false,
  shouldRetryOnError: false,
  errorRetryCount: 0,
  refreshInterval: 0,
};

export function useCparMeta() {
  return useSWR<CparMetaData>(cparApiPath.cparMeta(), apiFetch, SWR_OPTS);
}

export function useCparSearch(query: string, limit = 10) {
  const q = query.trim();
  const key = q.length > 0 ? cparApiPath.cparSearch(q, limit) : null;
  return useSWR<CparSearchData>(key, apiFetch, {
    ...SWR_OPTS,
    keepPreviousData: true,
  });
}

export function useCparExploreContext(enabled = true) {
  return useSWR<CparExploreContextData>(
    enabled ? cparApiPath.cparExploreContext() : null,
    apiFetch,
    SWR_OPTS,
  );
}

export function useCparTicker(ticker: string | null, ric?: string | null) {
  const cleanTicker = ticker?.trim().toUpperCase() || null;
  const cleanRic = ric?.trim().toUpperCase() || null;
  const key = cleanTicker ? cparApiPath.cparTicker(cleanTicker, cleanRic) : null;
  return useSWR<CparTickerDetailData>(key, apiFetch, SWR_OPTS);
}

export function useCparTickerHistory(ticker: string | null, years = 5, ric?: string | null) {
  const cleanTicker = ticker?.trim().toUpperCase() || null;
  const cleanRic = ric?.trim().toUpperCase() || null;
  const key = cleanTicker ? cparApiPath.cparTickerHistory(cleanTicker, years, cleanRic) : null;
  return useSWR<CparTickerHistoryData>(key, apiFetch, SWR_OPTS);
}

export function useCparRisk(enabled = true) {
  return useSWR<CparRiskData>(enabled ? cparApiPath.cparRisk() : null, apiFetch, SWR_OPTS);
}

export function useCparFactorHistory(
  factorId: string | null,
  years = 5,
  mode: CparFactorHistoryMode = "market_adjusted",
  enabled = true,
) {
  const cleanFactorId = factorId?.trim().toUpperCase() || null;
  const key = enabled && cleanFactorId ? cparApiPath.cparFactorHistory(cleanFactorId, years, mode) : null;
  return useSWR<CparFactorHistoryData>(key, apiFetch, SWR_OPTS);
}

export function useCparPortfolioHedge(
  accountId: string | null,
  mode: CparHedgeMode,
  enabled = true,
) {
  const cleanAccountId = accountId?.trim() || null;
  const key = enabled && cleanAccountId ? cparApiPath.cparPortfolioHedge(cleanAccountId, mode) : null;
  return useSWR<CparPortfolioHedgeData>(key, apiFetch, SWR_OPTS);
}

export function useCparPortfolioWhatIf(
  accountId: string | null,
  mode: CparHedgeMode,
  scenarioRows: Array<{ ric: string; ticker?: string | null; quantity_delta: number }>,
  enabled = true,
) {
  const cleanAccountId = accountId?.trim() || null;
  const cleanScenarioRows = scenarioRows.map((row) => ({
    ric: row.ric.trim().toUpperCase(),
    ticker: row.ticker?.trim().toUpperCase() || null,
    quantity_delta: row.quantity_delta,
  }));
  const serializedRows = JSON.stringify(cleanScenarioRows);
  const key = enabled && cleanAccountId && cleanScenarioRows.length > 0
    ? [cparApiPath.cparPortfolioWhatIf(), cleanAccountId, mode, serializedRows]
    : null;
  return useSWR<CparPortfolioWhatIfData>(
    key,
    ([path, account_id, hedgeMode, rows]) => apiFetch<CparPortfolioWhatIfData>(String(path), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        account_id,
        mode: hedgeMode,
        scenario_rows: JSON.parse(String(rows)),
      }),
    }),
    SWR_OPTS,
  );
}

export async function previewCparExploreWhatIf(payload: {
  scenario_rows: Array<{
    account_id: string;
    ticker?: string | null;
    ric: string;
    quantity: number;
    source?: string | null;
  }>;
}): Promise<CparExploreWhatIfData> {
  return apiFetch<CparExploreWhatIfData>(cparApiPath.cparExploreWhatIf(), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export function preloadCparTickerBundle(ticker: string | null, ric?: string | null, years = 5) {
  const cleanTicker = ticker?.trim().toUpperCase() || null;
  const cleanRic = ric?.trim().toUpperCase() || null;
  if (!cleanTicker) return;
  void preload(cparApiPath.cparTicker(cleanTicker, cleanRic), apiFetch);
  void preload(cparApiPath.cparTickerHistory(cleanTicker, years, cleanRic), apiFetch);
}
