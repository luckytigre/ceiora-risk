// cPAR-only frontend API helpers for the namespaced cPAR route family.
// Prefer this over generic frontend API alias layers in cPAR-owned frontend code.

import { ApiError, apiFetch } from "@/lib/apiTransport";

export { ApiError, apiFetch };

export const cparApiPath = {
  cparExploreContext: () => "/api/cpar/explore/context",
  cparMeta: () => "/api/cpar/meta",
  cparSearch: (query: string, limit: number) =>
    `/api/cpar/search?q=${encodeURIComponent(query)}&limit=${limit}`,
  cparTicker: (ticker: string, ric?: string | null) =>
    ric && ric.trim().length > 0
      ? `/api/cpar/ticker/${encodeURIComponent(ticker)}?ric=${encodeURIComponent(ric.trim())}`
      : `/api/cpar/ticker/${encodeURIComponent(ticker)}`,
  cparTickerHistory: (ticker: string, years: number, ric?: string | null) => {
    const params = new URLSearchParams();
    params.set("years", String(years));
    if (ric && ric.trim().length > 0) params.set("ric", ric.trim());
    return `/api/cpar/ticker/${encodeURIComponent(ticker)}/history?${params.toString()}`;
  },
  cparRisk: () => "/api/cpar/risk",
  cparFactorHistory: (factorId: string, years: number, mode: string) =>
    `/api/cpar/factors/history?factor_id=${encodeURIComponent(factorId)}&years=${years}&mode=${encodeURIComponent(mode)}`,
  cparPortfolioHedge: (accountId: string, mode: string) => {
    const params = new URLSearchParams();
    params.set("account_id", accountId.trim());
    params.set("mode", mode);
    return `/api/cpar/portfolio/hedge?${params.toString()}`;
  },
  cparPortfolioWhatIf: () => "/api/cpar/portfolio/whatif",
  cparExploreWhatIf: () => "/api/cpar/explore/whatif",
} as const;

export const apiPath = cparApiPath;
