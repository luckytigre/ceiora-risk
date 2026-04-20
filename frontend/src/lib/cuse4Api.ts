// cUSE4-only frontend API helpers for the default route family.
// Prefer this over generic frontend API alias layers in cUSE4-owned frontend code.

import { ApiError, apiFetch } from "@/lib/apiTransport";
import { holdingsApiPath } from "@/lib/holdingsApi";

export { ApiError, apiFetch };

export const cuse4ApiPath = {
  exploreContext: () => "/api/cuse/explore/context",
  riskPageSnapshot: () => "/api/cuse/risk-page",
  riskPageExposureMode: (mode: string) => `/api/cuse/risk-page/exposure-mode?mode=${encodeURIComponent(mode)}`,
  riskPageCovariance: () => "/api/cuse/risk-page/covariance",
  portfolio: () => "/api/portfolio",
  portfolioWhatIf: () => "/api/portfolio/whatif",
  ...holdingsApiPath,
  exposures: (mode: string) => `/api/exposures?mode=${encodeURIComponent(mode)}`,
  exposureHistory: (factorId: string, years: number) =>
    `/api/exposures/history?factor_id=${encodeURIComponent(factorId)}&years=${years}`,
  risk: () => "/api/risk",
  universeTicker: (ticker: string) => `/api/universe/ticker/${encodeURIComponent(ticker)}`,
  universeTickerHistory: (ticker: string, years: number) =>
    `/api/universe/ticker/${encodeURIComponent(ticker)}/history?years=${years}`,
  universeSearch: (query: string, limit: number) =>
    `/api/universe/search?q=${encodeURIComponent(query)}&limit=${limit}`,
  universeFactors: () => "/api/universe/factors",
  healthDiagnostics: () => "/api/health/diagnostics",
  dataDiagnostics: (opts?: { includeExactRowCounts?: boolean; includeExpensiveChecks?: boolean }) => {
    const params = new URLSearchParams();
    if (opts?.includeExactRowCounts) params.set("include_exact_row_counts", "true");
    if (opts?.includeExpensiveChecks) params.set("include_expensive_checks", "true");
    const qs = params.toString();
    return qs ? `/api/data/diagnostics?${qs}` : "/api/data/diagnostics";
  },
  operatorStatus: () => "/api/operator/status",
  refreshProfile: (profile: string) => `/api/refresh?profile=${encodeURIComponent(profile)}`,
  refreshStatus: () => "/api/refresh/status",
} as const;

export const apiPath = cuse4ApiPath;
