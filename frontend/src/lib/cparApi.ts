// cPAR-only frontend API helpers for the namespaced cPAR route family.
// Prefer this over `@/lib/api` in cPAR-owned frontend code.

import { ApiError, apiFetch, apiPath as legacyApiPath } from "@/lib/api";

export { ApiError, apiFetch };

export const cparApiPath = {
  cparMeta: legacyApiPath.cparMeta,
  cparSearch: legacyApiPath.cparSearch,
  cparTicker: legacyApiPath.cparTicker,
  cparTickerHistory: legacyApiPath.cparTickerHistory,
  cparRisk: legacyApiPath.cparRisk,
  cparFactorHistory: legacyApiPath.cparFactorHistory,
  cparPortfolioHedge: legacyApiPath.cparPortfolioHedge,
  cparPortfolioWhatIf: legacyApiPath.cparPortfolioWhatIf,
  cparExploreWhatIf: legacyApiPath.cparExploreWhatIf,
  // Shared infrastructure reused by the cPAR risk workspace.
  holdingsAccounts: legacyApiPath.holdingsAccounts,
  holdingsPositions: legacyApiPath.holdingsPositions,
  portfolioWhatIfApply: legacyApiPath.portfolioWhatIfApply,
} as const;

export const apiPath = cparApiPath;
