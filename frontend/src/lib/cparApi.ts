// cPAR-only frontend API helpers for the namespaced cPAR route family.
// Prefer this over `@/lib/api` in cPAR-owned frontend code.

import { ApiError, apiFetch, apiPath as legacyApiPath } from "@/lib/api";

export { ApiError, apiFetch };

export const cparApiPath = {
  cparMeta: legacyApiPath.cparMeta,
  cparSearch: legacyApiPath.cparSearch,
  cparTicker: legacyApiPath.cparTicker,
  cparHedge: legacyApiPath.cparHedge,
  cparPortfolioHedge: legacyApiPath.cparPortfolioHedge,
  cparPortfolioWhatIf: legacyApiPath.cparPortfolioWhatIf,
  // Shared infrastructure reused by the cPAR risk workspace.
  holdingsAccounts: legacyApiPath.holdingsAccounts,
} as const;

export const apiPath = cparApiPath;
