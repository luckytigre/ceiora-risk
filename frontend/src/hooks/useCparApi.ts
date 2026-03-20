"use client";

// cPAR-only hook barrel for the namespaced cPAR frontend surfaces.
// Prefer this over `@/hooks/useApi` in cPAR-owned frontend code.

export {
  ApiError,
  useCparHedge,
  useCparMeta,
  useCparPortfolioHedge,
  useCparPortfolioWhatIf,
  useCparSearch,
  useCparTicker,
  useHoldingsAccounts,
} from "@/hooks/useApi";
