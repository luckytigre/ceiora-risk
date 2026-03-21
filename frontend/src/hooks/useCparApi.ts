"use client";

// cPAR-only hook barrel for the namespaced cPAR frontend surfaces.
// Prefer this over `@/hooks/useApi` in cPAR-owned frontend code.

export {
  ApiError,
  applyPortfolioWhatIf,
  previewCparExploreWhatIf,
  useCparFactorHistory,
  useCparMeta,
  useCparTicker,
  useCparTickerHistory,
  useCparRisk,
  useCparPortfolioHedge,
  useCparPortfolioWhatIf,
  useCparSearch,
  useHoldingsAccounts,
  useHoldingsPositions,
} from "@/hooks/useApi";
