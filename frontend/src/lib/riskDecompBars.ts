"use client";

import type { FactorDetail, Position, RiskShares } from "@/lib/types/analytics";
import type { CparFactorChartRow, CparLoading, CparPortfolioPositionRow, CparRiskShares } from "@/lib/types/cpar";

export const RISK_DECOMP_SECTION_TITLE = "Factor Decomposition";
export const RAW_LOADING_SUBTITLE =
  "Absolute raw loading footprint split across market, industry, style, and implied idiosyncratic loading.";
export const VOL_SCALED_SUBTITLE =
  "Vol-scaled footprint split across market, industry, style, and idiosyncratic components.";

type SharedRiskBucket = "market" | "industry" | "style";

function normalizeSharedBuckets(buckets: Record<SharedRiskBucket, number>): RiskShares {
  const total = buckets.market + buckets.industry + buckets.style + (buckets as RiskShares).idio;
  if (total <= 1e-12) {
    return { market: 0, industry: 0, style: 0, idio: 0 };
  }
  return {
    market: Number(((buckets.market / total) * 100).toFixed(1)),
    industry: Number(((buckets.industry / total) * 100).toFixed(1)),
    style: Number(((buckets.style / total) * 100).toFixed(1)),
    idio: Number((((buckets as RiskShares).idio / total) * 100).toFixed(1)),
  };
}

function resolveReferenceFactorVol(values: Array<number | null | undefined>): number {
  for (const value of values) {
    const numeric = Number(value || 0);
    if (Number.isFinite(numeric) && numeric > 1e-12) return numeric;
  }
  return 0;
}

function portfolioSpecificVolFromPositions(positions: Position[] | null | undefined): number {
  let specificVariance = 0;
  for (const row of positions || []) {
    const weight = Number(row?.weight || 0);
    const specificVar = Number(row?.specific_var ?? NaN);
    const specificVol = Number(row?.specific_vol ?? NaN);
    const resolvedVar = Number.isFinite(specificVar) && specificVar >= 0
      ? specificVar
      : Number.isFinite(specificVol) && specificVol >= 0
        ? specificVol ** 2
        : 0;
    specificVariance += (weight ** 2) * resolvedVar;
  }
  return Math.sqrt(Math.max(0, specificVariance));
}

function portfolioSpecificVolFromCparRows(rows: CparPortfolioPositionRow[] | null | undefined): number {
  let specificVariance = 0;
  for (const row of rows || []) {
    const weight = Number(row?.portfolio_weight || 0);
    const specificVar = Number(row?.specific_variance_proxy ?? NaN);
    const specificVol = Number(row?.specific_volatility_proxy ?? NaN);
    const resolvedVar = Number.isFinite(specificVar) && specificVar >= 0
      ? specificVar
      : Number.isFinite(specificVol) && specificVol >= 0
        ? specificVol ** 2
        : 0;
    specificVariance += (weight ** 2) * resolvedVar;
  }
  return Math.sqrt(Math.max(0, specificVariance));
}

export function deriveRawLoadingSharesFromRiskDetails(
  rows: FactorDetail[] | null | undefined,
  positions: Position[] | null | undefined,
): RiskShares {
  const buckets: RiskShares = {
    market: 0,
    industry: 0,
    style: 0,
    idio: 0,
  };
  for (const row of rows || []) {
    const category = String(row?.category || "").toLowerCase();
    const magnitude = Math.abs(Number(row?.exposure || 0));
    if (!Number.isFinite(magnitude) || magnitude <= 1e-12) continue;
    if (category === "market") buckets.market += magnitude;
    else if (category === "industry") buckets.industry += magnitude;
    else if (category === "style") buckets.style += magnitude;
  }
  const marketVol = resolveReferenceFactorVol(
    (rows || [])
      .filter((row) => String(row?.category || "").toLowerCase() === "market")
      .map((row) => row?.factor_vol)
      .concat((rows || []).map((row) => row?.factor_vol)),
  );
  const specificVol = portfolioSpecificVolFromPositions(positions);
  buckets.idio = marketVol > 1e-12 ? Math.abs(specificVol / marketVol) : 0;
  return normalizeSharedBuckets(buckets);
}

export function deriveRawLoadingSharesFromCparLoadings(
  rows: CparLoading[] | null | undefined,
  factorChartRows: CparFactorChartRow[] | null | undefined,
  positions: CparPortfolioPositionRow[] | null | undefined,
): CparRiskShares {
  const buckets: CparRiskShares = {
    market: 0,
    industry: 0,
    style: 0,
    idio: 0,
  };
  for (const row of rows || []) {
    const group = String(row?.group || "").toLowerCase();
    const magnitude = Math.abs(Number(row?.beta || 0));
    if (!Number.isFinite(magnitude) || magnitude <= 1e-12) continue;
    if (group === "market") buckets.market += magnitude;
    else if (group === "sector") buckets.industry += magnitude;
    else if (group === "style") buckets.style += magnitude;
  }
  const marketVol = resolveReferenceFactorVol(
    (factorChartRows || [])
      .filter((row) => String(row?.group || "").toLowerCase() === "market")
      .map((row) => row?.factor_volatility)
      .concat((factorChartRows || []).map((row) => row?.factor_volatility)),
  );
  const specificVol = portfolioSpecificVolFromCparRows(positions);
  buckets.idio = marketVol > 1e-12 ? Math.abs(specificVol / marketVol) : 0;
  return normalizeSharedBuckets(buckets);
}
