"use client";

import type { FactorDetail, RiskShares } from "@/lib/types/analytics";
import type { CparLoading, CparRiskShares } from "@/lib/types/cpar";

type SharedRiskBucket = "market" | "industry" | "style";

function normalizeSharedBuckets(buckets: Record<SharedRiskBucket, number>): RiskShares {
  const total = buckets.market + buckets.industry + buckets.style;
  if (total <= 1e-12) {
    return { market: 0, industry: 0, style: 0, idio: 0 };
  }
  return {
    market: Number(((buckets.market / total) * 100).toFixed(1)),
    industry: Number(((buckets.industry / total) * 100).toFixed(1)),
    style: Number(((buckets.style / total) * 100).toFixed(1)),
    idio: 0,
  };
}

export function deriveRawLoadingSharesFromRiskDetails(rows: FactorDetail[] | null | undefined): RiskShares {
  const buckets: Record<SharedRiskBucket, number> = {
    market: 0,
    industry: 0,
    style: 0,
  };
  for (const row of rows || []) {
    const category = String(row?.category || "").toLowerCase();
    const magnitude = Math.abs(Number(row?.exposure || 0));
    if (!Number.isFinite(magnitude) || magnitude <= 1e-12) continue;
    if (category === "market") buckets.market += magnitude;
    else if (category === "industry") buckets.industry += magnitude;
    else if (category === "style") buckets.style += magnitude;
  }
  return normalizeSharedBuckets(buckets);
}

export function deriveRawLoadingSharesFromCparLoadings(rows: CparLoading[] | null | undefined): CparRiskShares {
  const buckets: Record<SharedRiskBucket, number> = {
    market: 0,
    industry: 0,
    style: 0,
  };
  for (const row of rows || []) {
    const group = String(row?.group || "").toLowerCase();
    const magnitude = Math.abs(Number(row?.beta || 0));
    if (!Number.isFinite(magnitude) || magnitude <= 1e-12) continue;
    if (group === "market") buckets.market += magnitude;
    else if (group === "sector") buckets.industry += magnitude;
    else if (group === "style") buckets.style += magnitude;
  }
  return normalizeSharedBuckets(buckets);
}
