"use client";

// cUSE4-only truth helper for the default risk/explore/positions surfaces.
// cPAR uses its own package-based truth semantics in `cparTruth.ts`.

import type {
  CuseRiskPageSummaryRiskData,
  ExposuresData,
  PortfolioData,
  RiskData,
  SourceDates,
} from "@/lib/types/cuse4";

function cleanDate(value: string | null | undefined): string | null {
  const text = String(value || "").trim();
  return text || null;
}

function pickDate(...values: Array<string | null | undefined>): string | null {
  for (const value of values) {
    const clean = cleanDate(value);
    if (clean) return clean;
  }
  return null;
}

export function compareIsoDate(a: string | null | undefined, b: string | null | undefined): number {
  const left = cleanDate(a);
  const right = cleanDate(b);
  if (!left && !right) return 0;
  if (!left) return -1;
  if (!right) return 1;
  return left.localeCompare(right);
}

export function formatAsOfDate(value: string | null | undefined): string {
  const isoDate = cleanDate(value);
  if (!isoDate) return "—";
  const parts = isoDate.split("-");
  if (parts.length === 3) {
    const year = Number(parts[0]);
    const month = Number(parts[1]);
    const day = Number(parts[2]);
    if (
      Number.isFinite(year)
      && Number.isFinite(month)
      && Number.isFinite(day)
    ) {
      const parsed = new Date(year, month - 1, day, 12, 0, 0);
      return parsed.toLocaleDateString("en-US", {
        year: "numeric",
        month: "short",
        day: "2-digit",
      });
    }
  }
  const parsed = new Date(isoDate);
  if (Number.isNaN(parsed.getTime())) return isoDate;
  return parsed.toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "2-digit",
  });
}

function mergeSourceDates(
  ...parts: Array<SourceDates | undefined | null>
): SourceDates {
  return {
    fundamentals_asof: pickDate(...parts.map((part) => part?.fundamentals_asof)),
    exposures_asof: pickDate(...parts.map((part) => part?.exposures_asof)),
    exposures_latest_available_asof: pickDate(
      ...parts.map((part) => part?.exposures_latest_available_asof),
    ),
    exposures_served_asof: pickDate(...parts.map((part) => part?.exposures_served_asof)),
    prices_asof: pickDate(...parts.map((part) => part?.prices_asof)),
    classification_asof: pickDate(...parts.map((part) => part?.classification_asof)),
  };
}

export interface AnalyticsTruthSummary {
  sourceDates: SourceDates;
  snapshotId: string | null;
  runId: string | null;
  refreshStartedAt: string | null;
  snapshotIds: string[];
  runIds: string[];
  snapshotsCoherent: boolean;
  exposuresServedAsOf: string | null;
  exposuresLatestAvailableAsOf: string | null;
  coreStateThroughDate: string | null;
  coreRebuildDate: string | null;
  updateAvailable: boolean;
  servedLoadingsBehindLatestSource: boolean;
  coreStateLaggingServedLoadings: boolean;
}

export function summarizeAnalyticsTruth({
  portfolio,
  risk,
  exposures,
}: {
  portfolio?: PortfolioData | null;
  risk?: RiskData | CuseRiskPageSummaryRiskData | null;
  exposures?: ExposuresData | null;
}): AnalyticsTruthSummary {
  const sourceDates = mergeSourceDates(
    portfolio?.source_dates,
    risk?.source_dates,
    exposures?.source_dates,
  );
  const snapshotIds = [
    portfolio?.snapshot_id,
    risk?.snapshot_id,
    exposures?.snapshot_id,
  ]
    .map((value) => cleanDate(value))
    .filter((value): value is string => Boolean(value));
  const runIds = [
    portfolio?.run_id,
    risk?.run_id,
    exposures?.run_id,
  ]
    .map((value) => cleanDate(value))
    .filter((value): value is string => Boolean(value));
  const exposuresServedAsOf = pickDate(
    sourceDates.exposures_served_asof,
    risk?.model_sanity?.served_loadings_asof,
    risk?.model_sanity?.coverage_date,
  );
  const exposuresLatestAvailableAsOf = pickDate(
    sourceDates.exposures_latest_available_asof,
    sourceDates.exposures_asof,
    risk?.model_sanity?.latest_loadings_available_asof,
    risk?.model_sanity?.latest_available_date,
  );
  const coreStateThroughDate = pickDate(
    risk?.risk_engine?.core_state_through_date,
    risk?.risk_engine?.factor_returns_latest_date,
    exposuresServedAsOf,
  );
  const coreRebuildDate = pickDate(
    risk?.risk_engine?.core_rebuild_date,
    risk?.risk_engine?.last_recompute_date,
  );
  const servedLoadingsBehindLatestSource = compareIsoDate(
    exposuresLatestAvailableAsOf,
    exposuresServedAsOf,
  ) > 0;
  const coreStateLaggingServedLoadings = compareIsoDate(
    exposuresServedAsOf,
    coreStateThroughDate,
  ) > 0;
  return {
    sourceDates,
    snapshotId: snapshotIds[0] ?? null,
    runId: runIds[0] ?? null,
    refreshStartedAt: pickDate(
      portfolio?.refresh_started_at,
      risk?.refresh_started_at,
      exposures?.refresh_started_at,
    ),
    snapshotIds,
    runIds,
    snapshotsCoherent: snapshotIds.length <= 1 || new Set(snapshotIds).size === 1,
    exposuresServedAsOf,
    exposuresLatestAvailableAsOf,
    coreStateThroughDate,
    coreRebuildDate,
    updateAvailable: Boolean(risk?.model_sanity?.update_available || servedLoadingsBehindLatestSource),
    servedLoadingsBehindLatestSource,
    coreStateLaggingServedLoadings,
  };
}

export function buildAnalyticsTruthCompactSummary(
  summary: AnalyticsTruthSummary,
  { prefix }: { prefix?: string | null } = {},
): string {
  const parts = [cleanDate(prefix)];
  const loadings = cleanDate(summary.exposuresServedAsOf);
  const coreThrough = cleanDate(summary.coreStateThroughDate);
  const rebuilt = cleanDate(summary.coreRebuildDate);
  if (loadings) parts.push(`Loadings = ${loadings}`);
  if (coreThrough) parts.push(`Core Through = ${coreThrough}`);
  if (rebuilt) parts.push(`Rebuilt = ${rebuilt}`);
  return parts.filter((part): part is string => Boolean(part)).join(" · ");
}
