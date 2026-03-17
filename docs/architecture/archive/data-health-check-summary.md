# Data Health Check Summary

Date: 2026-03-16  
Mode: Read-only

## Bottom Line

The current served snapshot is usable and internally aligned, but the underlying data platform is not fully healthy.

The main issues are not random one-off glitches. They cluster in a few systemic areas:

- Neon is behind local for core durable model outputs
- the local source archive has intermittent row-count spikes and low-quality extra names
- recent durable factor-return history has gaps
- stale cache-era tables are still driving some metadata
- Neon metadata appears polluted by test rows

## Main Issues Found

1. Neon factor returns and covariance are stale or broken relative to local.
2. Neon raw cross-section history is missing seven repaired latest-date names, including `AAPL.OQ`.
3. Local prices and raw cross-section history show suspicious spikes on `2026-03-04` and `2026-03-13`.
4. `estu_membership_daily` has a zero-membership day on `2026-03-03`.
5. `model_factor_returns_daily` has missing recent business dates.
6. Exposure and diagnostics metadata still rely on stale cache-era tables even when current values are correct.
7. Neon `model_run_metadata` includes test-like run IDs.

## Systemic vs Isolated

Systemic:

- Neon/local durability drift
- intermittent source-history quality issues
- stale cache metadata
- recent factor-history gaps
- test contamination risk in Neon metadata

Isolated or narrower:

- seven repaired names missing from Neon latest raw history
- lineage-heavy ticker / ISIN mismatches in older LSEG spot checks

## Confidence

High confidence:

- table freshness counts
- latest snapshot alignment
- Neon/local drift
- stale cache-table dates
- ESTU zero-membership anomaly
- factor-return gaps

Medium confidence:

- exact upstream cause of the intermittent source-name spikes
- exact workflow that allowed test rows into Neon metadata

## Recommended Next 3-5 Investigations

1. Trace the intermittent source-ingest or repair path that admits the extra legacy / low-quality names on selected dates.
2. Trace the publish or mirroring path that left Neon factor returns and covariance stale.
3. Explain the missing durable factor-return dates from `2026-03-04` through `2026-03-09`.
4. Decide whether stale cache-era tables should be rebuilt or removed from all live metadata paths.
5. Audit test isolation around Neon writes, especially `model_run_metadata`.

## Answer To The Core Question

This does not look like a wholly broken platform, but it also does not look fully healthy.

The served application is currently being held together by:

- a good latest snapshot
- fallback logic that prefers newer durable truth over stale cache-era sources

That is workable in the short term, but it means several deeper data-integrity issues are still present underneath the current UI.
