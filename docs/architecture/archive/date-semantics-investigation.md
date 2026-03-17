# Date Semantics Investigation

Date: 2026-03-17
Status: Implemented semantics cleanup recorded
Owner: Codex

## Summary

This investigation originally identified one main problem: the frontend was collapsing multiple timelines into one vague `Model` date.

That ambiguity has now been corrected. The system now presents three distinct timelines clearly:

1. Core risk-state timeline
   - factor returns / covariance / specific risk
   - core rebuild date
   - lagged estimation exposure anchor
2. Daily serving timeline
   - current served loadings / projected portfolio outputs
   - holdings / prices
3. PIT source timeline
   - fundamentals
   - classifications

The backend field model was already mostly sound. The work was primarily a semantics, contract, and UI cleanup.

## Current Fields And Current Usage

| Field | Current source | True meaning | Current display | Misleading now? |
| --- | --- | --- | --- | --- |
| `risk_engine.core_state_through_date` | `backend/analytics/refresh_metadata.py` via `risk_engine_meta` | Latest return date covered by the current core risk package | Exposures/Positions compact line as `Core Through`; Health page `Core State Through`; Operator Status `Core Through` | No |
| `risk_engine.core_rebuild_date` | `backend/analytics/refresh_metadata.py` via `risk_engine_meta` | Date the core risk package was last rebuilt | Exposures/Positions compact line as `Rebuilt`; Health page `Core Rebuilt`; Operator Status `Core Rebuilt` | No |
| `risk_engine.estimation_exposure_anchor_date` | `backend/analytics/refresh_metadata.py` | Lagged exposure snapshot basis used for the current stable core package | Health page `Estimation Anchor`; Operator Status `Estimation Anchor` | No |
| `risk_engine.factor_returns_latest_date` | compatibility alias in served payloads | Legacy alias for `core_state_through_date` | Fallback-only compatibility reader | Acceptable as alias only |
| `risk_engine.last_recompute_date` | compatibility alias in served payloads | Legacy alias for `core_rebuild_date` | Fallback-only compatibility reader | Acceptable as alias only |
| `model_sanity.served_loadings_asof` | `backend/analytics/refresh_metadata.py` from refreshed eligibility summary | Current served loadings date used for dashboard-facing payloads | Health page current loadings summary via canonical field | No |
| `model_sanity.latest_loadings_available_asof` | refreshed eligibility summary | Latest available loadings source date, even if the served snapshot uses an older well-covered date | Health page update prompt and freshness logic via canonical field | No |
| `model_sanity.coverage_date` | compatibility alias | Legacy alias for `served_loadings_asof` | Fallback-only compatibility reader | Acceptable as alias only |
| `model_sanity.latest_available_date` | compatibility alias | Legacy alias for `latest_loadings_available_asof` | Fallback-only compatibility reader | Acceptable as alias only |
| `source_dates.exposures_served_asof` | `refresh_metadata.serving_source_dates(...)` | Current served loadings / served exposure snapshot date | Exposures/Positions compact line as `Loadings`; What-if panel served exposures note | No |
| `source_dates.exposures_latest_available_asof` | `refresh_metadata.serving_source_dates(...)` | Latest available loadings source date | Health/operator freshness logic as `Loadings Available` | No |
| `source_dates.exposures_asof` | compatibility alias in `source_dates` | Legacy alias for `exposures_latest_available_asof` | Fallback-only compatibility reader | Acceptable as alias only |
| `source_dates.prices_asof` | `core_reads.load_source_dates()` | Latest canonical price source date | Operator Status / Positions source dates | No |
| `source_dates.fundamentals_asof` | `core_reads.load_source_dates()` | Latest policy-compliant PIT fundamentals anchor | Operator Status / Positions source dates | No |
| `source_dates.classification_asof` | `core_reads.load_source_dates()` | Latest policy-compliant PIT classification anchor | Operator Status / Positions source dates | No |

## Where The Frontend Now Reflects The Cleanup

### `frontend/src/lib/analyticsTruth.ts`

The compact analytics summary now renders:

- `Loadings = ...`
- `Core Through = ...`
- `Rebuilt = ...`

It no longer emits a generic `Model = ...` date.

### `frontend/src/app/health/page.tsx`

The Health page now separates:

- `R-Squared`
- `Core State Through`
- `Estimation Anchor`
- `Core Rebuilt`
- `Lag Policy`

It also describes daily serving loadings separately from the frozen stable core package.

### `frontend/src/features/health/OperatorStatusSection.tsx`

Operator Status now separates:

- `Authoritative Source Recency`
  - `Prices`
  - `Fundamentals`
  - `Classification`
  - `Loadings Available`
- `Core Risk State`
  - `Core Through`
  - `Estimation Anchor`
  - `Core Rebuilt`
  - `Lag Policy`
  - `Risk Engine`

So factor-return/core dates are no longer presented as raw source-recency fields.

## Remaining Compatibility Rules

These older fields still exist for compatibility and fallback decoding:

- `factor_returns_latest_date`
- `last_recompute_date`
- `coverage_date`
- `latest_available_date`
- `exposures_asof`

They should not drive new UI labels or docs.

## Conclusion

The investigation is now reflected in current behavior:

1. the backend exposes explicit core, serving, and PIT date semantics
2. the frontend labels those timelines explicitly
3. compatibility aliases remain available without defining user-facing meaning
