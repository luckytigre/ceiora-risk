# Area 3 Findings — Stale Cache-Era Metadata And Misleading Read Surfaces

Date: 2026-03-17
Scope: focused corrective pass only

## Metadata Surface Inventory

### Durable source-of-truth

- `backend/runtime/data.db:model_factor_returns_daily`
  - current factor-return coverage date, `cross_section_n`, `eligible_n`, `coverage`
- `backend/runtime/data.db:serving_payload_current`
  - current served dashboard payloads for:
    - `risk`
    - `exposures`
    - `portfolio`
    - `eligibility`
    - `model_sanity`
    - `refresh_meta`
- authoritative source recency from `backend.data.core_reads.load_source_dates()`
  - prices
  - PIT fundamentals
  - PIT classifications
  - latest exposure availability

### Legacy cache-era metadata tables

- `backend/runtime/cache.db:daily_factor_returns`
  - frozen at `2017-12-22`
  - no longer valid as current factor-coverage truth
- `backend/runtime/cache.db:daily_universe_eligibility_summary`
  - frozen at `2017-01-04`
  - no longer valid as current eligibility-summary truth

### Derived read models

- `/api/risk` and `/api/exposures`
  - read from `serving_payload_current`
- `/api/data/diagnostics`
  - local diagnostics surface that inspects SQLite/cache state and summarizes active metadata
- `model_sanity`
  - derived during refresh from eligibility summary + current risk decomposition

### Intentionally compatibility-only after this pass

- `daily_factor_returns`
- `daily_universe_eligibility_summary`

They remain useful for compatibility and forensics, but not as primary current metadata truth for the corrected surfaces in this pass.

## First Bad Value Traces

### 1) Stale factor coverage metadata in Exposures

Symptom:
- `/api/exposures?mode=raw` reported:
  - `coverage_date = 2017-12-22`
  - `cross_section_n = 2669`
  - `eligible_n = 2685`

First bad value appeared in:
- `cache.db:daily_factor_returns`

Read path:
- `backend.analytics.services.universe_loadings.load_latest_factor_coverage()`
- `backend.analytics.pipeline.run_refresh()`
- persisted `serving_payload_current.exposures`
- `/api/exposures`

Conclusion:
- this was a backend metadata read-path problem, not a frontend rendering problem

### 2) Stale PIT dates in served dashboard payloads

Symptom:
- `/api/risk` and `/api/exposures` reported:
  - `fundamentals_asof = 2026-03-13`
  - `classification_asof = 2026-03-13`
- but the intended PIT policy-correct date was:
  - `2026-02-27`

First bad value appeared in:
- stored rows in `serving_payload_current`

Conclusion:
- the authoritative source-date logic had already been corrected, but the current served payload snapshot still needed to be rebuilt and republished

### 3) Data diagnostics depending on stale cache-era tables

Symptom:
- diagnostics could still surface stale cache-era tables as if they were ordinary current metadata sources

First bad values appeared in:
- `daily_universe_eligibility_summary`
- `daily_factor_returns`

Conclusion:
- this was a diagnostics/source-selection problem
- the endpoint needed durable-first preference, not frontend patching

## Correct Treatment Decisions

- `daily_factor_returns`
  - retired from the active factor-coverage read path
  - kept compatibility-only
- `daily_universe_eligibility_summary`
  - deprioritized behind current durable `eligibility` serving payload
  - kept compatibility-only
- `serving_payload_current`
  - kept as the live dashboard read model
  - rebuilt / republished after the metadata-source corrections
- `model_run_metadata`
  - no new Area 3 cleanup needed
  - test contamination was already fixed in Area 1

## Implemented Fixes

- `backend/analytics/services/universe_loadings.py`
  - `load_latest_factor_coverage(...)` now prefers durable `model_factor_returns_daily`
  - cache `daily_factor_returns` is fallback-only
- `backend/analytics/pipeline.py`
  - light/full refresh now pass `data_db` into factor-coverage loading so the serving payload is built from durable current coverage metadata
- `backend/services/data_diagnostics_service.py`
  - eligibility summary now prefers durable `eligibility` serving payload
  - factor cross-section summary now prefers durable `model_factor_returns_daily`
  - `risk_engine_meta` now has a durable model-output fallback
- `frontend/src/app/data/page.tsx`
  - legacy cache-table descriptions now explicitly say compatibility-only / legacy
- corrective republish:
  - ran `serve-refresh` to rebuild and republish the current serving payload snapshot from the corrected metadata sources

## Result

After the corrective refresh:
- `/api/risk`
  - `fundamentals_asof = 2026-02-27`
  - `classification_asof = 2026-02-27`
  - `coverage_date = 2026-03-13`
- `/api/exposures?mode=raw`
  - first factor metadata now reports:
    - `coverage_date = 2026-03-13`
    - `cross_section_n = 3446`
    - `eligible_n = 3455`
- the current serving snapshot is:
  - `model_run_20260317T050943Z`

## Intentionally Deferred

- `backend/services/factor_history_service.py`
  - still uses cache-era factor-return history for historical charts
- `backend/analytics/health.py`
  - still uses cache-era factor-return history for deep diagnostics

Those are separate read-path decisions and were intentionally not reopened in this narrow Area 3 correction.
