# Performance Summary

Date: 2026-03-17
Status: Completed targeted performance and efficiency pass
Owner: Codex

## Top Improvements Made

### 1. Batched durable serving-payload reads

Added multi-payload helpers in [serving_outputs.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/data/serving_outputs.py):

- `load_current_payloads(...)`
- `load_runtime_payloads(...)`

This reduces repeated connection setup and repeated one-row queries when a single request needs
multiple payloads.

### 2. Risk route stops paying two separate serving-output reads

[dashboard_payload_service.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/services/dashboard_payload_service.py)
now batch-loads `risk` and `model_sanity` in the default path, while preserving custom loader test
seams.

### 3. What-if preview reduces serving-output round-trips

[portfolio_whatif.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/services/portfolio_whatif.py)
now batch-loads current durable serving payloads for:

- `portfolio`
- `universe_loadings`
- `risk_engine_cov`
- `risk_engine_specific_risk`

in the default path, instead of opening separate durable reads for each payload.

### 4. Factor-history lookup avoids unnecessary large payload reads

[factor_history_service.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/services/factor_history_service.py)
now prefers `universe_factors` first and stops after the first available factor catalog instead of
walking redundant payload catalogs by default.

### 5. Diagnostics table stats use fewer SQL round-trips

[data_diagnostics_sqlite.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/services/data_diagnostics_sqlite.py)
now:

- uses one `MIN/MAX` query instead of separate ascending/descending date queries
- avoids redundant existence/schema checks in the source-table inventory path

## Where Efficiency Improved

### Request paths

- `GET /api/risk`
  - fewer serving-output round-trips
- `POST /api/portfolio/whatif`
  - fewer durable payload reads before analytics recomputation
- `GET /api/exposures/history`
  - less redundant payload loading for factor catalog lookup
- `GET /api/data/diagnostics`
  - fewer table-inspection queries

### Data access behavior

Concrete reductions:

- what-if preview no longer performs four separate default durable payload reads in the happy path
- risk response no longer performs two separate default runtime payload reads
- diagnostics table stats no longer perform two date-bound queries per table

## Validation

Passed:

- `python3 -m compileall` on changed backend modules/tests
- targeted pytest slice:
  - `backend/tests/test_serving_outputs.py`
  - `backend/tests/test_dashboard_payload_service.py`
  - `backend/tests/test_portfolio_whatif_service.py`
  - `backend/tests/test_exposure_history_route.py`
  - `backend/tests/test_data_diagnostics_route.py`
  - `backend/tests/test_serving_output_route_preference.py`
  - `backend/tests/test_serving_output_route_fallbacks.py`
  - `backend/tests/test_api_golden_snapshots.py`
  - `backend/tests/test_refresh_profiles.py`
  - `backend/tests/test_operating_model_contract.py`
  - `backend/tests/test_operator_status_route.py`
- result: `139 passed`

Also passed:

- `npm run typecheck`
- `git diff --check`

Confirmed:

- no broken imports in changed areas
- no source-of-truth changes
- no new speculative cache layer introduced
- no serving/core timeline rules changed

## Remaining Hotspots

These remain the most likely future profiling targets:

- [universe_loadings.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/analytics/services/universe_loadings.py)
- [pipeline.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/analytics/pipeline.py)
- [daily_factor_returns.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/risk_model/daily_factor_returns.py)
- [raw_cross_section_history.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/risk_model/raw_cross_section_history.py)
- [health.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/analytics/health.py)
- fallback factor-resolution in [history_queries.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/data/history_queries.py)

## What Was Intentionally Deferred

- deep optimization of the full-universe loadings builder
- core model math / factor-return estimation changes
- deep health diagnostics internals
- Neon mirror/parity flows

These areas need real profiling or correctness review, not opportunistic efficiency edits.

## Recommendations For Future Profiling

1. Measure wall-clock time and query counts for:
   - `serve-refresh`
   - `portfolio/whatif`
   - `data/diagnostics`

2. Profile dataframe-heavy time inside:
   - `build_universe_ticker_loadings(...)`
   - `compute_daily_factor_returns(...)`
   - raw cross-section build paths

3. If historical chart traffic becomes material, profile:
   - `resolve_factor_history_factor(...)`
   - `load_factor_return_history(...)`

4. Keep future optimizations explicit:
   - reduce repeated work first
   - avoid new hidden caches
   - do not blur authoritative durable reads with compatibility fallbacks
