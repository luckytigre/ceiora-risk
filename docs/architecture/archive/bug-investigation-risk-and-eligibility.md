# Bug Investigation: Risk, Eligibility, and Diagnostics

Date: 2026-03-16

## Scope

Investigated and repaired four reported symptoms:

1. Risk page variance attribution / decomposition collapsing into implausible Beta-style dominance.
2. Factor Exposures `Sensitivity` and `Risk Contrib` views zeroing non-Beta factors.
3. Explore / security lookup marking valid equities like `AAPL` as ineligible.
4. Data tab `Data Pipeline Overview` cards showing `-` for most values.

The investigation traced each issue through:

- frontend rendering / selectors
- API payloads
- backend routes and services
- refresh / publish pipeline
- underlying SQLite source tables, runtime cache tables, and durable model outputs

## Findings by Symptom

### 1. Explore / Security Lookup: valid equities marked ineligible

- Symptom:
  - `/api/universe/ticker/AAPL` returned `model_status = "ineligible"`, empty exposures, and `eligibility_reason = "missing_factor_exposures"`.
  - Same reproduced for `AAL` and `AAP`.
- Expected behavior:
  - Valid common equities with current classifications, prices, and factor rows should resolve as structurally eligible / projectable and carry exposures in the latest served snapshot.
- Actual behavior:
  - A small set of valid names were frozen out of ingest repair and then preserved as stale ineligible entries in served `universe_loadings`.
- First bad value:
  - `security_master` already held corrupted rows for exactly seven names, including `AAPL.OQ`.
  - Example before repair:
    - `isin = "263.75"`
    - `exchange_name = "Technology"`
    - `classification_ok = 0`
    - `is_equity_eligible = 0`
  - After repairing those rows locally, `barra_raw_cross_section_history` contained healthy `AAPL` factor rows on `2026-03-13`, but the served snapshot still showed `missing_factor_exposures`.
- Affected modules / files:
  - `backend/scripts/download_data_lseg.py`
  - `backend/analytics/pipeline.py`
  - `backend/orchestration/runtime_support.py`
  - `backend/orchestration/stage_serving.py`
- Root causes:
  1. **Security-master ingest freeze-out**
     - Default LSEG ingest excluded currently flagged-ineligible rows, so corrupted `security_master` entries were never repaired unless explicitly requested.
  2. **Serve-refresh visibility gap**
     - Local `serve-refresh` could still read source history through Neon-backed `core_reads`, so locally repaired raw-history rows were not necessarily visible to the refresh that rebuilt the served snapshot.
  3. **Stale `universe_loadings` reuse**
     - Even with repaired local source rows available, light refresh reused cached `universe_loadings` whenever source/risk signatures matched, preserving old `missing_factor_exposures` decisions.
- Isolated or systemic:
  - Systemic for any ticker whose security-master row is corrupted and whose latest local repairs are hidden by stale refresh reuse.
- Implemented fix:
  - Default ingest no longer filters out rows solely because current security-master flags are false.
  - Local `serve-refresh` now explicitly prefers the local source archive.
  - When local source archive is preferred, light refresh rebuilds `universe_loadings` instead of reusing the stale cached snapshot.

### 2. Risk page Beta / style dominance

- Symptom:
  - Variance attribution showed Beta near 100%.
  - Risk decomposition was dominated by style, with market / industry effectively absent.
- Expected behavior:
  - Portfolio risk should be split plausibly across market, industry, style, and idiosyncratic components.
- Actual behavior:
  - The served risk payload was built from a pathological cached runtime covariance / specific-risk state.
- First bad value:
  - Cached `risk_engine_cov` was effectively one-factor / Beta-only.
  - Cached `risk_engine_meta` showed stale and underspecified state.
  - Durable model outputs already contained full covariance and specific-risk state through `2026-03-13`.
- Affected modules / files:
  - `backend/analytics/pipeline.py`
  - `backend/data/model_output_state.py`
  - `backend/data/model_outputs.py`
- Root cause:
  - `serve-refresh` trusted degraded runtime cache covariance / specific-risk payloads even when durable model outputs were fresher and structurally complete.
- Isolated or systemic:
  - Systemic for light refreshes after cache drift or interrupted prior runs.
- Implemented fix:
  - Added persisted-model fallback readers for covariance and specific risk.
  - Light refresh now falls back to durable model outputs when runtime cache inputs are missing or implausibly incomplete.

### 3. Factor Exposures `Sensitivity` / `Risk Contrib` views zeroing non-Beta factors

- Symptom:
  - Raw exposures showed many factors.
  - `Sensitivity` and `Risk Contrib` zeroed most factors except Beta.
- Expected behavior:
  - Non-Beta style, market, and industry factors should remain non-zero when covariance / volatility inputs are valid.
- Actual behavior:
  - API payloads already carried zeros upstream of the frontend.
- First bad value:
  - `/api/exposures?mode=sensitivity` and `/api/exposures?mode=risk_contribution` were using factor volatilities from the same degraded cached covariance state that broke the risk page.
- Affected modules / files:
  - `backend/analytics/pipeline.py`
  - `backend/analytics/services/risk_views.py`
  - `frontend/src/app/exposures/page.tsx`
- Root cause:
  - Same upstream cause as Symptom 2: stale runtime covariance / specific-risk cache trusted over durable model outputs.
- Isolated or systemic:
  - Shared upstream cause with the risk-page collapse.
- Implemented fix:
  - Same fix as Symptom 2. No frontend contract change was needed once the backend payload was corrected.

### 4. Data tab `Data Pipeline Overview` cards show `-`

- Symptom:
  - Data diagnostics overview cards showed `-` or stale old dates.
- Expected behavior:
  - The cards should show current eligibility and factor cross-section dates/counts when those facts exist either in local cache or in durable serving/model-output surfaces.
- Actual behavior:
  - Diagnostics either came back empty or preferred stale cache-era tables even when newer durable truth existed.
- First bad value:
  - `data_diagnostics_sections.py` looked only at local cache tables for:
    - `daily_universe_eligibility_summary`
    - `daily_factor_returns`
  - In the current runtime, those tables were absent or stale, even though durable `eligibility` payloads and `model_factor_returns_daily` were current through `2026-03-13`.
- Affected modules / files:
  - `backend/services/data_diagnostics_sections.py`
  - `backend/services/data_diagnostics_service.py`
- Root causes:
  1. **Cache-only assumptions**
     - Diagnostics expected local cache tables to be authoritative.
  2. **Stale metadata preference**
     - Even when durable truth existed, the diagnostics service could keep older cache-era section summaries instead of preferring newer durable values.
- Isolated or systemic:
  - Isolated to diagnostics, but it reflects the broader class of stale-cache-overrides-current-truth bugs.
- Implemented fix:
  - Added durable fallback sources:
    - eligibility summary from served `eligibility`
    - factor cross-section from durable `model_factor_returns_daily`
  - Diagnostics now prefer the newer of cache-derived and durable-derived section dates.

## Shared Upstream Causes

There was **shared drift**, but not a single root cause.

The shared upstream pattern was:

1. **Stale or degraded cache/runtime surfaces were trusted too early**
   - Broke risk, factor sensitivities, and diagnostics freshness.

2. **Local repairs were not guaranteed to be visible to local serve-refresh**
   - Allowed repaired source history to remain invisible to the snapshot rebuild that should have fixed eligibility and exposures.

3. **One separate source-ingest issue froze out affected equities**
   - Corrupted security-master rows plus default ingest filtering kept valid names like `AAPL` from self-repairing.

## Implemented Fixes

1. **Ingest scope repair**
   - `backend/scripts/download_data_lseg.py`
   - Default ingest no longer excludes names purely because current security-master flags are false.

2. **Durable covariance / specific-risk fallback**
   - `backend/data/model_output_state.py`
   - `backend/data/model_outputs.py`
   - `backend/analytics/pipeline.py`
   - Light refresh now uses persisted model outputs when runtime cache risk state is degraded.

3. **Local serve-refresh visibility fix**
   - `backend/orchestration/runtime_support.py`
   - `backend/orchestration/stage_serving.py`
   - `backend/analytics/pipeline.py`
   - Local `serve-refresh` now prefers the local source archive and rebuilds `universe_loadings` instead of reusing stale cache when local source truth is explicitly preferred.

4. **Stale eligibility metadata overlay fix**
   - `backend/analytics/refresh_metadata.py`
   - `backend/analytics/services/cache_publisher.py`
   - Publish-time eligibility summaries are refreshed from current snapshot truth instead of preserving stale cache-era dates/counts.

5. **Diagnostics freshness fix**
   - `backend/services/data_diagnostics_sections.py`
   - `backend/services/data_diagnostics_service.py`
   - Diagnostics now fall back to durable truth and prefer newer section dates over stale cache summaries.

## Validation

### Targeted tests

- `backend/tests/test_security_master_lineage.py`
- `backend/tests/test_model_outputs_local_regression.py`
- `backend/tests/test_cache_publisher_service.py`
- `backend/tests/test_data_diagnostics_route.py`
- `backend/tests/test_refresh_profiles.py`
- `backend/tests/test_operating_model_contract.py`

Latest focused run:

- `84 passed in 1.19s`

### Live validation

After the patched `serve-refresh`, the live snapshot is `model_run_20260317T031226Z`.

- `/api/universe/ticker/AAPL`
  - `model_status = "core_estimated"`
  - `eligibility_reason = ""`
  - `exposure_count = 16`
- `/api/universe/ticker/AAL`
  - `model_status = "core_estimated"`
  - `eligibility_reason = ""`
  - `exposure_count = 16`
- `/api/universe/ticker/AAP`
  - `model_status = "core_estimated"`
  - `eligibility_reason = ""`
  - `exposure_count = 16`

- `/api/risk`
  - `risk_shares = {idio: 60.64, style: 13.39, market: 3.89, industry: 22.08}`
  - `factor_detail_count = 45`

- `/api/exposures?mode=raw`
  - `factor_count = 19`
  - `nonzero = 19`
- `/api/exposures?mode=sensitivity`
  - `factor_count = 19`
  - `nonzero = 19`
- `/api/exposures?mode=risk_contribution`
  - `factor_count = 19`
  - `nonzero = 19`

- `/api/data/diagnostics`
  - `eligibility_summary.latest.date = 2026-03-13`
  - `factor_cross_section.latest.date = 2026-03-13`

## Remaining Uncertainties

Two non-blocking follow-ups remain outside the scope of the original bug report:

1. `/api/health` may still display stale `neon_sync_health` state even when `/api/operator/status` reflects the current runtime state correctly.
   - This appears to be a separate health-surface consistency issue.

2. Some exposure payload metadata still reports old `coverage_date` values (`2017-12-22`) even though the actual exposure, sensitivity, and risk-contribution values are correct and current.
   - That metadata drift did not drive the reported functional bugs, so it was not changed in this pass.
