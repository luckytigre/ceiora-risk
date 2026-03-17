# Simplification Candidates

Date: 2026-03-17
Status: Ranked candidate list
Owner: Codex

## Safe Deletions

### 1. Delete `backend/data/cache.py`
- Why: it is a pure alias shim over `backend.data.sqlite` with no policy, no branching, and only a handful of imports.
- Impact if removed: slightly fewer indirection layers; imports become explicit about the real owner.
- Confidence: High
- Validation needed:
  - import checks for `backend/main.py`, `backend/services/refresh_manager.py`, `backend/services/holdings_runtime_state.py`
  - targeted tests on health and refresh status surfaces

### 2. Delete pure forwarding wrappers in `backend/analytics/services/cache_publisher.py`
- Candidates:
  - `build_risk_engine_state`
  - `_health_reuse_signature`
  - `_serving_source_dates`
  - `_carry_forward_health_payload`
  - `build_model_sanity_report`
  - `load_latest_eligibility_summary`
- Why: each wrapper only forwards into `refresh_metadata` or `health_payloads`, both already imported directly.
- Impact if removed: less indirection, fewer fake ownership seams, easier test patching.
- Confidence: High
- Validation needed:
  - `test_cache_publisher_service.py`
  - `test_operating_model_contract.py`

## Likely Deletions Needing Validation

### 3. Delete forwarding wrappers in `backend/analytics/pipeline.py`
- Candidates:
  - `_load_publishable_payloads`
  - `_restamp_publishable_payloads`
- Why: they only proxy `publish_payloads.*`; the only substantial reason they still exist is old test patching.
- Impact if removed: simpler `publish-only` path and cleaner tests.
- Confidence: High-Medium
- Validation needed:
  - `test_operating_model_contract.py`
  - `test_refresh_profiles.py`
  - publish-only smoke path

### 4. Collapse duplicated numeric coercion helpers onto `refresh_metadata`
- Candidates:
  - `_finite_float` in `pipeline.py`
  - `_finite_float` in `risk_views.py`
  - `_finite_float` in `universe_loadings.py`
- Why: repeated local clones with identical behavior add maintenance cost and drift risk.
- Impact if removed: one canonical coercion behavior in analytics.
- Confidence: High-Medium
- Validation needed:
  - analytics contract tests
  - risk / exposures serving tests

## Merge / Collapse Candidates

### 5. Collapse `backend/data/cache.py` callers onto `backend.data.sqlite`
- Why: same as candidate 1, but listed separately because the change is mostly import churn across dependent modules/tests.
- Impact if removed: clearer data ownership.
- Confidence: High
- Validation needed:
  - import smoke
  - `test_runtime_state.py`
  - `test_operator_status_route.py`

### 6. Collapse direct fallback pairs onto `cache_get_live_first` where semantics are identical
- Candidate examples:
  - some `cache_get_live(...) or cache_get(...)` patterns in services
- Why: a few paths manually reimplement the live-first fallback already owned by `sqlite.cache_get_live_first`.
- Impact if removed: less repeated cache-selection logic.
- Confidence: Medium
- Validation needed:
  - check each site individually; some may intentionally distinguish direct live vs active-snapshot reads

## Candidates To Leave Alone For Now

### `backend/analytics/health.py`
- Why not now: large deferred diagnostics surface with known legacy dependencies; simplification here risks mixing into deferred correctness work.
- Confidence to defer: High

### `backend/services/neon_mirror.py`
- Why not now: large operational module with real stateful behavior; simplification needs a dedicated publish/parity review.
- Confidence to defer: High

### `backend/risk_model/daily_factor_returns.py`
- Why not now: core math and cadence-sensitive logic; deletion/merge risk is high.
- Confidence to defer: High

### `backend/scripts/_archive/*`
- Why not now: probably deletable eventually, but docs still reference them as archived historical context.
- Confidence to defer: Medium
- Validation needed before deletion:
  - doc reference cleanup
  - confirm no operator workflow still points at them

## Ranked Execution Order

1. Delete `backend/data/cache.py` and update imports.
2. Remove wrapper-only helpers from `backend/analytics/services/cache_publisher.py`.
3. Remove wrapper-only helpers from `backend/analytics/pipeline.py` and update tests.
4. Collapse duplicate numeric coercion helpers in analytics.
5. Rescan for any newly exposed dead imports or low-value seams after those deletions.
