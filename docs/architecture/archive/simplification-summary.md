# Simplification Summary

Date: 2026-03-17
Status: Completed simplification and deletion campaign
Owner: Codex

## What Was Removed

### Deleted files
- `backend/data/cache.py`

### Deleted helpers
- `backend/analytics/health_payloads.py`
  - `can_reuse_cached_health_payload`

### Deleted wrapper-only seams
- `backend/analytics/services/cache_publisher.py`
  - `build_risk_engine_state`
  - `_health_reuse_signature`
  - `_serving_source_dates`
  - `_carry_forward_health_payload`
  - `build_model_sanity_report`
  - `load_latest_eligibility_summary`
- `backend/analytics/pipeline.py`
  - `_load_publishable_payloads`
  - `_restamp_publishable_payloads`
  - duplicate local numeric coercion helpers removed in favor of `refresh_metadata`

## What Was Merged Or Collapsed

### Cache ownership collapsed onto `backend.data.sqlite`

These callers no longer go through the alias shim:
- `backend/main.py`
- `backend/services/refresh_manager.py`
- `backend/services/holdings_runtime_state.py`
- `backend/tests/test_health_neon_sync_signal.py`

### Analytics numeric coercion collapsed onto one owner

Canonical owner:
- `backend/analytics/refresh_metadata.py`

Removed local clones from:
- `backend/analytics/pipeline.py`
- `backend/analytics/services/risk_views.py`
- `backend/analytics/services/universe_loadings.py`

### Publish and health helper ownership made explicit

`cache_publisher.py` now calls the real owners directly:
- `backend.analytics.refresh_metadata`
- `backend.analytics.health_payloads`

### Live-first cache fallback unified in what-if service

`backend/services/portfolio_whatif.py` now uses:
- `backend.data.sqlite.cache_get_live_first`

instead of repeating `cache_get_live(...) or cache_get(...)`.

## What Got Simpler

### Fewer fake ownership seams

The deleted wrappers were not adding policy. They mostly existed as historical seams or local
forwarders. Removing them makes it clearer which modules actually own:

- publish payload loading/restamping
- risk-engine state shaping
- health payload reuse/carry-forward
- cache read policy

### Less duplicated low-level policy

Numeric coercion and cache-selection behavior now have clearer single owners instead of repeated
small clones.

### Cleaner imports and easier reasoning

The repo no longer suggests a separate `backend.data.cache` subsystem when the real owner is
`backend.data.sqlite`.

## What Remains Intentionally Deferred

These areas were reviewed and intentionally left alone:

- `backend/analytics/health.py`
- `backend/services/neon_mirror.py`
- `backend/risk_model/daily_factor_returns.py`
- `backend/risk_model/raw_cross_section_history.py`
- archive scripts under `backend/scripts/_archive/*`

Reason:
- they still own real workflow or domain behavior
- simplifying them now would stop being a safe deletion campaign and turn into correctness or
  redesign work

## Validation

Passed:

- `python3 -m compileall` on changed backend modules and touched tests
- targeted pytest slice:
  - `backend/tests/test_cache_publisher_service.py`
  - `backend/tests/test_health_neon_sync_signal.py`
  - `backend/tests/test_operating_model_contract.py`
  - `backend/tests/test_refresh_profiles.py`
  - `backend/tests/test_operator_status_route.py`
  - `backend/tests/test_holdings_service.py`
  - `backend/tests/test_holdings_route_dirty_state.py`
  - `backend/tests/test_portfolio_whatif_service.py`
- result: `114 passed`
- `git diff --check`

Confirmed:

- no broken imports in the changed areas
- no remaining runtime references to `backend.data.cache`
- no remaining duplicate `cache_get_live(...) or cache_get(...)` pattern in runtime code
- no new source-of-truth ambiguity introduced

## What Future Contributors Should Avoid Reintroducing

- Do not add alias shims like `backend.data.cache` when the real owner already exists.
- Do not add wrapper-only helpers that simply forward to another imported module.
- Do not duplicate low-level policy helpers such as float coercion or cache-selection logic.
- Do not reintroduce manual live-first cache fallbacks when `cache_get_live_first(...)` already
  owns that behavior.
- Do not simplify deferred large modules opportunistically; handle them only in dedicated,
  correctness-focused passes.
