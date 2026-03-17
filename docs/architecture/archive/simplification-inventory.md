# Simplification Inventory

Date: 2026-03-17
Status: Active inventory for simplification campaign
Owner: Codex

## Scope

This inventory focuses on repo-owned runtime code and active docs. It excludes:
- virtualenv/vendor content
- runtime artifacts
- generated frontend build output

The goal is simplification without changing the intended architecture or source-of-truth rules.

## Major Subsystems

### Backend entrypoints
- `backend/api/routes/*`
- `backend/main.py`
- thin HTTP/auth transport layer

### Application services
- `backend/services/*`
- API-facing payload assembly, holdings mutations, operator/status and diagnostics surfaces

### Orchestration
- `backend/orchestration/*`
- profile planning, stage dispatch, run finalization, runtime policy helpers

### Analytics / serving
- `backend/analytics/*`
- refresh pipeline, serving payload staging, reuse policy, refresh metadata, diagnostics helpers

### Risk model / domain compute
- `backend/risk_model/*`
- raw cross-section build, eligibility, daily factor returns, regression frame construction, risk attribution

### Data / persistence
- `backend/data/*`
- SQLite/Neon persistence, serving outputs, model outputs, source reads, runtime state

### Universe / source ingest
- `backend/universe/*`
- security master, ESTU, schema maintenance
- `backend/scripts/*`
- LSEG ingest/backfill and operational tooling

### Frontend
- `frontend/src/app/*`
- `frontend/src/features/*`
- `frontend/src/lib/*`

## Largest / Broadest Runtime Modules

These are the biggest active repo-owned runtime modules and remain complexity hotspots:

- `backend/analytics/health.py` (`1304` lines)
- `backend/services/neon_mirror.py` (`1290`)
- `backend/risk_model/daily_factor_returns.py` (`1058`)
- `backend/risk_model/raw_cross_section_history.py` (`852`)
- `backend/services/neon_holdings.py` (`837`)
- `backend/analytics/pipeline.py` (`829`)
- `backend/services/neon_stage2.py` (`788`)
- `backend/scripts/download_data_lseg.py` (`756`)
- `backend/data/cross_section_snapshot_build.py` (`670`)

These are not automatic deletion candidates. They remain broad because they still own meaningful workflow or domain logic.

## Repeated Utility / Transformation Patterns

### Numeric coercion helpers

Near-identical float coercion helpers exist in:
- `backend/analytics/refresh_metadata.py`
- `backend/analytics/pipeline.py`
- `backend/analytics/services/risk_views.py`
- `backend/analytics/services/universe_loadings.py`

This is low-level duplication rather than separate policy.

### Risk-engine metadata aliasing

The same legacy/core aliases still appear in multiple places:
- `factor_returns_latest_date`
- `last_recompute_date`
- `core_state_through_date`
- `core_rebuild_date`
- `estimation_exposure_anchor_date`

Some duplication is intentional for compatibility, but several helper wrappers only forward to one canonical implementation.

### Publish / restamp wrappers

`backend/analytics/pipeline.py` still carries private wrapper seams that only forward to other modules:
- `_load_publishable_payloads`
- `_restamp_publishable_payloads`
- `_risk_engine_reuse_signature`
- `_latest_factor_return_date`

Not all of these are good deletion candidates, but some are pure indirection now.

### Cache staging wrappers

`backend/analytics/services/cache_publisher.py` includes several wrappers that only forward into `refresh_metadata` or `health_payloads`:
- `build_risk_engine_state`
- `_health_reuse_signature`
- `_serving_source_dates`
- `_carry_forward_health_payload`
- `build_model_sanity_report`
- `load_latest_eligibility_summary`

These add little ownership value because the file already imports the target modules directly.

## Stale Compatibility / Legacy Surfaces Still Present

### Clearly intentional compatibility

These are still active on purpose and should not be deleted casually:
- `run_model_pipeline._run_stage` seam
- compatibility aliases in risk-engine payloads:
  - `factor_returns_latest_date`
  - `last_recompute_date`
- frozen legacy cache tables documented as compatibility-only:
  - `daily_factor_returns`
  - `daily_universe_eligibility_summary`

### Compatibility logic that looks stale or overly broad

- `backend/data/cache.py`: pure re-export alias over `backend/data/sqlite.py`
- `backend/analytics/pipeline.py`: several private wrappers that only pass through to helper modules
- `backend/analytics/services/cache_publisher.py`: similar local wrappers over already-imported helpers
- tests still patch some private wrapper seams instead of patching the real owners

## Apparently Unused / Low-Value Wrappers

High-confidence examples:
- `backend/data/cache.py`
- `backend/analytics/pipeline.py`:
  - `_load_publishable_payloads`
  - `_restamp_publishable_payloads`
- `backend/analytics/services/cache_publisher.py`:
  - `build_risk_engine_state`
  - `_health_reuse_signature`
  - `_serving_source_dates`
  - `_carry_forward_health_payload`
  - `build_model_sanity_report`
  - `load_latest_eligibility_summary`

These are not independent policy owners; they mostly preserve local call sites or old test seams.

## Archive / Historical Artifacts

Tracked archive scripts still present:
- `backend/scripts/_archive/audit_cuse4_schema_and_coverage.py`
- `backend/scripts/_archive/backfill_trbc_history_lseg.py`
- `backend/scripts/_archive/lseg_ric_resolver.py`
- `backend/scripts/_archive/migrate_to_canonical_timeseries.py`

They are documented as archived and not part of active runtime paths. They are deletion candidates only if docs referencing them are updated and no operational value remains.

## Modules Whose Names No Longer Fit Perfectly

### Mild naming drift
- `backend/analytics/health_payloads.py`
  - actually a reuse/carry-forward helper module, not a broad payload builder
- `backend/data/cache.py`
  - not a cache subsystem, only an alias shim

### Names to leave alone for now
- `refresh_manager.py`
  - allowed reviewed exception in architecture docs
- `stage_runner.py`
  - still correctly names orchestration dispatch ownership after stage-family split

## Strong First-Pass Simplification Opportunities

1. Delete the `backend/data/cache.py` alias module and import `backend.data.sqlite` directly where needed.
2. Remove pure forwarding wrappers in `backend/analytics/services/cache_publisher.py`.
3. Remove pure forwarding wrappers in `backend/analytics/pipeline.py` where tests can patch the real helper modules instead.
4. Collapse duplicate numeric coercion helpers in analytics modules onto one canonical implementation.

## Areas To Leave Alone In This Campaign

These remain large, but they are not safe first-pass deletion/merge targets:
- `backend/analytics/health.py`
- `backend/services/neon_mirror.py`
- `backend/risk_model/daily_factor_returns.py`
- `backend/risk_model/raw_cross_section_history.py`
- `backend/services/neon_stage2.py`
- `backend/scripts/download_data_lseg.py`

They still own real workflow/domain behavior and need dedicated correctness-focused passes, not opportunistic simplification.
