# Audit: Architectural Integrity

Date: 2026-03-16
Reviewer: Independent architecture audit
Status: Boundary and integrity review

Update note:

- The `temporary_runtime_paths()` finding documented here was remediated in follow-up Batch 2 later on 2026-03-16.
- The remaining findings in this audit file should still be treated as active unless separately noted.

## Summary

The architecture is cleaner than before, but it is not yet strictly layered. The most important remaining integrity problems are:

- routes still importing data-layer modules directly
- service-to-orchestrator coupling in places where it is not justified
- hidden cross-module state mutation during rebuild workflows
- a few very large modules that still own too many responsibilities

## Boundary Violations

### 1. API routes still cross directly into the data layer

This is the clearest dependency-rule violation.

Examples:

- `backend/api/routes/exposures.py`
- `backend/api/routes/risk.py`
- `backend/api/routes/portfolio.py`
- `backend/api/routes/health.py`
- `backend/api/routes/universe.py`
- `backend/api/routes/readiness.py`

These files import things like:

- `backend.data.serving_outputs`
- `backend.data.sqlite`
- `backend.data.history_queries`

That directly conflicts with `docs/architecture/dependency-rules.md`, which frames routes as thin entrypoints delegating downward through services/presenters rather than reaching into storage.

### 2. `operator_status_service.py` depends on the full orchestrator module

`backend/services/operator_status_service.py` imports:

- `backend.orchestration.run_model_pipeline.DATA_DB`

This is poor boundary direction for a service that is assembling operator truth. It creates hidden coupling to the main job engine just to get a path constant.

This is not a theoretical purity concern. It makes the operator service structurally dependent on orchestration initialization even though its real dependency is just job-run storage.

### 3. `refresh_manager.py` depends on both orchestration metadata and the full orchestrator

`backend/services/refresh_manager.py` imports:

- `backend.orchestration.profiles`
- `backend.orchestration.run_model_pipeline.run_model_pipeline`

Calling the workflow entrypoint is legitimate. Importing profile metadata is also fine.

What is not clean is that the same service is both:

- local API execution manager
- background worker owner
- refresh-state cache owner
- workflow launcher

This is acceptable operationally for a hobby tool, but it means the service layer is not purely “application services”; it also owns runtime process control.

## Hidden Coupling

### 1. `temporary_runtime_paths()` mutates global state across modules

`backend/orchestration/runtime_support.py` mutates:

- `config.DATA_DB_PATH`
- `config.SQLITE_PATH`
- `backend.analytics.pipeline.DATA_DB`
- `backend.analytics.pipeline.CACHE_DB`
- `backend.data.core_reads.DATA_DB`

That is a hidden cross-module side effect, even though it is wrapped in a context manager.

This is one of the highest-risk pieces of remaining architecture because it creates action-at-a-distance in the middle of a rebuild workflow.

### 2. Operator truth is centralized but still assembled from too many stores

`backend/services/operator_status_service.py` assembles runtime truth from:

- `job_runs`
- `refresh_manager`
- `runtime_state`
- `sqlite` cache fallback
- `core_reads`
- holdings runtime state

Centralization is better than scattering, but the coupling is still high. This service is now the single place where store reconciliation happens, which is useful, but it is also a sign that the underlying runtime truth model is still not cleanly unified.

### 3. `portfolio_whatif.py` still mixes serving truth with fallback cache semantics

`backend/services/portfolio_whatif.py` directly decides between:

- durable serving payloads
- live cache
- cache fallback policy

That makes the service more than a domain/use-case layer. It is partially an adapter-selection layer too.

## Unclear Ownership

### 1. `backend/services/` is still not one coherent layer

This package currently mixes:

- application services:
  - `dashboard_payload_service.py`
  - `operator_status_service.py`
  - `data_diagnostics_service.py`
  - `holdings_service.py`
- workflow/runtime control:
  - `refresh_manager.py`
- infrastructure:
  - `neon_mirror.py`
  - `neon_authority.py`
  - `neon_stage2.py`
  - `neon_holdings.py`

That is workable, but it means `services/` is still an ownership bucket, not a strictly coherent layer.

### 2. `neon_holdings.py` is not clearly “service” or “adapter”

`backend/services/neon_holdings.py` owns:

- schema application
- identifier resolution
- CSV parsing
- validation
- mutation DML

This is too much for one file and too mixed for one module role.

## Duplicated Logic That Survived

### 1. Risk-recompute policy wrappers are still duplicated

There are multiple wrapper surfaces around the same recompute logic:

- `backend/analytics/pipeline.py`
- `backend/orchestration/runtime_support.py`
- `backend/services/operator_status_service.py`

This is not catastrophic, but it is exactly the kind of low-grade duplication that causes drift later.

### 2. Serialization and payload-shaping helpers still exist in multiple layers

Covariance/payload shaping logic appears in more than one place:

- `backend/analytics/pipeline.py`
- `backend/orchestration/runtime_support.py`
- serving/model-output payload code

Again, not broken, but not fully normalized either.

## Over-Abstraction / Under-Abstraction

### Slight over-abstraction

`backend/data/core_reads.py` and `backend/data/model_outputs.py` are now facades over smaller modules, but they still contain many wrapper methods. That leaves the code in a middle state:

- not monolithic anymore
- not a truly lean facade either

### Under-abstraction where it still matters

The largest remaining modules are still too broad:

- `backend/services/neon_mirror.py`
- `backend/analytics/health.py`
- `backend/services/neon_holdings.py`
- `backend/risk_model/daily_factor_returns.py`
- `backend/data/cross_section_snapshot.py`

These are the places where structural risk still concentrates.

## God Modules That Still Exist

The biggest remaining “god modules” are:

1. `backend/services/neon_mirror.py`
2. `backend/analytics/health.py`
3. `backend/services/neon_holdings.py`
4. `backend/risk_model/daily_factor_returns.py`
5. `backend/data/cross_section_snapshot.py`
6. `backend/services/data_diagnostics_service.py`

The orchestrator itself is much improved and should no longer be the first item on this list.

## Bottom Line

The architecture is not fake-clean. The route/service/orchestration/data split is visibly improved.

But the integrity rules are not yet consistently enforced. The remaining issues are concentrated, identifiable, and mostly in the same places:

- route-to-data leakage
- service/orchestrator coupling
- hidden global state mutation
- a handful of very large operational modules
