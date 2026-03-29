# Dependency Rules

Date: 2026-03-16
Status: Active dependency rules
Owner: Codex

## Purpose

These rules keep the restructuring grounded and prevent new cross-layer leakage while the codebase is being cleaned up incrementally.

## Allowed Dependency Direction

### Backend

Allowed:

- `api` -> `services`
- `api` -> small presenter helpers
- `services` -> `analytics`
- `services` -> `risk_model`
- `services` -> `universe`
- `services` -> `portfolio`
- `services` -> `data`
- `orchestration` -> `analytics`
- `orchestration` -> `risk_model`
- `orchestration` -> `universe`
- `orchestration` -> `data`
- `orchestration` -> narrow service surfaces when operationally justified
- `analytics` -> `risk_model`
- `analytics` -> `portfolio`
- `analytics` -> `universe`
- `analytics` -> `data`
- `risk_model` -> `data` only where storage-backed inputs are still unavoidable during migration
- `portfolio` -> `data`
- `universe` -> `data`

Avoid:

- `data` -> `api`
- `data` -> `frontend`
- `risk_model` -> `api`
- `analytics` -> `api`
- `services` -> `api`
- `services` -> full orchestration jobs just to inspect static metadata

### cPAR-Specific Rule

`cPAR` is parallel to cUSE4, but it still follows the same layered ownership rules.

Current cPAR placement:
- pure cPAR math/domain logic lives in `backend/cpar/*`
- cPAR integration code lives in `backend/data/*`, `backend/orchestration/*`, `backend/services/*`, and `backend/api/routes/*`

Avoid:
- `backend/cpar/*` importing any integration layer
- cPAR routes importing `backend.data` or `backend.cpar`
- cPAR services importing API or orchestration layers
- cPAR integration reusing `serving_payload_current` or runtime-state surfaces unless a later documented exception is approved
- routing aggregate cPAR snapshot assembly back through `cpar_portfolio_snapshot_service.build_cpar_risk_snapshot()` once `cpar_aggregate_risk_service.py` exists as the explicit aggregate owner

Current cPAR owner exception:
- `backend/services/cpar_risk_service.py` may stay as a thin route-facing shim over `backend/services/cpar_aggregate_risk_service.py`
- other cPAR services may call `backend/services/cpar_aggregate_risk_service.py` directly when they need the same package-pinned aggregate snapshot semantics
- `backend/services/cpar_portfolio_hedge_service.py` may stay as the route-facing hedge orchestration owner over the shared account-context/support loaders in `backend/services/cpar_portfolio_snapshot_service.py`
- `backend/services/cpar_portfolio_account_snapshot_service.py` owns the shared account-scoped hedge snapshot builder reused by the hedge and what-if services
- `backend/services/cpar_portfolio_snapshot_service.py` remains the shared support/core layer below those owners, and `build_cpar_portfolio_hedge_snapshot()` is compatibility only while callers migrate
- `backend/services/cpar_portfolio_snapshot_service.py` should not silently become the primary aggregate owner again

Current universe runtime owner exception:
- `backend/universe/runtime_authority.py` owns current-table authority loading for registry/policy/taxonomy/source-observation rows
- `backend/universe/runtime_rows.py` remains the mixed-state runtime owner over compat/legacy fallback, historical classification reads, structural/policy resolution, candidate-RIC selection, and the public runtime-row loaders

Current source-read owner exception:
- `backend/data/source_read_authority.py` owns the lower registry-first source authority helpers
- `backend/data/source_reads.py` remains the public source-read facade and keeps SQLite cache/compat logic plus raw cross-section exposure helpers

Current serving-output owner exception:
- `backend/data/serving_output_read_authority.py` owns the lower Neon/SQLite serving-payload read helpers
- `backend/data/serving_outputs.py` remains the public serving-payload facade and keeps route-facing read semantics plus write/verify ownership

## Entrypoint Rules

Routes, CLI wrappers, and local scripts must stay thin.

They may:
- validate inputs
- authenticate
- call a service or workflow entrypoint
- translate exceptions

They should not:
- implement reusable business logic
- assemble truth from multiple stores inline
- own policy branching that belongs in services or workflows

## Service Rules

Service modules should:
- expose one coherent application surface
- call lower layers explicitly
- centralize one family of payload semantics

Service modules should not:
- become generic repositories
- become unbounded dumping grounds
- reimplement storage adapters inline

## Workflow Rules

Workflow/orchestration modules should:
- define stages and job sequencing
- coordinate long-running tasks
- publish run status and artifacts
- pass runtime db targets and similar execution context explicitly when stage execution is redirected to a workspace or non-canonical store

Workflow/orchestration modules should not:
- own every low-level helper used inside a stage
- become the only place where profile metadata can be queried
- mutate process-wide module globals just to retarget data or cache paths for one run

## Data Adapter Rules

Data modules should:
- encapsulate persistence and provider-specific behavior
- expose stable surfaces by data product

Data modules should not:
- own UI semantics
- own route-specific response normalization
- silently choose authorities without explicit configuration or documented fallback

## Frontend Rules

Frontend pages should:
- read a small number of backend surfaces
- use shared truth helpers for user-facing freshness semantics
- keep intentional shared surfaces explicit when they compose shared holdings owners plus read-only family overlays

Frontend pages should not:
- reconstruct source-of-truth semantics independently
- duplicate page banner logic across pages
- hide mixed-family ownership behind compatibility barrels when a page is intentionally shared

## Shared Code Rules

Shared code is allowed only if all of these are true:
- it is used by more than one real consumer
- it has a narrow responsibility
- its owner is obvious
- its name is concrete

Prohibited patterns:
- `common.py`
- `shared.py`
- `manager.py` without lifecycle responsibility
- catch-all `utils` modules that are actually mini-frameworks

Current reviewed exception:
- `backend/services/refresh_control_service.py` is allowed because it owns the application-facing refresh control flow for routes and control clients
- `backend/services/refresh_manager.py` is allowed because it owns concrete process-local refresh lifecycle control for local compatibility
- `backend/ops/cloud_run_jobs.py` is allowed because provider-specific Cloud Run Jobs dispatch belongs in an operational adapter, not in routes

Additional cloud-runtime rule:
- serve-facing services may read persisted refresh status, but they may not import refresh-manager execution helpers that assume local worker ownership

## Cleanup Slice Rules

While the repo remains in an active cleanup phase:

- structural cleanup must stay path-scoped and rollback-safe
- do not mix live operational work, unrelated migrations, or opportunistic tidy-ups into a cleanup slice
- if two authority seams have different rollback behavior or different validation gates, they belong in different slices
- update canonical docs in the same change when ownership, runtime authority, route semantics, or operator expectations change
- repo-hygiene ignore rules must be root-anchored and concrete; do not add broad patterns that can hide legitimate repository content

## Documentation Rules

Any structural refactor that changes ownership or semantics must update:
- `maintainer-guide.md` or `architecture-invariants.md` when active rules or maintainer guidance changed
- canonical docs outside `docs/architecture/` when operator behavior or frontend truth wording changed
- archived snapshots such as `archive/current-state.md`, `archive/target-architecture.md`, or `archive/module-inventory.md` only when preserving historical context is worthwhile
