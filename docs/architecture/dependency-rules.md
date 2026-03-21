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

Frontend pages should not:
- reconstruct source-of-truth semantics independently
- duplicate page banner logic across pages

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
- `backend/services/refresh_manager.py` is allowed because it owns concrete process-local refresh lifecycle control inside the control-plane surface

Additional cloud-runtime rule:
- serve-facing services may read persisted refresh status, but they may not import refresh-manager execution helpers that assume local worker ownership

## Documentation Rules

Any structural refactor that changes ownership or semantics must update:
- `maintainer-guide.md` or `architecture-invariants.md` when active rules or maintainer guidance changed
- canonical docs outside `docs/architecture/` when operator behavior or frontend truth wording changed
- archived snapshots such as `archive/current-state.md`, `archive/target-architecture.md`, or `archive/module-inventory.md` only when preserving historical context is worthwhile
