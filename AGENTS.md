# AGENTS.md

## Architectural Intent

This repository is a layered application, not a generic framework.

The intended structure is:
- `backend/api`: thin transport entrypoints
- `backend/services`: application-facing payload and mutation surfaces
- `backend/orchestration`: refresh/rebuild workflows
- `backend/analytics`, `backend/risk_model`, `backend/universe`, `backend/portfolio`: reusable domain and compute logic
- `backend/data`: persistence and provider-specific adapters

Prefer boring, explicit ownership over clever abstractions.

## Boundary Rules

1. Routes stay thin.
   They may validate, authenticate, delegate, and translate errors.
   They should not assemble cross-store truth inline.

2. Services own application-facing assembly.
   If multiple lower-layer reads are needed for one UI/API surface, one service module should own that composition.

3. Workflows coordinate long-running work.
   `backend/orchestration` should sequence stages and pass execution context explicitly.
   Do not reintroduce hidden module-global path mutation.

4. Data modules own persistence.
   `backend/data` must not import API or frontend semantics.

## Dependency Direction

Allowed:
- `api` -> `services`
- `services` -> `analytics` / `risk_model` / `universe` / `portfolio` / `data`
- `orchestration` -> lower layers and narrow operational service surfaces

Avoid:
- `routes` importing `backend.data`
- `services` importing API layers
- `services` importing full workflow modules only to inspect static metadata

## Placement Guidance

Put new code:
- in `services` when it defines one application-facing payload or mutation flow
- in `orchestration` when it affects staged jobs or rebuild workflows
- in `data` when it is a stable persistence or provider adapter
- in domain packages when it is reusable compute logic

Do not add:
- `shared.py`
- `common.py`
- vague `*manager.py` files unless the module truly owns lifecycle control

## Anti-Patterns To Avoid

- route-local SQL or cache wiring
- hidden workflow side effects through mutated globals
- reaccumulating branch-heavy logic in `stage_runner.py` or `run_model_pipeline.py`
- turning façade modules back into god files after they were split
- creating a new generic abstraction layer to avoid choosing an owner

## Change Strategy

- prefer incremental changes over structural drift
- preserve stable facades when they already exist
- add the smallest helper/module that creates a clearer boundary
- update `docs/architecture/` when ownership or semantics materially change
- if you introduce a justified exception to these rules, document it in `docs/architecture/dependency-rules.md` and `docs/architecture/architecture-invariants.md`

## The repository should not accumulate temporary artifacts.

Rules:
- scratch outputs must go to a temporary location and be removed
- investigation artifacts should not remain in active directories
- only durable, reusable assets belong in the main repo surface