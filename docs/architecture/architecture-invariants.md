# Architecture Invariants

Date: 2026-03-16
Status: Active invariants
Owner: Codex

These are the non-negotiable structural rules for this repository.

## Invariants

1. Routes stay thin.
   Routes may validate, authorize, and delegate. They should not assemble cross-store truth inline.

2. Services own application-facing payloads.
   If a route or UI surface depends on multiple lower-layer reads, one service module should own that assembly.

3. Workflows pass execution context explicitly.
   Rebuild and refresh flows must pass workspace or canonical db targets explicitly rather than mutating module globals.

4. Orchestration coordinates; it does not absorb every helper.
   Stage-family logic belongs in orchestration-local stage modules, not in one branch-heavy integration file.

5. Data facades stay facades.
   Files like `core_reads.py`, `model_outputs.py`, and `cross_section_snapshot.py` should not reaccrete raw helper logic that was intentionally moved behind them.

6. New junk-drawer modules are forbidden.
   Do not add `shared.py`, `common.py`, or vague `*manager.py` modules unless there is an explicit reviewed lifecycle responsibility.

7. Dependency direction is one-way.
   `data` must not import `api` or frontend code.
   `services` must not import API layers.
   `services` should not import full workflow modules just to inspect static metadata.

## Existing Guardrails

The repository already enforces several of these with lightweight tests in [test_architecture_boundaries.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/barra-dashboard/backend/tests/test_architecture_boundaries.py):
- routes may not import `backend.data` directly
- `operator_status_service.py` may not import `backend.orchestration.run_model_pipeline`
- new `shared.py`, `common.py`, and vague `*manager.py` files are rejected under `backend/`

## What These Guardrails Prevent

- route-to-data leakage returning through later edits
- operator-service/orchestrator coupling regressing
- visual structure drift through vague catch-all modules
- new hidden path-retargeting helpers creeping in through convenience edits

## Low-Overhead Maintenance Rule

When adding a new module or feature:
- place it near the surface it primarily serves
- prefer extending an existing coherent owner over creating a vague new module
- if a new exception to the invariants is truly necessary, document it here and in `dependency-rules.md`
