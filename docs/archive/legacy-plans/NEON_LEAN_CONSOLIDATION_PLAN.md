# Neon Lean Consolidation Plan

Date: 2026-03-16
Owner: Codex
Status: Completed on 2026-03-16

## Review Log

### 2026-03-16: Pre-Implementation Plan Review

Reviewer A, architecture/runtime:

- approved the plan as narrow enough for a hobby tool
- required one constraint:
  - do not force the shared helper into `backend/data/model_outputs.py` unless it obviously reduces reasoning cost
- required one emphasis:
  - runtime-state hardening must reduce quiet failure without expanding `runtime_state` into a broader cache framework

Reviewer B, data/test/docs:

- approved the plan as codebase-aware and appropriately scoped
- required one constraint:
  - `runtime_state_current` should be covered by a light readiness/parity contract, not a new heavy mirror workflow unless clearly necessary
- required one emphasis:
  - docs should treat this file as the governing plan for the simplification pass and avoid duplicating the same checkpoint narrative elsewhere

### 2026-03-16: Mid-Implementation Review

Reviewer A, architecture/runtime:

- approved Workstream 2 and Workstream 3 implementation
- confirmed that the shared helper reduced duplication in `serving_outputs` and `runtime_state`
- confirmed that leaving `model_outputs` untouched in this pass is the correct tradeoff because forcing the helper there now would not materially simplify the file

Reviewer B, observability/drift:

- approved the runtime-state hardening approach
- confirmed that `health` and `operator` now form the intended light integrity contract for `runtime_state_current`
- confirmed that additional heavy mirror wiring for `runtime_state_current` is not needed in this pass

### 2026-03-16: Final Review

Reviewer A, architecture/runtime:

- approved the completed pass
- confirmed that the shared Neon-primary helper reduced duplicated storage-policy logic in `serving_outputs` and `runtime_state`
- confirmed that the runtime-state surface stayed intentionally small rather than turning into a generalized cache layer
- recorded one explicit residual, not a blocker:
  - `backend/data/model_outputs.py` and the scratch-workspace rebuild path remain larger and more transitional than ideal, but those are intentionally outside this simplification pass

Reviewer B, docs/tests/operability:

- approved the completed pass
- confirmed that `/api/health` and `/api/operator/status` now surface runtime-state source and status clearly enough to expose missing or degraded runtime truth
- confirmed that the docs now describe the lean contract and remaining SQLite rebuild caveat honestly
- recorded one explicit residual, not a blocker:
  - `runtime_state_current` uses the intended light health/operator integrity contract rather than full mirror parity, so future deeper Neon migration work should revisit whether stronger parity evidence is needed

## Purpose

This plan is the execution contract for the next Neon migration wave.

Its goal is not to add more platform surface area.

Its goal is to make the current Neon-first changes:

- leaner
- easier to reason about
- less redundant
- more observable
- less likely to drift quietly

This is explicitly a hobby-tool discipline pass. The app should stay simple enough to operate and debug without enterprise-style abstraction layers.

## Governing Principles

1. Prefer small explicit modules over generalized frameworks.
2. Remove duplicated storage-policy code before adding more Neon surfaces.
3. Keep `runtime_state` tiny and fixed-scope.
4. Make quiet failure harder.
5. Do not hide transitional SQLite dependencies behind vague wording.
6. Do not attempt the full Neon-native rebuild migration in this pass.

## Non-Goals

This pass does not attempt to:

- remove the Neon-backed scratch SQLite rebuild workspace
- port the core math engine to Postgres-native execution
- replace the full SQLite cache system
- introduce a large repository or service-layer framework

## Scope

This pass will address four specific problems:

1. duplicated Neon-primary plus SQLite-mirror write policy
2. weak runtime-state observability and quiet failure risk
3. oversized storage-heavy modules that now carry too much policy logic
4. documentation drift and migration-plan sprawl

## Acceptance Criteria

This pass is complete only when all of the following are true:

- one shared helper owns the common Neon-primary write policy for at least `serving_outputs` and `runtime_state`
- `runtime_state` is explicitly restricted to a small allowed key set
- runtime-state write/read failures are surfaced more clearly in operator and health behavior
- `runtime_state_current` is covered by Neon schema/readiness/parity or an explicit equivalent health contract
- storage policy duplication is reduced, not increased
- docs describe the lean contract clearly
- tests demonstrate the new failure and drift behavior

## Workstreams

### Workstream 1: Plan Review And Seam Map

Goal:

- validate that the planned slimming work matches the current code seams

Tasks:

1. review the plan against current storage modules and operator surfaces
2. map every current seam where the same persistence policy is repeated
3. list the exact runtime-state keys that should remain durable

Exit criteria:

- the plan is updated with reviewer feedback
- the seam map is explicit enough to guide implementation

### Workstream 2: Shared Neon-Primary Persistence Policy

Goal:

- remove duplicate write-order and required/optional failure logic

Tasks:

1. add a small shared helper for:
   - Neon-primary write
   - optional SQLite/local mirror write
   - required versus optional Neon failure policy
   - consistent result payload
2. adopt that helper in:
   - `backend/data/serving_outputs.py`
   - `backend/data/runtime_state.py`
3. adopt it in `backend/data/model_outputs.py` only if the result is clearly simpler rather than more abstract

Guardrails:

- no class hierarchy
- no generic repository framework
- helper should stay small and obvious

Exit criteria:

- serving outputs and runtime state no longer each own a hand-rolled copy of the same policy

### Workstream 3: Runtime-State Hardening

Goal:

- keep runtime state lean and make failures more visible

Tasks:

1. explicitly restrict durable runtime-state keys to:
   - `risk_engine_meta`
   - `neon_sync_health`
   - `__cache_snapshot_active`
2. make runtime-state read behavior less silent for cloud-serving/operator truth paths
3. make runtime-state write results visible enough to detect drift or failed mirror writes
4. wire `runtime_state_current` into Neon schema and the relevant readiness/parity contract

Exit criteria:

- runtime state is a tiny operator/recovery surface, not a second general cache
- operator/health behavior can distinguish missing runtime-state truth from healthy state

### Workstream 4: Targeted Module Slimming

Goal:

- reduce the weight of the worst storage-heavy modules without over-refactoring

Tasks:

1. extract one small helper module for shared Neon-primary persistence policy
2. split `backend/data/model_outputs.py` only if the split makes the file more legible and lowers duplication
3. extract orchestration-side Neon status publication helpers from `backend/orchestration/run_model_pipeline.py` if doing so simplifies that file materially

Exit criteria:

- module count may increase slightly, but overall reasoning cost decreases

### Workstream 5: Docs And Cleanup

Goal:

- keep the repo’s operating model and execution docs compact and consistent

Tasks:

1. update canonical docs with the lean contract
2. mark this plan as the governing file for the consolidation pass
3. avoid duplicating the same execution status across too many planning docs

Exit criteria:

- a reader can tell what changed, what remains transitional, and what file governs the next work

## Seam Map

Current repeated persistence-policy seams to consolidate:

- `backend/data/serving_outputs.py`
- `backend/data/runtime_state.py`
- `backend/data/model_outputs.py`

Concrete repeated logic already present in code:

- `backend/data/serving_outputs.py:94-125`
  - constructs a result envelope
  - decides Neon-primary versus SQLite-only
  - decides required versus optional Neon failure behavior
  - applies SQLite mirror fallback
- `backend/data/runtime_state.py:121-168`
  - constructs a parallel result envelope
  - decides Neon-primary versus fallback writer behavior
  - decides required versus optional Neon failure behavior
  - applies fallback writer status handling
- `backend/data/model_outputs.py:881-946`
  - owns another custom Neon-primary plus SQLite-mirror decision block
  - should only adopt the shared helper if doing so clearly reduces complexity

Concrete runtime-state keys that remain in scope for durable Neon runtime state:

- `risk_engine_meta`
- `neon_sync_health`
- `__cache_snapshot_active`

Concrete runtime-state observability seams to harden:

- `backend/main.py`
  - health should not treat missing runtime-state truth the same as healthy runtime-state truth
- `backend/api/routes/operator.py`
  - operator truth should distinguish missing runtime-state payloads from real payloads
- `backend/analytics/pipeline.py`
  - runtime-state write results should not be silently ignored
- `backend/orchestration/run_model_pipeline.py`
  - Neon sync and serving write health publication should not silently ignore runtime-state write results

Concrete schema/readiness seams to harden:

- `backend/services/neon_authority.py:240-330`
  - currently validates model tables but not `runtime_state_current`
- `backend/services/neon_stage2.py`
  - currently treats `serving_payload_current` and model tables as canonical, but not `runtime_state_current`
- `backend/services/neon_mirror.py`
  - parity/prune coverage should either include `runtime_state_current` or explicitly state why a lighter equivalent check is used

Current runtime-state read/write consumers that must stay aligned:

- `backend/analytics/pipeline.py`
- `backend/orchestration/run_model_pipeline.py`
- `backend/api/routes/operator.py`
- `backend/main.py`

Current remaining SQLite-heavy seams that are intentionally out of scope for this pass:

- `backend/analytics/services/cache_publisher.py`
- `backend/data/sqlite.py`
- `backend/services/portfolio_whatif.py`
- rebuild execution in `backend/orchestration/run_model_pipeline.py`

## Review Protocol

Before implementation:

- Reviewer A, architecture/runtime:
  - verify that the plan stays narrow and does not introduce framework bloat
  - verify that the plan addresses quiet failure and drift risk
- Reviewer B, data/test/docs:
  - verify that the plan covers schema/readiness/parity implications
  - verify that docs and tests are included as first-class work

During implementation:

- after Workstream 2, run one architecture/runtime review
- after Workstream 3, run one observability/drift review
- after Workstream 4 and 5, run one final two-reviewer audit

Each review must output:

- findings only
- explicit file references
- approval or rejection

## Validation Protocol

Minimum checks for this pass:

- compile check for touched backend modules
- targeted pytest for:
  - persistence helper coverage
  - serving outputs
  - runtime state
  - operator route
  - health route
- broader pytest slice for:
  - operating model contract
  - refresh profiles
  - cloud runtime roles

## Final Acceptance Questions

1. Did this pass reduce duplicated storage-policy code?
2. Is `runtime_state` clearly smaller and more disciplined than before?
3. Are quiet failures less likely and easier to observe?
4. Did we avoid unnecessary architecture weight?
5. Are the docs still honest about the remaining scratch-SQLite rebuild caveat?

## Execution Outcome

This pass is complete.

Implemented:

- added `backend/data/neon_primary_write.py` as the small shared Neon-primary persistence helper
- moved `backend/data/serving_outputs.py` to Neon-primary writes through the shared helper while preserving SQLite as the mirror/local diagnostic surface
- added `backend/data/runtime_state.py` as the narrow Neon-backed runtime-state surface
- restricted durable runtime-state keys to:
  - `risk_engine_meta`
  - `neon_sync_health`
  - `__cache_snapshot_active`
- updated `backend/main.py` and `backend/api/routes/operator.py` to expose runtime-state status/source instead of treating missing runtime truth as implicitly healthy
- fixed rebuild-time `serving_refresh` so core/cold-core publish from the same local/workspace source tables that just produced rebuilt raw history instead of reading stale Neon source surfaces mid-run
- fixed light `serve-refresh` so stale runtime-state cannot overwrite the current weekly core model: effective core-state now falls back to the latest durable `model_run_metadata` before reusing or republishing loadings
- tightened serving payload source-date assembly so published loadings dates are derived from rebuilt payload/eligibility truth rather than stale upstream date fields
- updated canonical docs to describe the lean contract and remaining transitional seams

Deliberately left out of this pass:

- refactoring `backend/data/model_outputs.py` into smaller modules
- removing the Neon-backed scratch SQLite rebuild workspace
- broad cache-system replacement
- heavy parity/mirror workflow for `runtime_state_current`

Validation completed:

- compile checks on touched backend modules and tests
- focused pytest coverage for helper, serving outputs, runtime state, operator route, and health route
- broader pytest coverage for operating-model contract, refresh profiles, cache publisher, and cloud runtime roles
- final integrated validation run passed:
  - `122 passed in 46.10s`

Residual follow-up after this pass:

- `backend/data/model_outputs.py` is still the largest custom persistence module and remains the next best refactor target if more simplification is needed
- ordinary rebuild execution still uses the Neon-backed scratch SQLite workspace and is still the main architectural caveat in the broader Neon migration
