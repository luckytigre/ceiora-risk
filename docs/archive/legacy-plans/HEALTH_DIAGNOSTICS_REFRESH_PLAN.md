# Health Diagnostics Refresh Plan

Date: 2026-03-16
Owner: Codex
Status: Implemented for the current scoped pass on 2026-03-16

## Implementation Summary

This pass implemented the core lane split without adding extra surface area:

- `serve-refresh` now carries forward or defers `health_diagnostics` instead of recomputing them
- `core-weekly` and `cold-core` now own deep diagnostics recompute
- Health page copy now tells operators to use a core lane when diagnostics were deferred
- no separate `health-refresh` lane was added in this pass
- no new standalone lightweight `health_status` payload was added; existing lightweight truth continues to come from operator status, risk payloads, and model-sanity fields

## Review Log

### 2026-03-16: Pre-Implementation Plan Review

Reviewer A, runtime/architecture:

- approved the lane split
- required one emphasis:
  - the decision to recompute deep diagnostics must come from the core-lane context, not from `light` vs `full` mode alone

Reviewer B, UI/operability:

- approved the plan direction
- required one emphasis:
  - weekly core lag should remain quiet in the UI, while deferred diagnostics should tell the operator to use a core lane instead of implying `serve-refresh` can fix it

### 2026-03-16: Mid-Implementation Review

Reviewer A, backend/runtime:

- approved the explicit `refresh_deep_health_diagnostics` control flowing from the orchestrator into `run_refresh(...)`
- confirmed that `serve-refresh` now carries diagnostics forward or defers them instead of recomputing heavy studies on the quick path

Reviewer B, UI/contracts:

- approved the carried-forward / deferred Health page semantics
- confirmed that the health route and page copy now point operators at `core-weekly` / `cold-core` when deep diagnostics are missing

### 2026-03-16: Final Review

Reviewer A, architecture/runtime:

- approved the completed split
- confirmed that core lanes own deep diagnostics recompute while quick refreshes preserve serving coherence without the heavy study
- recorded one non-blocking follow-up:
  - an explicit diagnostics-only lane is still optional and should only be added if it clearly improves operator workflow

Reviewer B, docs/tests:

- approved the docs and test coverage
- confirmed that the new tests lock in:
  - quick-refresh carry-forward behavior
  - deferred fallback behavior
  - core-lane diagnostics ownership

## Purpose

This plan narrows the role of `health_diagnostics` so that:

- `serve-refresh` stays a genuinely light lane
- deep model-audit analytics move to `core-weekly`, `cold-core`, or an explicit diagnostics lane
- weekly core-model lag remains quiet and normal in the UI
- holdings, prices, and latest loadings can publish without dragging heavy model studies behind them

This is an operating-model cleanup, not a model-methodology change.

## Current Problem

`health_diagnostics` is currently computed inside serving snapshot staging in
[cache_publisher.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/barra-dashboard/backend/analytics/services/cache_publisher.py).

That means a nominally light `serve-refresh` can still trigger heavy work from
[health.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/barra-dashboard/backend/analytics/health.py), including:

- long factor-return history analysis
- residual history analysis
- style canonicalization
- raw cross-section exposure snapshot studies
- eligibility / coverage diagnostics

That is the wrong workload shape for the quick refresh lane.

## Target Behavior

### Lane Responsibilities

`serve-refresh` should do only:

- holdings-serving updates
- prices / source-date refresh
- latest served loadings / portfolio / risk presentation refresh
- snapshot publication
- lightweight freshness / coherence status

`core-weekly` and `cold-core` should do:

- factor-return recompute
- covariance / specific-risk recompute
- durable model-output persistence
- deep model-health diagnostics recompute

Optional explicit lane:

- `health-refresh` or `diagnostics-refresh`
- runs deep health analytics without forcing a full cold-core rebuild

### Payload Split

Keep two different health surfaces:

1. `health_status` or equivalent lightweight serving truth
   - snapshot coherence
   - served loadings date
   - latest available source date
   - core model date
   - update-available flags
   - runtime-state / Neon truth indicators

2. `health_diagnostics`
   - deep historical and statistical model-audit payload
   - slower to compute
   - only refreshed on core lanes or explicit diagnostics lane

## Design Principles

1. Do not put deep model studies on the hot path for quick refresh.
2. Preserve Neon as the durable platform; this is not a return to SQLite authority.
3. Keep the UI quiet when the state is normal by design.
4. Prefer reuse of the last good `health_diagnostics` payload over recomputing it during `serve-refresh`.
5. Fail clearly if a deep diagnostics payload is missing when a core lane expected to produce it.

## Workstreams

### Workstream 1: Separate Light Health From Deep Diagnostics

Goal:

- make quick refresh depend only on lightweight health facts

Tasks:

1. inventory every consumer of `health_diagnostics`
2. identify which fields are truly required for ordinary serving
3. introduce a slim lightweight payload or derived status surface for:
   - snapshot freshness
   - source-date alignment
   - served loadings vs core-model cadence
   - update-available state
4. stop using the deep diagnostics payload as the default serving-health truth

Exit criteria:

- ordinary serving pages no longer require a fresh deep diagnostics recompute

### Workstream 2: Remove Deep Diagnostics From `serve-refresh`

Goal:

- make `serve-refresh` reuse cached diagnostics by default

Tasks:

1. change `stage_refresh_cache_snapshot(...)` so `light_mode` does not recompute `health_diagnostics`
2. allow `serve-refresh` to:
   - reuse the current diagnostics payload when present
   - restamp or carry forward metadata if needed
3. if diagnostics are missing, use:
   - last durable payload if available, or
   - a small explicit placeholder status instead of kicking off heavy compute

Exit criteria:

- `serve-refresh` never calls `compute_health_diagnostics(...)` unless explicitly forced

### Workstream 3: Recompute Deep Diagnostics On Core Lanes

Goal:

- attach heavy model-audit work to the lanes that actually change core model state

Tasks:

1. ensure `core-weekly` recomputes `health_diagnostics`
2. ensure `cold-core` recomputes `health_diagnostics`
3. fail closed if those lanes claim success but diagnostics recompute was required and missing
4. persist the refreshed diagnostics into the serving payload surface

Exit criteria:

- deep diagnostics stay in sync with weekly core-model changes

### Workstream 4: Optional Explicit Diagnostics Lane

Goal:

- preserve operator access to deep diagnostics without overloading `serve-refresh`

Tasks:

1. decide whether a separate lane is worth the added surface area
2. if yes, add a small explicit profile such as `health-refresh`
3. make it:
   - operator/API/CLI accessible
   - blocked from ordinary UI one-click flows unless intentionally exposed

Guardrail:

- do not add this lane unless it reduces operational confusion more than it adds complexity

Exit criteria:

- either a clear explicit diagnostics lane exists, or the plan records that core lanes alone own diagnostics

### Workstream 5: Quiet UI Semantics

Goal:

- make weekly core lag read as normal status, not an alert

Tasks:

1. keep banner language compact and matter-of-fact
2. treat this state as normal:
   - loadings current
   - core model on weekly cadence
3. only surface stronger copy when:
   - newer loadings exist but are not served
   - snapshot truth is incoherent
   - runtime / Neon truth is degraded

Exit criteria:

- the common weekly-lag state is quiet everywhere

### Workstream 6: Observability And Tests

Goal:

- prove the quick lane is actually light and the deep lane is still covered

Tasks:

1. add tests showing `serve-refresh` does not recompute `health_diagnostics`
2. add tests showing `core-weekly` / `cold-core` do recompute it
3. add contract tests for fallback behavior when diagnostics are missing
4. add operator/health tests for the lightweight status surface
5. if helpful, add cheap timing/log markers around diagnostics recompute so slow paths are obvious in logs

Exit criteria:

- regressions in lane behavior are detectable by tests

## Independent Review Checkpoints

### Checkpoint A: Plan Review

Reviewer 1, runtime/architecture:

- confirm the lane split is coherent
- confirm this does not undermine Neon-first operating truth

Reviewer 2, product/operability:

- confirm the quick lane remains aligned with the hobby-tool operating model
- confirm the UI semantics stay calm and understandable

### Checkpoint B: Mid-Implementation Review

Reviewer 1:

- verify `serve-refresh` no longer reaches the deep diagnostics path

Reviewer 2:

- verify `core-weekly` / `cold-core` still produce the diagnostics payload and docs remain honest

### Checkpoint C: Final Review

Reviewer 1:

- confirm workload split is correct and no core analytics accidentally remain on the quick path

Reviewer 2:

- confirm UI, docs, and operator surfaces match the implemented behavior

## Test Plan

Minimum required checks:

- unit tests for `stage_refresh_cache_snapshot(...)` light-mode reuse behavior
- unit tests for core-lane diagnostics recompute behavior
- API tests for serving / health behavior when diagnostics are stale or missing
- frontend typecheck after any banner or page changes
- live smoke test:
  - `serve-refresh`
  - `core-weekly` or `cold-core`
  - `/api/health`
  - `/api/operator/status`

## Rollout Order

1. carve out lightweight health status from deep diagnostics consumers
2. stop recomputing `health_diagnostics` on `serve-refresh`
3. ensure core lanes own diagnostics recompute
4. update UI wording and runbooks
5. optionally add an explicit diagnostics lane if still justified

## Acceptance Criteria

This plan is complete only when all of the following are true:

- `serve-refresh` does not trigger heavy health-diagnostics recompute by default
- `core-weekly` and `cold-core` own deep diagnostics refresh
- quick refresh still publishes coherent loadings / portfolio / risk payloads
- weekly core-model lag is treated as normal state in the UI
- docs clearly describe the lane split
- tests lock the behavior in place

## Governing Reference

Use this file as the implementation reference for the diagnostics / refresh split.

Related documents:

- [NEON_LEAN_CONSOLIDATION_PLAN.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/barra-dashboard/docs/NEON_LEAN_CONSOLIDATION_PLAN.md)
- [OPERATIONS_PLAYBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/barra-dashboard/docs/OPERATIONS_PLAYBOOK.md)
