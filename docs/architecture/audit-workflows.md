# Audit: Workflow And Execution Paths

Date: 2026-03-16
Reviewer: Independent architecture audit
Status: Execution-path review

Update note:

- The hidden runtime path mutation finding documented here was remediated in follow-up Batch 2 later on 2026-03-16.
- The `stage_runner.py` family-split finding documented here was remediated in follow-up Batch 3 later on 2026-03-16.
- References to `temporary_runtime_paths()` below are preserved as historical audit evidence, not current-state claims.

## Summary

The execution model is more understandable than it was before the restructure, but it is still not simple.

A new developer can trace the major flows, but only if they know where the indirection points are:

- refresh API -> refresh manager -> orchestrator -> stage execution -> stage runner -> analytics pipeline
- serve request -> route -> dashboard payload service or direct data adapters
- holdings mutation -> service -> Neon holdings adapter -> optional refresh trigger

The paths are traceable, but not yet consistently explicit.

## Can A New Developer Trace The Main Flows?

### 1. “Refresh data”

Trace:

- `backend/api/routes/refresh.py`
- `backend/services/refresh_manager.py`
- `backend/orchestration/run_model_pipeline.py`
- `backend/orchestration/stage_execution.py`
- `backend/orchestration/stage_runner.py`
- `backend/analytics/pipeline.py`
- `backend/analytics/services/cache_publisher.py`

Assessment:

- yes, traceable
- no longer hidden in one giant file
- still requires jumping through several orchestration-local modules plus runtime helpers

The main improvement is that stage sequencing is no longer buried in one monolith.

The main remaining problem is that `stage_runner.py` still implements many stages in one large branchy function, so the workflow is separated, but not yet expressed as small stage-family modules.

### 2. “Run model”

Trace:

- same entry as refresh, but with `core-weekly` / `cold-core` profile
- `run_model_pipeline.py` decides policy and selected stages
- `stage_execution.py` runs stages
- `stage_runner.py` handles:
  - source sync
  - readiness
  - raw history rebuild
  - feature build
  - ESTU
  - factor returns
  - risk model
  - serving refresh

Assessment:

- clearer than before
- still concentrated in `stage_runner.py`
- still depends on hidden runtime path switching via `temporary_runtime_paths()`

The biggest workflow clarity issue here is that Neon/workspace/local path selection still happens via global path mutation and stage-level side effects rather than a more explicit runtime context object.

### 3. “Serve request”

Trace for main dashboard surfaces:

- route
- `backend/services/dashboard_payload_service.py`
- `backend/data/serving_outputs.load_runtime_payload(...)`

Assessment:

- portfolio/risk/exposures main payloads are reasonably clear
- this is one of the best outcomes of the refactor

But not all serve paths follow that pattern:

- `backend/api/routes/universe.py` still contains search/history logic inline
- `backend/api/routes/health.py` directly loads data-layer payloads
- `backend/api/routes/exposures.py` still owns history lookup logic

So “serve request” is clear for the core dashboard payloads, but less clear once a route leaves the happy path.

## Tangled Execution Paths

### 1. Holdings mutation implicitly triggers refresh orchestration

`backend/services/holdings_service.py` mutates holdings and can immediately trigger:

- `backend/services.refresh_manager.start_refresh(...)`

That is operationally convenient, but it means data mutation and workflow launch are coupled in one service.

This is a hidden workflow edge. It is not wrong, but it should be treated as an explicit policy decision, not just a local convenience.

### 2. Workspace/local/Neon switching is still implicit

`backend/orchestration/stage_execution.py` chooses which DB paths a stage runs against, and `backend/orchestration/runtime_support.py` mutates runtime paths across modules.

That means the real runtime context of a stage is not obvious from the stage implementation alone.

This is the clearest workflow opacity still left in the codebase.

### 3. Operator truth is not a simple direct read

`backend/services/operator_status_service.py` reconciles:

- live refresh-manager state
- job-run rows
- runtime-state reads
- local fallback state

This is a reasonable operator payload builder, but it means “what is the current truth?” is still not a single-store question.

## Hidden Side Effects

### 1. `temporary_runtime_paths()`

This is the most important hidden side effect in the execution model.

It changes global config/module path state during rebuild windows.

Even though it is restored later, it makes workflow behavior depend on scoped global mutation rather than plain local arguments.

### 2. Cache/runtime-state publication happens inside stage flows

Publication of:

- serving payloads
- runtime-state keys
- active snapshot
- Neon mirror health

is distributed across the refresh pipeline, post-run publish logic, and runtime-state helpers. The pieces are clearer than before, but the side effects are still numerous.

## Entrypoint Quality

### Good

- `backend/api/routes/refresh.py` is acceptably thin
- `backend/scripts/run_model_pipeline.py` is thin
- `backend/main.py` is minimal except for the inline `/api/health` handler

### Not as good

- `backend/main.py` still owns health response assembly directly instead of delegating to a service
- `backend/scripts/run_model_pipeline.py` imports private `_parse_args` from the orchestrator, which is a small but real entrypoint/workflow leakage

## Bottom Line

The execution paths are understandable now, but they are not yet uniformly explicit.

The core improvement is real:

- orchestration is decomposed
- dashboard-serving payload flow is clearer

The main remaining clarity problems are:

- large stage implementation branching in `stage_runner.py`
- hidden runtime path mutation
- mixed route patterns for non-core serving endpoints
