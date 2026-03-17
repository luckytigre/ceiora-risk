# Target Architecture

Date: 2026-03-16
Status: Canonical target structure
Owner: Codex

## Synthesis Summary

This target architecture synthesizes three distinct perspectives:
- boundary stability
- simplification
- workflow clarity

### What All Three Perspectives Agree On

1. Routes should be thin and delegate.
2. `run_model_pipeline.py` should stop being a god module.
3. `analytics/pipeline.py` should be decomposed by role.
4. truth assembly should be centralized in a few explicit service modules.
5. generic frameworks and broad abstraction layers would be a mistake.
6. the repo should preserve its operating model while reducing structural sprawl.

### Where They Disagree

- The boundary view is most willing to formalize layers.
- The simplifier view wants fewer new modules and more deletion/consolidation.
- The workflow view prioritizes execution-path clarity over package purity.

### Recommended Direction

Use the boundary view as the structural skeleton, but apply the simplifier’s restraint and the workflow view’s vocabulary.

In practice:
- clarify layers
- avoid broad renames
- split only where ownership becomes sharper
- keep workflows explicit and entrypoints thin

## Canonical Layer Definitions

### 1) Entrypoints

What belongs here:
- FastAPI routes
- CLI wrappers
- local app scripts

Responsibilities:
- parse input
- authorize
- validate
- delegate
- translate exceptions to transport-level responses

Should not own:
- cross-surface truth assembly
- business workflows
- storage branching

Current homes:
- `backend/api`
- `backend/scripts`
- `scripts/local_app`

### 2) Application Services

What belongs here:
- route-facing or use-case-facing composed services
- workflow surfaces that are smaller than a full job
- runtime/operator/data/dashboard assembly services
- holdings mutation and preview services

Responsibilities:
- orchestrate reusable routines and adapters for one application surface
- assemble canonical payloads
- define one place where a UI-facing or API-facing concept is built

Current home:
- `backend/services`

### 3) Jobs / Workflows

What belongs here:
- named refresh and rebuild jobs
- stage planning
- stage execution
- post-run publication/reporting

Responsibilities:
- sequence multi-step operational work
- record checkpoints and artifacts
- call domain routines and adapters
- pass runtime execution context, including workspace/canonical db targets, explicitly rather than mutating process-wide module state

Current home:
- `backend/orchestration`

### 4) Domain / Compute

What belongs here:
- factor model routines
- analytics builders
- portfolio math
- universe and ESTU logic

Responsibilities:
- reusable computation
- domain transformations
- model-specific logic

Current homes:
- `backend/risk_model`
- `backend/analytics`
- `backend/universe`
- `backend/portfolio`

### 5) Infrastructure / Adapters

What belongs here:
- Neon and SQLite connection helpers
- durable payload stores
- runtime-state store
- job-run store
- sync/mirror adapters

Responsibilities:
- persistence
- provider-specific I/O
- bounded adapter logic

Current homes:
- `backend/data`
- parts of `backend/services` for Neon sync/mirror

## Canonical Target Directory Shape

This is the target shape. It is evolutionary, not a required big-bang rewrite.

```text
backend/
  api/
    auth.py
    router_registry.py
    routes/
    presenters/
  services/
    dashboard_payload_service.py
    data_diagnostics_service.py
    operator_status_service.py
    holdings_service.py
    portfolio_whatif.py
    refresh_manager.py
    neon_*.py
  orchestration/
    profiles.py
    stage_planning.py
    runtime_support.py
    stage_runner.py
    stage_source.py
    stage_core.py
    stage_serving.py
    post_run_publish.py
    run_model_pipeline.py
  analytics/
    contracts.py
    refresh_context.py
    reuse_policy.py
    payload_builders/
    health.py
    services/
  data/
    neon.py
    sqlite.py
    source_reads.py
    source_dates.py
    serving_outputs.py
    runtime_state.py
    job_runs.py
    model_outputs/
      __init__.py
      schema.py
      writers.py
      readers.py
      metadata.py
  risk_model/
  universe/
  portfolio/
```

Not every target path needs to exist immediately. The point is ownership, not directory churn.

The first pieces of this shape now exist in code:
- `backend/services/operator_status_service.py`
- `backend/services/data_diagnostics_service.py`
- `backend/services/dashboard_payload_service.py`
- `backend/orchestration/profiles.py`
- `backend/orchestration/stage_planning.py`
- `backend/orchestration/post_run_publish.py`
- `backend/orchestration/runtime_support.py`
- `backend/orchestration/stage_execution.py`
- `backend/orchestration/finalize_run.py`
- `backend/orchestration/stage_runner.py`
- `backend/orchestration/stage_source.py`
- `backend/orchestration/stage_core.py`
- `backend/orchestration/stage_serving.py`
- `backend/analytics/refresh_context.py`
- `backend/analytics/reuse_policy.py`
- `backend/analytics/publish_payloads.py`
- `backend/analytics/refresh_persistence.py`
- `backend/analytics/refresh_metadata.py`
- `backend/analytics/health_payloads.py`
- `backend/data/core_read_backend.py`
- `backend/data/source_dates.py`
- `backend/data/source_reads.py`
- `backend/data/model_output_schema.py`
- `backend/data/model_output_state.py`
- `backend/data/model_output_payloads.py`
- `backend/data/model_output_writers.py`

## Ownership Of Major Areas

| Area | Target Owner |
| --- | --- |
| operator truth | `backend/services/operator_status_service.py` |
| dashboard-serving truth for portfolio/risk/exposures | `backend/services/dashboard_payload_service.py` |
| data diagnostics payload | `backend/services/data_diagnostics_service.py` |
| refresh API lifecycle state | `backend/services/refresh_manager.py` |
| refresh/rebuild job sequencing | `backend/orchestration/*` |
| serving payload persistence | `backend/data/serving_outputs.py` |
| runtime truth persistence | `backend/data/runtime_state.py` |
| model-output persistence | `backend/data/model_outputs/*` target split |
| source-date and source read helpers | split out of `backend/data/core_reads.py` |
| heavy model diagnostics | `backend/analytics/health.py` and related payload builders |

## Naming Conventions

### Services

Use `*_service.py` only for modules that expose an application-facing surface.

Good examples:
- `operator_status_service.py`
- `data_diagnostics_service.py`
- `dashboard_payload_service.py`

Do not use `service` for:
- raw adapters
- math helpers
- one-off utility bags

### Orchestration

Use names that describe workflow role:
- `profiles.py`
- `stage_planning.py`
- `stage_runner.py`
- `post_run_publish.py`

Avoid vague names like `helpers.py` or `manager.py` unless the module truly manages lifecycle.

### Data / Adapters

Name by surface owned:
- `serving_outputs.py`
- `runtime_state.py`
- `job_runs.py`
- `source_reads.py`
- `source_dates.py`

Avoid names that hide mixed scope.

## Boundary Rules

1. Routes may call services and presenter helpers.
2. Routes should not directly assemble cross-surface truth if a service owns that payload.
3. Services may call analytics, risk-model, universe, portfolio, and data adapters.
4. Services should not import full orchestration jobs merely to inspect static metadata.
5. Orchestration may call services only for narrow post-run publication surfaces or runtime status surfaces when justified.
6. Domain/compute packages must not depend on routes.
7. Data adapters must not depend on route or frontend semantics.
8. Shared code is allowed only when it is:
   - generic,
   - small,
   - used by at least two real consumers,
   - and still clearly owned.

## Protocol / Contract Placement

### Visible Contracts

Keep contracts close to the surface they define:
- analytics payload contracts in `backend/analytics/contracts.py`
- route presenters in `backend/api/presenters/` or `backend/api/routes/presenters.py`
- frontend page/feature contracts split gradually out of `frontend/src/lib/types.ts`

### Implicit Protocols To Make Explicit

1. runtime-state key set
2. dashboard-serving payload minimum fields
3. refresh/orchestration profile metadata
4. source-date ownership
5. route-level normalized field conventions

## Orchestration vs Domain Rules

- workflows coordinate; they do not own all computation
- payload builders compute and shape; they do not decide job policy
- adapters persist; they do not decide business cadence
- entrypoints trigger; they do not assemble the world

## Shared-Code Rule

Do not create a generic `shared/` or `common/` junk drawer.

If code is reused:
- place it near the surface it primarily serves
- keep the API narrow
- name it by responsibility, not by “sharedness”

## What Changes Now vs Later

### Change Now

1. route/service truth consolidation for dashboard surfaces
2. orchestration profile extraction
3. first refresh-pipeline decomposition
4. doc consolidation into `docs/architecture/`

### Change Later

1. deeper `core_reads.py` split
2. `model_outputs.py` multi-file split
3. `frontend/src/lib/types.ts` contract decomposition
4. broader Neon infrastructure slimming
