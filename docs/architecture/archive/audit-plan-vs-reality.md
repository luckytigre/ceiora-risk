# Audit: Plan Vs Reality

Date: 2026-03-16
Reviewer: Independent architecture audit
Status: Post-restructure verification

## Summary

The restructuring work is real. The repo is materially cleaner than a monolithic pre-refactor state, and the architecture package under `docs/architecture/` is useful.

The plan is not fully achieved, though. The biggest issue is not that the refactor failed; it is that the docs mark several phases as fully complete while the code still contains meaningful exceptions.

## What Was Actually Achieved

These parts of the plan are substantively implemented:

- `docs/architecture/` exists and is now the best current source for architecture intent.
- route-local payload assembly was reduced for operator/data/dashboard surfaces.
- orchestration was broken into smaller modules:
  - `backend/orchestration/profiles.py`
  - `backend/orchestration/stage_planning.py`
  - `backend/orchestration/stage_execution.py`
  - `backend/orchestration/stage_runner.py`
  - `backend/orchestration/finalize_run.py`
  - `backend/orchestration/post_run_publish.py`
  - `backend/orchestration/runtime_support.py`
- the refresh pipeline was decomposed around:
  - `backend/analytics/refresh_context.py`
  - `backend/analytics/reuse_policy.py`
  - `backend/analytics/publish_payloads.py`
  - `backend/analytics/refresh_persistence.py`
- `backend/data/core_reads.py` and `backend/data/model_outputs.py` were split into smaller supporting modules.
- frontend contracts were split into domain files behind `frontend/src/lib/types.ts`.

These are real structural wins, not just renamed files.

## Where Reality Diverges From The Plan

### 1. Thin-route target is only partially achieved

The target architecture and dependency rules imply routes should delegate to services and presenters, but several routes still depend directly on `backend.data`:

- `backend/api/routes/exposures.py`
- `backend/api/routes/risk.py`
- `backend/api/routes/portfolio.py`
- `backend/api/routes/health.py`
- `backend/api/routes/universe.py`
- `backend/api/routes/readiness.py`

This is the clearest plan-vs-reality mismatch. Dashboard payload assembly moved out, but route-to-data coupling still exists.

### 2. Orchestration decomposition is real, but the main integration shell is still heavy

`backend/orchestration/run_model_pipeline.py` is no longer a god module in the old sense, but it is still the central integration hub and still owns:

- CLI argument parsing
- run-level policy decisions
- stage dispatch wiring
- cross-module data path setup
- final result assembly

The restructure plan marks orchestration decomposition as complete. That is slightly overstated. It is better described as “substantially improved, but still central.”

### 3. Storage cleanup is only partially complete

`backend/data/core_reads.py` and `backend/data/model_outputs.py` are now facades, but they are not especially thin.

They still contain:

- fallback/authority policy
- wiring helpers
- internal wrapper methods
- public behavior plus refactor-era compatibility shaping

The plan marks storage cleanup as complete. In practice it is “good enough for now,” not “fully resolved.”

### 4. Frontend contract cleanup is structurally true but behaviorally partial

The barrel split happened:

- `frontend/src/lib/types/analytics.ts`
- `frontend/src/lib/types/data.ts`
- `frontend/src/lib/types/health.ts`
- `frontend/src/lib/types/holdings.ts`
- `frontend/src/lib/types/operator.ts`

But almost every frontend consumer still imports from `@/lib/types`. So the split improved file ownership, but it did not yet narrow dependency surfaces in most consumers.

### 5. Documentation consolidation is improved, not fully settled

The docs now clearly point to `docs/architecture/` as the active architecture package. That is good.

But the plan’s “documentation consolidation completed” status is stronger than the actual state:

- multiple top-level plan documents still exist
- several still read like operationally relevant live artifacts
- `docs/PROJECT_HARDENING_ORGANIZATION_PLAN.md` is marked completed, but it still reads like an active program document after the status line

## Parts Of The Plan That Were Implemented Inconsistently

### Target directory shape

The target architecture document includes structures that were not actually adopted:

- `backend/api/presenters/`
- `backend/analytics/payload_builders/`
- `backend/data/model_outputs/` as a subpackage

Reality uses:

- `backend/api/routes/presenters.py`
- flat helper modules in `backend/analytics/`
- flat helper modules in `backend/data/`

This is not necessarily bad, but it means the target architecture doc is more aspirational than descriptive in those sections.

### Service boundaries

The target architecture implies `backend/services/` is the application-service layer. In reality it still mixes:

- route/application services
- infrastructure-heavy Neon sync/mirror modules
- holdings data-manipulation internals

That mixed ownership is not reconciled in the plan.

## Parts Of The Plan That Are Now Outdated Or Unrealistic

### 1. The package-level directory targets are too specific

The target document’s package shape is more specific than necessary in places. The flat helper-module approach now in the codebase is reasonable, and forcing everything into deeper package trees would likely add churn without meaningful clarity.

The main examples:

- `backend/api/presenters/`
- `backend/analytics/payload_builders/`
- `backend/data/model_outputs/`

These should now be treated as optional future shapes, not still-implied completion criteria.

### 2. “Completed” phase labels should be softened

The plan is stronger if it admits:

- route thinness is still uneven
- storage facades are still transitional
- orchestration is still concentrated in one integration shell

The current phase labels overstate finality.

## Bottom Line

The implemented code did not merely “look cleaner.” The refactor delivered real improvements.

But the documentation currently overstates how complete the target architecture is. The right interpretation is:

- architecture direction: correct
- implementation quality: materially improved
- completion level: partial, with several important exceptions still present
