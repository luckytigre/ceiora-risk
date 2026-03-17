# Current Architecture Diagnosis

Date: 2026-03-16
Status: Current-state diagnosis
Owner: Codex

## Scope

This diagnosis covers the full repository with emphasis on:
- backend execution paths
- data flow and storage authority
- frontend truth assembly
- entrypoints, jobs, and workflows
- current documentation structure

It is grounded in the current codebase and the canonical operating-model docs:
- [../ARCHITECTURE_AND_OPERATING_MODEL.md](../ARCHITECTURE_AND_OPERATING_MODEL.md)
- [../OPERATIONS_PLAYBOOK.md](../OPERATIONS_PLAYBOOK.md)
- [../README.md](../README.md)

## System Overview

The repository is a single-product application with five major technical areas:

1. `backend/`
   - FastAPI API
   - refresh/orchestration jobs
   - analytics payload construction
   - risk-model compute
   - Neon and SQLite persistence

2. `frontend/`
   - Next.js dashboard
   - operator, data, positions, exposures, risk, explore surfaces

3. `scripts/` and `backend/scripts/`
   - CLI entrypoints and operational utilities

4. `docs/`
   - architecture, runbooks, plans, and migration notes

5. `data/`
   - local reference artifacts and SQLite-backed operational data

The architectural intent is already strong:
- Neon is becoming the main durable operating platform.
- SQLite remains local ingest/archive and transitional scratch.
- the frontend is meant to read durable serving surfaces instead of rebuilding truth ad hoc.

The main problem is not conceptual confusion. The problem is structural sprawl around that concept.

## Major Subsystems

### Backend API

Primary entrypoints:
- `backend/main.py`
- `backend/api/router_registry.py`
- `backend/api/routes/*`

Strengths:
- route registry is explicit
- auth boundaries are understandable
- recent refactors already thinned `operator` and `data` routes

Problems:
- route thinness is inconsistent
- `operator` and `data` routes are now properly thin, and `portfolio` / `risk` / `exposures` now delegate serving-payload assembly to a dedicated service
- some routes still own specialized history or what-if behavior inline, so route consistency is improved but not complete
- response contracts are partly implicit in route code and partly implicit in frontend expectations

### Refresh / Orchestration

Primary entrypoints:
- `backend/orchestration/run_model_pipeline.py`
- `backend/services/refresh_manager.py`
- `backend/scripts/run_model_pipeline.py`

Strengths:
- profile-driven operational model is explicit
- lanes and stages are discoverable in one file
- job status and refresh status exist

Problems:
- one orchestrator file owns too much:
  - stage planning
  - stage execution
  - readiness gating
  - scratch workspace behavior
  - artifact writing
  - CLI concerns
- profile and stage-selection metadata have now been extracted to `backend/orchestration/profiles.py`, but the main execution and publication logic still live together
- as-of/session planning and post-run Neon publication now also live in dedicated orchestration modules, but stage execution and scratch-workspace control are still concentrated in one file
- the stage loop, finalization flow, and `_run_stage` implementation are now extracted as orchestration-local modules, and stage implementation is now split into `stage_source.py`, `stage_core.py`, and `stage_serving.py`; `run_model_pipeline.py` still imports many lower-layer dependencies and remains the main integration hub
- `refresh_manager` imports orchestration details directly, which couples API-driven refresh management to the full job engine
- orchestration policy and execution are too intertwined

### Analytics / Serving

Primary modules:
- `backend/analytics/pipeline.py`
- `backend/analytics/services/cache_publisher.py`
- `backend/analytics/services/universe_loadings.py`
- `backend/analytics/services/risk_views.py`
- `backend/analytics/health.py`
- `backend/analytics/contracts.py`

Strengths:
- serving payloads are explicit named surfaces
- `analytics/contracts.py` gives some typed structure
- caching and publish behavior are operationally grounded

Problems:
- `pipeline.py` is now thinner, but it still remains the main refresh coordinator and heavy serving integration hub
- `cache_publisher.py` is materially smaller after helper extraction, but it still remains the main serving snapshot staging coordinator
- `health.py` is a heavy diagnostics engine mixed into the same package as lighter serving work
- boundaries between “reusable analytics computation” and “refresh workflow behavior” are not sharp

### Data Access / Persistence

Primary modules:
- `backend/data/core_reads.py`
- `backend/data/model_outputs.py`
- `backend/data/serving_outputs.py`
- `backend/data/runtime_state.py`
- `backend/data/job_runs.py`
- `backend/data/sqlite.py`
- `backend/data/neon.py`

Strengths:
- durable serving outputs, runtime state, and model outputs have become explicit surfaces
- Neon-primary write policy now exists
- storage intent is much clearer than earlier repo states

Problems:
- `core_reads.py` is now a stable facade over smaller transport/source modules, but legacy callers still patch facade-private helpers
- `model_outputs.py` is now a stable facade over smaller schema/state/payload/writer modules, but the public file still preserves many compatibility hooks for tests
- read authority is explicit, but implementation still fans out across multiple files with similar branching
- runtime truth still has multiple overlapping surfaces:
  - `runtime_state`
  - `job_runs`
  - `refresh_status`
  - durable serving payload metadata
  - local cache fallbacks

### Neon / Sync / Mirror Infrastructure

Primary modules:
- `backend/services/neon_mirror.py`
- `backend/services/neon_stage2.py`
- `backend/services/neon_authority.py`
- `backend/services/neon_holdings.py`
- `backend/services/neon_holdings_identifiers.py`
- `backend/services/neon_holdings_store.py`

Strengths:
- Neon migration work is concrete and well documented
- mirror/parity logic is explicit rather than hidden
- holdings is now a dedicated service area
- holdings workflows are clearer after separating identifier resolution and persistence primitives out of `neon_holdings.py`

Problems:
- Neon sync infrastructure is operationally important but spread across several large modules
- mirror/parity code mixes schema, sync, pruning, audit, and repair-oriented logic
- rebuild authority, mirror authority, and durable-serving authority are clearer conceptually than they are structurally

### Cross-Section Snapshot Surface

Primary modules:
- `backend/data/cross_section_snapshot.py`
- `backend/data/cross_section_snapshot_schema.py`
- `backend/data/cross_section_snapshot_build.py`

Strengths:
- the rebuild entrypoint is now explicit and smaller
- schema maintenance is separated from source loading and payload assembly

Problems:
- snapshot build logic is still operationally dense and should stay behind the stable facade instead of leaking into callers

### Diagnostics Service Surface

Primary modules:
- `backend/services/data_diagnostics_service.py`
- `backend/services/data_diagnostics_sections.py`
- `backend/services/data_diagnostics_sqlite.py`

Strengths:
- the route-facing diagnostics surface is now explicit and smaller
- low-level SQLite inspection is separated from section-level diagnostics assembly

Problems:
- this remains a local diagnostics surface backed by direct SQLite inspection, so it should stay deliberately narrow and not turn into a second operator-status system

### Risk Model / Domain Compute

Primary modules:
- `backend/risk_model/*`
- `backend/universe/*`
- `backend/portfolio/*`

Strengths:
- numeric and model-specific code is mostly kept away from the API layer
- factor catalog and regression components are reasonably isolated

Problems:
- some “domain” areas still depend directly on storage-layer concerns
- `daily_factor_returns.py` and `raw_cross_section_history.py` are large workflow-heavy modules rather than narrow compute kernels
- domain packages are not yet consistently separated from persistence and orchestration

### Frontend

Primary areas:
- `frontend/src/app/*`
- `frontend/src/features/*`
- `frontend/src/components/*`
- `frontend/src/lib/*`

Strengths:
- page boundaries are understandable
- Health/Data/Positions/Exposures are now more distinct in purpose
- `analyticsTruth.ts` centralizes some important page-level freshness logic

Problems:
- some pages still merge backend payloads locally
- frontend contract types now live behind `src/lib/types.ts`, but a few pages still consume a very broad barrel instead of narrower feature-local imports
- page shells and feature modules are cleaner than the backend, but the API contract layer could be more explicit

### Docs

Strengths:
- canonical operating-model docs are strong
- migration history is well preserved
- `docs/architecture/` now provides one current architecture package with diagnosis, target structure, dependency rules, and active restructure status

Problems:
- several focused top-level execution plans still exist, so doc status has to stay explicit to avoid them reading like competing master plans

## Cross-Cutting Diagnosis

### 1) Mixed Responsibilities

The largest structural issue is responsibility mixing.

Key examples:
- `backend/orchestration/run_model_pipeline.py`
  - catalog + workflow planning + execution + reporting + CLI
- `backend/analytics/services/cache_publisher.py`
  - now narrower, but still owns staged serving snapshot coordination and payload publication coupling
- `backend/analytics/pipeline.py`
  - context loading + reuse policy + build logic + persistence decisions
- `backend/data/model_outputs.py`
  - schema + extraction + transformation + durable writes
- `backend/data/core_reads.py`
  - adapter selection + SQL translation + domain query library + local cache maintenance

### 2) “God Files”

The repository’s biggest fragility points are large ownership-heavy modules:
- `backend/orchestration/run_model_pipeline.py` is no longer a god module, but it remains the central orchestration integration shell
- `backend/services/neon_mirror.py`
- `backend/analytics/health.py`
- `backend/risk_model/daily_factor_returns.py`
- `frontend/src/lib/types.ts` is now only a barrel, but its callers still need gradual narrowing

These files are not large only because the domain is large. They are large because multiple roles are merged into one place.

### 3) Implicit Protocols

Some important contracts are still implicit:
- runtime-state key semantics
- serving payload minimum fields and metadata behavior
- route-level response normalization rules
- operator-status vs dashboard-truth ownership
- stage-to-stage orchestration contracts inside `run_model_pipeline.py`

The app has many protocols. They just are not all named and housed clearly.

### 4) Overlapping Truth Surfaces

The repo now has better truth surfaces than before, but still too many places can answer similar questions:
- “what snapshot is active?”
- “what run is current?”
- “is core due?”
- “what source date is authoritative?”
- “what should the frontend display?”

This overlap increases fragility even when the implementation is individually reasonable.

### 5) Dependency Direction Is Not Obvious Enough

The intended layering is roughly:
- routes
- services/application
- data/infrastructure
- domain compute
- orchestration/jobs

But actual dependencies are less clean:
- services import orchestration
- orchestration imports services
- routes sometimes bypass service layers
- data-access modules carry business-policy behavior

The current code works, but dependency direction is not self-evident.

### 6) Orchestration And Reusable Logic Are Tangled

The repo needs a clearer distinction between:
- reusable routines
- workflows
- jobs
- command/control entrypoints

That distinction is one of the main missing architectural boundaries.

### 7) Naming And Discoverability Issues

Current names are mostly understandable, but several patterns blur ownership:
- `services/` contains both application services and infrastructure-heavy workflows
- `data/` contains both adapters and domain-shaped query helpers
- `analytics/services/` mixes payload-build helpers with view-model logic
- multiple plan docs appear equally current

## What Is Actually Working Well

The diagnosis is not “the repo is a mess.” Several choices are strong and should be preserved:
- one backend app and one frontend app
- explicit refresh profiles
- explicit durable serving payloads
- explicit operator page and data diagnostics page
- Neon migration tracked in docs and code
- recent extraction of route-local assembly into dedicated services
- avoidance of overengineered frameworks so far

The right move is not reinvention. It is disciplined separation, thinning, and consolidation.

## Primary Architecture Risks

1. High change risk in oversized modules.
2. Drift risk from overlapping truth surfaces.
3. Maintenance risk from route/service/data inconsistency.
4. Operability risk from orchestration internals being too monolithic.
5. Discoverability risk for future contributors and coding agents.

## Recommended Immediate Direction

The next architecture moves should be:

1. Make route entrypoints consistently thin.
2. Centralize dashboard-serving truth assembly behind explicit service modules.
3. Break orchestration into profile, planning, execution, and post-run publication roles.
4. Break refresh pipeline into context, reuse policy, payload builders, and coordinator.
5. Split oversized storage modules by surface ownership instead of adding generic abstractions.
6. Consolidate architecture documentation into this `docs/architecture/` package.
