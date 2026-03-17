# Project Hardening And Organization Plan

Date: 2026-03-16
Owner: Codex
Status: Completed precursor to `docs/architecture/restructure-plan.md`

This file is kept for context. Use `docs/architecture/restructure-plan.md` as the active repository-structure tracker.

## Purpose

This plan is the concrete cleanup program for making the project more robust, easier to reason about, and less fragile as Neon becomes the main durable operating platform.

It is not a feature roadmap. Its focus is:
- reducing accidental complexity
- separating responsibilities more cleanly
- shrinking the number of truth surfaces that can drift
- making the wrong data harder to read or publish by mistake
- improving observability without making the UI noisy
- keeping docs and frontend readouts aligned with the actual operating model

## Audit Summary

The codebase is directionally good. It already has:
- a clear intended operating model in `ARCHITECTURE_AND_OPERATING_MODEL.md`
- an explicit runtime/operator surface
- a durable serving payload layer
- improving Neon-first persistence
- a stronger frontend truth split than before

The main risks are organizational, not conceptual.

### Main findings

1. A few modules still carry too many responsibilities.
   - `backend/orchestration/run_model_pipeline.py`
   - `backend/analytics/pipeline.py`
   - `backend/data/model_outputs.py`
   - `backend/data/core_reads.py`
   - `backend/api/routes/data.py`
   - `backend/api/routes/operator.py`

2. Storage policy is clearer than before, but read/write logic is still spread across several modules with similar Neon-vs-SQLite branching.

3. Runtime truth is still assembled from multiple stores:
   - `job_runs`
   - `refresh_status`
   - `runtime_state`
   - durable serving payloads
   - SQLite cache fallbacks

   That is workable during migration, but it increases the chance of drift and stale readouts.

4. The route layer is inconsistent.
   - some routes are thin
   - some still assemble domain logic, readiness logic, and response shaping inline

5. Frontend truth surfaces are better organized now, but still rely on multiple backend payloads with page-level joining logic.
   - that is acceptable for now
   - but the next step should be to make “which dates/snapshot/runtime facts belong on which page” even more explicit

6. The docs are strong, but there are too many active planning documents.
   - the architecture doc is canonical
   - the operations playbook is canonical
   - several Neon plans are still useful, but should be treated as subordinate execution artifacts rather than equal top-level guidance

## Independent Review Feedback

Three independent senior-level review passes over the current codebase and this plan converged on the following refinements:

1. Do not build a large generic architecture.
   - The project needs thinner modules, not a big repository/service framework.
   - Shared code should be introduced only where at least two real surfaces already duplicate the same behavior.

2. Sequence matters more than breadth.
   - The first slice should reduce truth ambiguity and route-local assembly before deeper persistence refactors.
   - If the first step is too broad, the plan risks adding churn without reducing real fragility.

3. Preserve the operating model while cleaning the code.
   - Weekly core lag and faster loadings refresh are correct by design.
   - Cleanup should not accidentally collapse those two operational concepts together.

4. Prefer payload-specific services over abstract generic layers.
   - `operator_status_service` and `data_diagnostics_service` are good fits.
   - A giant “truth manager” or “storage repository” would be the wrong move for a hobby tool.

5. Add explicit non-goals so the hardening plan does not expand indefinitely.
   - no Kubernetes-style architecture
   - no event bus
   - no generalized ORM/repository migration
   - no broad frontend state-management rewrite unless the current page model becomes a real blocker

6. Make documentation cleanup an explicit deliverable, not an afterthought.
   - When a service extraction changes which layer owns truth or wording, docs and frontend copy should be updated in the same slice.

## Non-Goals

This hardening plan is not intended to:
- replace explicit SQL with an ORM
- introduce generalized repository or domain-driven framework layers
- convert the app into a microservice architecture
- eliminate SQLite immediately from all transitional surfaces
- rewrite the frontend into a global state store architecture
- remove all duplication before the truth surfaces are stabilized

## Target End State

The project should converge on this shape:

- one canonical operating model
- one canonical refresh/orchestration model
- one small set of durable truth surfaces
- thin routes
- explicit service modules
- small storage adapters
- frontend pages that consume intentionally designed payloads rather than reconstructing truth ad hoc
- docs that clearly distinguish:
  - canonical architecture
  - operator runbook
  - active implementation plan
  - archived historical plans

## Design Rules

1. Policy, I/O, compute, and presentation should not live in the same file unless the file is genuinely small.

2. Durable truth must be explicit.
   - serving truth
   - operator/runtime truth
   - model-output truth
   - local ingest/archive truth

3. A page should read as few backend surfaces as possible.
   - prefer one composed serving payload over multiple page-local joins when practical

4. The route layer should validate, authorize, and delegate.
   - it should not own domain assembly where a service can do it once

5. Transitional fallback behavior must be visible.
   - no silent Neon-to-SQLite drift
   - no quiet missing-state success semantics

6. Every structural cleanup step must update:
   - docs
   - tests
   - frontend readouts if the meaning of a state or warning changes

## Workstreams

### Workstream 1: Canonical Truth Surface Consolidation

Objective:
- reduce the number of places where freshness, snapshot, and runtime truth are assembled independently

Problems being addressed:
- operator/runtime truth assembled from several stores
- page-level joining of freshness facts
- serving vs runtime vs diagnostics meaning can blur

Implementation:
1. Define and document the canonical truth surfaces in code:
   - `serving_outputs`
   - `runtime_state`
   - `job_runs`
   - `source_dates`
   - `local_archive_source_dates`
2. Add one small backend module that exposes the canonical runtime/operator truth contract.
3. Add one small backend module that exposes the canonical dashboard truth contract.
4. Move route-local truth assembly into those modules.

Likely module targets:
- `backend/services/operator_status_service.py`
- `backend/services/dashboard_truth_service.py`

Required doc updates:
- `docs/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/OPERATIONS_PLAYBOOK.md`

Frontend implications:
- update any Health/Data/Positions/Exposures readouts that are still inferring truth indirectly instead of consuming the explicit contract

### Workstream 2: Orchestrator Decomposition

Objective:
- split `run_model_pipeline.py` into small, role-specific modules

Problems being addressed:
- profile catalog, stage planning, stage execution, Neon artifact writing, CLI behavior, and recovery are all mixed together

Implementation:
1. Extract profile and lane definitions into a dedicated module.
2. Extract stage-window planning into a dedicated module.
3. Extract stage implementations into dedicated functions/modules.
4. Extract post-run Neon mirror/report handling into a dedicated module.
5. Keep `run_model_pipeline.py` as a thin composition/CLI layer.

Likely module targets:
- `backend/orchestration/profiles.py`
- `backend/orchestration/stage_planning.py`
- `backend/orchestration/stage_runner.py`
- `backend/orchestration/post_run_publish.py`

Required doc updates:
- architecture lane descriptions
- operations playbook commands and behavior notes

Frontend implications:
- update operator lane descriptions only if behavior labels change

### Workstream 3: Refresh Pipeline Decomposition

Objective:
- split `backend/analytics/pipeline.py` into explicit refresh-context, reuse-policy, and payload-builder layers

Problems being addressed:
- reuse policy, risk-engine truth resolution, payload staging, cache reuse, and persistence-policy decisions are all mixed

Implementation:
1. Extract refresh-context loading.
2. Extract reuse / cache eligibility decisions.
3. Extract payload building into smaller modules by surface.
4. Keep publish/persist decisions in one thin coordinator layer.

Likely module targets:
- `backend/analytics/refresh_context.py`
- `backend/analytics/reuse_policy.py`
- `backend/analytics/payload_builders/portfolio.py`
- `backend/analytics/payload_builders/risk.py`
- `backend/analytics/payload_builders/exposures.py`
- `backend/analytics/payload_builders/health.py`

Required doc updates:
- serving-layer data flow in the architecture doc
- quick-refresh vs core-lane semantics in the operations playbook

Frontend implications:
- verify Health, Exposures, Positions, and Explore still reflect the same payload semantics

### Workstream 4: Model Output Persistence Cleanup

Objective:
- reduce the size and fragility of `backend/data/model_outputs.py`

Problems being addressed:
- schema creation
- load-from-cache behavior
- Neon writes
- SQLite mirror writes
- metadata persistence
- persistence policy

all live in one file

Implementation:
1. Split schema concerns from persistence concerns.
2. Split data loading from writing.
3. Keep one service-level entrypoint for “persist model outputs.”
4. Remove duplicated responsibility between code-defined schema and migration docs where possible.

Likely module targets:
- `backend/data/model_outputs_schema.py`
- `backend/data/model_outputs_reader.py`
- `backend/data/model_outputs_writer_sqlite.py`
- `backend/data/model_outputs_writer_neon.py`
- `backend/data/model_outputs_service.py`

Required doc updates:
- Neon schema/mirror notes if any contract changes
- architecture doc references to durable model outputs

Frontend implications:
- none expected directly, but any change to model metadata truth should be reflected in Health/Exposures copy if needed

### Workstream 5: Core Read Layer Cleanup

Objective:
- make `core_reads` smaller, clearer, and less backend-specific in one place

Problems being addressed:
- backend switching
- generic SQL helpers
- latest-price cache management
- exposure source resolution
- source-date loading
- table existence logic

Implementation:
1. Split backend-agnostic query functions from backend adapters.
2. Move SQLite-only latest-price cache logic into a dedicated local adapter.
3. Keep one small public surface for domain reads.

Likely module targets:
- `backend/data/core_read_backend.py`
- `backend/data/core_read_queries.py`
- `backend/data/core_read_sqlite.py`
- `backend/data/core_read_neon.py`

Required doc updates:
- architecture references to authoritative source-date loading

Frontend implications:
- verify operator/source-date readouts still match after the change

### Workstream 6: Route-Service-Presenter Separation

Objective:
- make routes thinner and more uniform

Problems being addressed:
- some routes still combine:
  - authorization
  - readiness
  - domain assembly
  - normalization
  - response shaping

Implementation:
1. Expand the existing presenter pattern beyond sector normalization.
2. Move operator/data diagnostics assembly into services.
3. Keep route functions small and predictable.

Likely module targets:
- `backend/api/routes/presenters.py`
- `backend/services/operator_status_service.py`
- `backend/services/data_diagnostics_service.py`
- `backend/services/risk_payload_service.py`
- `backend/services/exposures_payload_service.py`

Required doc updates:
- none major unless route semantics change

Frontend implications:
- re-check frontend assumptions if any payload fields are renamed, removed, or normalized centrally

### Workstream 7: Runtime State And Refresh Status Contract

Objective:
- reduce multi-store runtime drift and make status truth easier to reason about

Problems being addressed:
- `refresh_status`, `job_runs`, `runtime_state`, and route reconciliation logic all overlap

Implementation:
1. Define one canonical runtime status contract:
   - in-flight status
   - latest terminal lane result
   - operator health/runtime truth
2. Document which store owns which piece.
3. Reduce route-layer reconciliation logic where possible.
4. Make runtime-state error semantics explicit and consistent.

Required doc updates:
- operations playbook
- architecture known-limitations section

Frontend implications:
- update operator/health readouts if status semantics become simpler or more explicit

### Workstream 8: Frontend Truth Surface Simplification

Objective:
- make frontend observability coherent, useful, and quiet

Problems being addressed:
- some page-specific joins still exist
- some operator semantics still depend on page-local choices rather than explicit backend contracts

Implementation:
1. Keep one shared truth-summary helper for user-facing freshness banners.
2. Keep Health as the control-room page.
3. Keep Data as the maintenance page.
4. Audit each page for:
   - redundant freshness cards
   - duplicate refresh actions
   - page-local reconstruction of backend truth
5. Collapse operator-heavy details on user-facing pages where possible.

Required doc updates:
- architecture dashboard wiring section
- operations playbook operator UI policy

Frontend implications:
- any readout that no longer reflects canonical truth must be updated in the same PR

### Workstream 9: Documentation Consolidation

Objective:
- keep docs authoritative without creating planning sprawl

Problems being addressed:
- too many top-level plan documents can dilute authority

Implementation:
1. Keep these as canonical:
   - `ARCHITECTURE_AND_OPERATING_MODEL.md`
   - `OPERATIONS_PLAYBOOK.md`
   - one active implementation plan for the current hardening wave
2. Move older phase-specific plans to archival status once superseded.
3. Update `docs/README.md` to reflect that hierarchy explicitly.

Required doc updates:
- `docs/README.md`

Frontend implications:
- none directly

## Execution Order

Recommended order:

1. Workstream 1: canonical truth surface consolidation
2. Workstream 6: route-service-presenter separation
3. Workstream 2: orchestrator decomposition
4. Workstream 3: refresh pipeline decomposition
5. Workstream 4: model output persistence cleanup
6. Workstream 5: core read layer cleanup
7. Workstream 7: runtime-state and refresh-status contract cleanup
8. Workstream 8: frontend truth surface simplification
9. Workstream 9: documentation consolidation

This order is intentional:
- first reduce truth ambiguity
- then reduce oversized route/orchestrator/pipeline files
- then simplify storage and runtime contracts
- then do the final frontend/docs quieting pass

## Tactical Suggestions For Implementation

These tactical suggestions came out of the implementation-focused review pass and should guide execution:

1. Start with route-owned truth assembly, not storage.
   - Extract `operator_status_service` and `data_diagnostics_service` first.
   - Keep route semantics unchanged while moving construction logic out of the route files.

2. Preserve import seams during extraction.
   - If tests currently monkeypatch route-level collaborators, migrate them gradually instead of rewriting the whole test strategy at once.
   - Prefer importing the new service module from the route and moving collaborators into the service module, then update tests explicitly.

3. Extract by real responsibility, not by arbitrary file size.
   - Example: split profile/lane metadata from stage execution in the orchestrator.
   - Do not split files into tiny fragments that only proxy one function each.

4. Keep canonical truth contracts close to the payloads they describe.
   - operator/runtime truth contract
   - dashboard truth contract
   - data diagnostics truth contract

5. Add “ownership comments” only where ambiguity has historically caused regressions.
   - short comments are acceptable ahead of modules/functions that define canonical truth surfaces
   - avoid explanatory noise everywhere else

6. For frontend alignment, prefer contract-preserving backend changes first.
   - If a payload shape does not need to change, keep it stable.
   - Update frontend readouts only when the ownership or meaning of a field truly changes.

7. Use tests as seam indicators.
   - if a refactor forces broad unrelated test rewrites, the module boundary is probably wrong

## First Implementation Slice

The first concrete slice of this plan should be:

1. Extract `backend/services/operator_status_service.py`
2. Extract `backend/services/data_diagnostics_service.py`
3. Thin:
   - `backend/api/routes/operator.py`
   - `backend/api/routes/data.py`
4. Keep API payloads stable unless a documented ambiguity requires a contract correction
5. Update:
   - `docs/ARCHITECTURE_AND_OPERATING_MODEL.md`
   - `docs/OPERATIONS_PLAYBOOK.md`
   - any frontend readout whose wording still assumes route-local truth assembly
6. Run focused tests for operator/data routes plus any affected frontend typing

Status:
- Completed on 2026-03-16.
- `backend/services/operator_status_service.py` now owns operator-status payload assembly.
- `backend/services/data_diagnostics_service.py` now owns data-diagnostics payload assembly.
- `backend/api/routes/operator.py` and `backend/api/routes/data.py` are now thin route entrypoints.
- A temporary compatibility seam remains in `backend/api/routes/operator.py` for existing monkeypatch-heavy tests. Remove that route-level compatibility layer once tests are fully migrated to patch the service module directly.

## Review Requirements

Each workstream should close with:
- one architecture review
- one data-flow review
- one UI/docs review when frontend-visible behavior is involved

Each review should answer:
- did we reduce the number of truth surfaces or just rename them?
- did we reduce code paths that can accidentally read stale or wrong data?
- did docs and UI language stay aligned with actual behavior?
- did we make the project smaller/simpler in practice?

## Test Requirements

Every workstream should run at least the relevant focused slice:

- backend unit/contract tests for the touched surfaces
- route tests when API payloads or readiness behavior change
- `npm run typecheck` when frontend code changes
- `git diff --check`

For major storage/runtime changes also run:
- operator status tests
- refresh profile tests
- serving payload tests
- runtime-state tests

## Acceptance Criteria

The hardening program is succeeding when:

1. The largest modules are smaller and more role-specific.
2. Frontend pages pull fewer backend surfaces and reconstruct less truth on the client.
3. Operator/runtime truth has clearer ownership and less route-local reconciliation.
4. Neon vs SQLite fallback behavior is more explicit and harder to misuse.
5. Docs clearly show:
   - the operating model
   - the runbook
   - the one active hardening plan
6. The project feels easier to change without accidentally breaking unrelated data flow.

## Immediate Next Step

Start with Workstream 1 plus Workstream 6 together:
- extract operator-status and dashboard-truth assembly into dedicated services
- thin the route layer
- update docs and any frontend readouts that still depend on route-local assembly details

That is the highest-leverage way to reduce fragility before deeper module decomposition.
