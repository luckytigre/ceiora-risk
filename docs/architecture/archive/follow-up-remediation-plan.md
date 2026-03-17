# Post-Restructure Remediation Plan

Date: 2026-03-16
Status: Completed workstreams; selective deferred reviews remain
Owner: Codex

## Purpose

The original restructure program materially improved the repository shape and is complete for its initial scope.

This follow-up plan addresses the issues confirmed by the post-restructure audit:

- route-to-data leakage
- service-to-orchestrator coupling
- hidden runtime path mutation
- remaining large operational modules with mixed responsibilities
- documentation that is slightly cleaner than the actual code in a few areas

This is not a second broad rewrite.

It is a targeted hardening pass intended to:

- enforce the architecture rules that now exist on paper
- reduce hidden behavior and accidental coupling
- lower the chance of wrong data being pulled from the wrong place
- keep the codebase boring, explicit, and durable

## Evidence Base

This plan is based on:

- [audit-plan-vs-reality.md](./audit-plan-vs-reality.md)
- [audit-architecture.md](./audit-architecture.md)
- [audit-workflows.md](./audit-workflows.md)
- [audit-simplification.md](./audit-simplification.md)
- [audit-correctness.md](./audit-correctness.md)
- [audit-docs.md](./audit-docs.md)
- [audit-summary.md](./audit-summary.md)

## Independent Review Track A: Boundary And Ownership

This review treated the codebase as a package-boundary problem.

Main conclusions:

1. The route layer still violates the declared dependency rules in several places.
2. `operator_status_service.py` still depends on the orchestrator for a path constant, which is the wrong ownership direction.
3. `services/` still mixes route-facing application services with Neon-heavy infrastructure modules.
4. The current large-module risk is concentrated in a few operational files, not spread everywhere.
5. Further broad directory churn would create more noise than value unless it follows a concrete responsibility split.

Primary evidence:

- `backend/api/routes/exposures.py`
- `backend/api/routes/risk.py`
- `backend/api/routes/portfolio.py`
- `backend/api/routes/health.py`
- `backend/api/routes/universe.py`
- `backend/api/routes/readiness.py`
- `backend/services/operator_status_service.py`
- `backend/services/neon_mirror.py`
- `backend/services/neon_holdings.py`

## Independent Review Track B: Workflow And Runtime-Safety

This review treated the codebase as an execution-path and operational-safety problem.

Main conclusions:

1. The highest-risk remaining issue is not route cleanliness. It is hidden global path mutation in rebuild workflows.
2. `stage_runner.py` is clearer than the old orchestrator, but it still owns too many stage families in one place.
3. `refresh_manager.py` is acceptable as a process-local runtime controller, but it should not become the place where more policy accumulates.
4. Operator truth is centralized, which is good, but it still reconciles too many underlying stores and runtime signals.
5. The remaining large modules should only be decomposed where the split reduces execution ambiguity or operational drift.

Primary evidence:

- `backend/orchestration/runtime_support.py`
- `backend/orchestration/stage_runner.py`
- `backend/orchestration/stage_execution.py`
- `backend/services/refresh_manager.py`
- `backend/services/operator_status_service.py`
- `backend/analytics/health.py`

## Where The Two Reviews Agree

1. The remaining issues are real, not cosmetic.
2. The next work should be selective and high leverage.
3. Route boundary cleanup should happen before any more package renames.
4. The runtime path mutation issue is the most important hidden-coupling problem.
5. Large modules should be split only along obvious responsibility seams.
6. `services/` should not be reorganized just for visual tidiness.

## Where The Two Reviews Disagree

### 1. `services/` package cleanup urgency

- Boundary view: the mixed package weakens ownership clarity and should be addressed sooner.
- Workflow view: do not move files between packages until the runtime/path issues are fixed first.

Recommended decision:

Do not rename or move the Neon modules yet. First reduce coupling and split responsibilities inside the most problematic modules. Repackage only if the new ownership becomes obvious.

### 2. `stage_runner.py` split timing

- Boundary view: not urgent if the boundaries around it improve.
- Workflow view: worthwhile after path handling becomes explicit.

Recommended decision:

Do not split `stage_runner.py` before the runtime-context work. Otherwise the same implicit path behavior will just be copied into smaller files.

## Non-Goals

This follow-up plan does **not** include:

- a full Neon-native rebuild rewrite
- a new repository / manager / framework layer
- broad frontend state-management changes
- aggressive package renaming to force the exact target tree
- decomposition of every large file on size alone

## Workstreams

### Workstream 1: Finish Route Boundary Cleanup

Status: `completed`

Goal:

Routes should stop pulling from `backend.data` directly except for explicitly allowed presenter-only helpers.

Current problems:

- `backend/api/routes/exposures.py` still resolves factor history and runtime payloads directly.
- `backend/api/routes/risk.py` and `backend/api/routes/portfolio.py` still wire `load_runtime_payload` and `cache_get` directly.
- `backend/api/routes/health.py` still reads health diagnostics directly from data adapters.
- `backend/api/routes/universe.py` still owns universe payload reads and price-history query wiring.
- `backend/api/routes/readiness.py` still reads refresh status from SQLite cache directly.

Planned changes:

1. Add one small route-facing service for universe queries and payload reads.
2. Add one small route-facing service for factor-history lookup and factor-name resolution.
3. Move health-diagnostics payload loading behind a service surface.
4. Make `readiness.py` take refresh truth from an application service or runtime-status surface instead of reading SQLite directly.
5. Remove direct `backend.data.*` imports from the remaining route files where the service layer should own them.

Guardrails:

- Do not introduce a generic `runtime_payload_service.py` dumping ground.
- Prefer services aligned to real surfaces:
  - universe
  - factor history
  - health diagnostics
  - refresh/readiness

Validation:

- targeted route tests
- a small architecture test that rejects direct `backend.data` imports from route modules, with explicit allow-list if needed

Docs to update:

- [dependency-rules.md](./dependency-rules.md)
- [current-state.md](./current-state.md)
- [restructure-plan.md](./restructure-plan.md)

Completed in Batch 1:

- removed direct `backend.data` imports from the remaining route files that were still violating the dependency rules
- added concrete route-facing services:
  - `backend/services/factor_history_service.py`
  - `backend/services/health_diagnostics_service.py`
  - `backend/services/readiness_service.py`
  - `backend/services/universe_service.py`
- updated `dashboard_payload_service.py` so routes no longer carry data-adapter wiring for portfolio/risk/exposures
- kept route boundaries thin without introducing a generic payload-wrapper layer

Remaining note:

- `backend/api/routes/presenters.py` still imports a domain-side formatting helper, which is acceptable for now because it is a small presenter seam, not a data-adapter leak

### Workstream 2: Decouple Operator Truth From The Orchestrator

Status: `completed`

Goal:

`operator_status_service.py` should depend on job-run storage and runtime truth surfaces, not the job-engine module.

Current problems:

- `backend/services/operator_status_service.py` imports `DATA_DB` from `backend/orchestration/run_model_pipeline.py`.
- `backend/api/routes/operator.py` still carries compatibility re-export seams.

Planned changes:

1. Give `backend/data/job_runs.py` a clean default-path behavior or a narrow path resolver so callers do not need orchestrator constants.
2. Remove the `run_model_pipeline.DATA_DB` dependency from `operator_status_service.py`.
3. Retire the compatibility re-export block in `backend/api/routes/operator.py` after the tests are migrated.
4. Keep `refresh_manager` as the runtime-status source for process-local refresh activity, but keep that dependency explicit and narrow.

Guardrails:

- Do not move operator-status assembly back into routes.
- Do not make `job_runs.py` depend on orchestration.

Validation:

- operator-status route tests
- architecture import-direction test: forbid `backend.services.operator_status_service` from importing `backend.orchestration.run_model_pipeline`

Docs to update:

- [target-architecture.md](./target-architecture.md)
- [audit-plan-vs-reality.md](./audit-plan-vs-reality.md) only via follow-up note if needed; do not rewrite the audit
- [restructure-plan.md](./restructure-plan.md)

Completed in Batch 1:

- added `job_runs.default_db_path()` so job-run storage owns its default path
- removed the `backend/orchestration/run_model_pipeline.DATA_DB` import from `backend/services/operator_status_service.py`
- removed the compatibility re-export block from `backend/api/routes/operator.py`
- migrated the affected tests to target `backend.services.operator_status_service` directly

### Workstream 3: Remove Hidden Runtime Path Mutation

Status: `completed`

Goal:

Rebuild workflows should pass explicit runtime paths and execution context rather than mutating module globals mid-run.

Current problems:

- `backend/orchestration/runtime_support.py` mutates:
  - `config.DATA_DB_PATH`
  - `config.SQLITE_PATH`
  - `backend.analytics.pipeline.DATA_DB`
  - `backend.analytics.pipeline.CACHE_DB`
  - `backend.data.core_reads.DATA_DB`
- The active stage context is therefore partly implicit.

Implemented:

1. `backend.analytics.pipeline.run_refresh(...)` now accepts explicit `data_db` and `cache_db` inputs with safe defaults for ordinary runtime use.
2. `backend.data.sqlite` cache helpers now accept explicit `db_path` overrides instead of depending on `config.SQLITE_PATH` mutation.
3. `backend.data.core_reads` load surfaces now accept explicit `data_db` input so orchestration-driven rebuilds do not rely on a mutable module global.
4. `backend.orchestration.stage_runner` now passes workspace/local paths explicitly through serving-refresh and risk-model execution.
5. `temporary_runtime_paths()` has been deleted.

Why this matters:

This is the largest remaining hidden side effect in the architecture. It is the most important correctness and durability fix in this follow-up plan.

Guardrails:

- Keep the API/runtime call surface simple for non-orchestration callers by providing defaults.
- Do not create a broad dependency injection framework.
- Prefer an explicit `RuntimePaths` or similarly concrete object over multiple new indirection layers.

Validation:

- refresh-profile tests
- operating-model contract tests
- a new test proving explicit workspace paths do not mutate unrelated module globals

Docs to update:

- [target-architecture.md](./target-architecture.md)
- [dependency-rules.md](./dependency-rules.md)
- [restructure-plan.md](./restructure-plan.md)
- operating-model docs if operator-visible behavior changes

Completed in Batch 2:

- removed `temporary_runtime_paths()` and the associated cross-module path mutation
- made workspace/canonical SQLite targets explicit in `pipeline.run_refresh(...)`, `core_reads`, and `sqlite`
- updated orchestration stage execution to forward `data_db` / `cache_db` explicitly
- added targeted tests proving:
  - workspace paths are forwarded into `run_refresh()`
  - `core_reads.DATA_DB` is not mutated during stage execution
  - risk-model cache writes land in the workspace cache db directly

### Workstream 4: Split Stage Families Inside `stage_runner.py`

Status: `completed`

Goal:

Stage execution should stay orchestration-local, but its structure should reflect the real stage families.

Current problem:

- `backend/orchestration/stage_runner.py` is still a branch-heavy implementation hub for ingest, sync, readiness, core compute, and serving refresh.

Implemented:

1. Kept `run_stage(...)` as the orchestration-local dispatch surface.
2. Split stage implementation helpers into family modules:
  - `backend/orchestration/stage_source.py`
  - `backend/orchestration/stage_core.py`
  - `backend/orchestration/stage_serving.py`
3. Kept workflow metadata, stage loop control, and run bookkeeping outside those family modules.

Guardrails:

- Do not split stage code before runtime paths are explicit.
- Do not create many tiny stage files with one function each.

Validation:

- refresh-profile tests
- stage-window and resume tests
- targeted orchestration integration slice

Docs to update:

- [module-inventory.md](./module-inventory.md)
- [restructure-plan.md](./restructure-plan.md)

Completed in Batch 3:

- reduced `backend/orchestration/stage_runner.py` to a family dispatcher
- moved ingest/source-sync/neon-readiness logic into `stage_source.py`
- moved raw-history/feature-build/ESTU/factor-returns/risk-model logic into `stage_core.py`
- moved serving-refresh logic into `stage_serving.py`
- preserved `run_model_pipeline._run_stage` and `stage_runner.run_stage(...)` as the stable orchestration-local entry surface
- validated the split against refresh-profile, operating-model, cloud-runtime-role, and audit-fix tests

### Workstream 5: Selective Large-Module Decomposition

Status: `completed`

Goal:

Reduce risk concentration in a few remaining broad modules without reopening the whole repository.

Priority order:

1. `backend/services/data_diagnostics_service.py`
2. `backend/services/neon_holdings.py`
3. `backend/data/cross_section_snapshot.py`

Candidate split seams:

#### `data_diagnostics_service.py`

- SQLite inspection helpers
- exposure-source analysis
- payload assembly

#### `neon_holdings.py`

- schema/application DDL
- CSV parsing and row normalization
- identifier resolution
- mutation/persistence

#### `cross_section_snapshot.py`

- schema maintenance
- source loading
- snapshot assembly

Explicitly deferred unless a behavior change demands more:

- `backend/services/neon_mirror.py`
- `backend/analytics/health.py`
- `backend/risk_model/daily_factor_returns.py`

Reason for deferral:

These files are large, but they are also operationally dense. They should only be split against real responsibility seams, not as an automatic size response.

Validation:

- module-specific targeted tests
- compile/import checks

Docs to update:

- [module-inventory.md](./module-inventory.md)
- [current-state.md](./current-state.md)
- [restructure-plan.md](./restructure-plan.md)

Completed in Batch 4:

- split `backend/services/data_diagnostics_service.py` along the first obvious responsibility seams without changing the route-facing surface
- added `backend/services/data_diagnostics_sqlite.py` for SQLite inspection helpers
- added `backend/services/data_diagnostics_sections.py` for exposure-source, eligibility-summary, and factor cross-section section builders
- kept `build_data_diagnostics_payload(...)` as the stable route-facing entrypoint
- validated the split against diagnostics-route, architecture-boundary, and cloud-auth/runtime-role tests

Completed in Batch 5:

- split `backend/services/neon_holdings.py` along two stable helper seams without changing the public workflow surface
- added `backend/services/neon_holdings_identifiers.py` for account/ticker/RIC normalization and ticker-to-RIC resolution
- added `backend/services/neon_holdings_store.py` for schema application, account/batch persistence, position mutation primitives, and holdings listing queries
- kept `neon_holdings.py` as the workflow surface for CSV/payload parsing, holdings import application, what-if apply, and single-position edit/remove flows
- preserved the existing module-level monkeypatch seams used by holdings and what-if tests
- validated the split against holdings-service, holdings-route, and portfolio-whatif tests

Completed in Batch 6:

- split `backend/data/cross_section_snapshot.py` along the planned schema/build seams without changing the public rebuild surface
- added `backend/data/cross_section_snapshot_schema.py` for table existence, PK, migration, and schema maintenance helpers
- added `backend/data/cross_section_snapshot_build.py` for source-event loading, as-of merge logic, and payload assembly
- kept `rebuild_cross_section_snapshot(...)` in `cross_section_snapshot.py` as the stable facade used by refresh/orchestration flows
- preserved the existing compatibility exports like `_table_exists`, `_table_columns`, and `_pk_cols` at the facade layer
- validated the split against audit-fix and operating-model contract tests

Remaining in this workstream:

- reassess whether `neon_holdings.py` needs further workflow extraction only after the snapshot module is reviewed
- reassess whether `data_diagnostics_service.py` needs any further split only after those higher-priority candidates are reviewed

### Workstream 6: Add Lightweight Architecture Guard Tests

Status: `completed`

Goal:

Prevent the same structural regressions from creeping back in quietly.

Planned checks:

1. Route modules should not import `backend.data` directly except from a short reviewed allow-list.
2. `operator_status_service.py` should not import `backend.orchestration.run_model_pipeline`.
3. New modules should not revive catch-all patterns like `shared.py`, `common.py`, or vague `manager.py`.

Guardrails:

- Keep the tests simple and local.
- Use static import inspection or AST parsing, not a heavy new lint framework.

Validation:

- the architecture guard test suite itself

Docs to update:

- [dependency-rules.md](./dependency-rules.md)
- [restructure-plan.md](./restructure-plan.md)

Started in Batch 1:

- added `backend/tests/test_architecture_boundaries.py`
- currently enforced:
  - route modules may not import `backend.data` directly
  - `operator_status_service.py` may not import `backend.orchestration.run_model_pipeline`

Completed in Batch 7:

- added a lightweight naming guard to `backend/tests/test_architecture_boundaries.py`
- the guard now rejects new `shared.py`, `common.py`, and vague `*manager.py` files under `backend/`
- kept `backend/services/refresh_manager.py` as the one explicit allow-listed exception because it owns concrete process-local refresh lifecycle control

## Recommended Sequence

1. Workstream 1: route boundary cleanup
2. Workstream 2: operator/orchestrator decoupling
3. Workstream 6: architecture guard tests
4. Workstream 3: runtime-path explicit context
5. Workstream 4: `stage_runner.py` family split
6. Workstream 5: selective large-module decomposition

Reason:

- The first two reduce immediately visible boundary violations.
- The guard tests keep those wins from regressing.
- The runtime-path work is the biggest safety fix, but it is easier to do once the surrounding entrypoint/service boundaries are cleaner.
- The stage split should follow the explicit runtime-context work.
- Large-module cleanup should be last and selective.

## Risks

1. The runtime-path work touches sensitive rebuild behavior and needs tighter tests than the earlier route/service refactors.
2. Over-correcting the route layer could create too many tiny services unless the surfaces stay concrete.
3. The mixed `services/` package may remain visually imperfect for a while; that is acceptable if ownership is clearer.
4. Existing tests still use monkeypatch-heavy seams in some areas, so test migration may be required before some cleanup can land safely.

## Open Questions

1. Should `health.py` eventually become a diagnostics package, or is it better left intact until there is a behavior-driven reason to split it?
2. Is `refresh_manager.py` the right long-term home for process-local refresh state, or should that be isolated later into a smaller runtime-control module?
3. When `services/neon_*` modules are decomposed, should they remain in `services/` or move into a more explicit infrastructure package?

## Completion Criteria

This follow-up plan is complete when:

1. Remaining route/data dependency-rule violations are removed or explicitly documented as justified exceptions.
2. `operator_status_service.py` no longer depends on the orchestrator module.
3. Hidden global runtime path mutation is removed and replaced by explicit runtime path/context passing.
4. `stage_runner.py` clearly separates stage families.
5. At least the first tier of large mixed-responsibility modules is decomposed or intentionally deferred with documented reasons.
6. Lightweight architecture guard tests protect the new boundaries.

## Next Recommended Step

Batch 1 is complete.
Batch 2 is complete.
Batch 3 is complete.

Next:

- continue with Workstream 5: selective large-module decomposition
- start with `backend/services/data_diagnostics_service.py`

Those are now the highest-leverage remaining fixes because the orchestration shape is materially cleaner and the next remaining risk is concentrated in a small number of broad operational modules.
