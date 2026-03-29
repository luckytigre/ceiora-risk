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
   Files like `core_reads.py`, `model_outputs.py`, `cross_section_snapshot.py`, `serving_outputs.py`, and `runtime_state.py` should not reaccrete raw helper logic that was intentionally moved behind them.

6. New junk-drawer modules are forbidden.
   Do not add `shared.py`, `common.py`, or vague `*manager.py` modules unless there is an explicit reviewed lifecycle responsibility.

7. Dependency direction is one-way.
   `data` must not import `api` or frontend code.
   `services` must not import API layers.
   `services` should not import full workflow modules just to inspect static metadata.

8. Serving paths must not advance the stable core package.
   `serve-refresh` and other serving-only paths may project against the current core package, but they must not compute or persist factor returns, covariance, specific risk, or advance `core_state_through_date`.

8a. Workspace paths do not retarget authority by themselves.
   Passing workspace `data_db` / `cache_db` paths into a serving lane does not automatically make `core_reads` local; workspace paths alone must not override the serving lane's existing backend-selection decision.

9. Serving-time prices are read-only.
   Serving/orchestration/API layers must not write serving-time or ad hoc prices into canonical model-estimation history tables such as `security_prices_eod`.

10. Canonical contract names win over compatibility aliases.
   UI, docs, and new code should prefer explicit fields such as `core_state_through_date`, `core_rebuild_date`, `exposures_served_asof`, `exposures_latest_available_asof`, `model_status_reason`, `factor_coverage_asof`, `served_loadings_asof`, and `latest_loadings_available_asof`.
   Legacy aliases may remain only for compatibility and fallback decoding.

11. Compatibility-named universe helpers must not retake authority.
   `security_master` and `security_master_sync.py` may remain for compatibility, diagnostics, and demotion rollout only.
   Runtime/bootstrap/seed/LSEG update flows must treat `security_registry`, `security_policy_current`, `security_taxonomy_current`, and `security_source_observation_daily` as authoritative and use `security_master_compat_current` only as the compatibility projection.

12. Projection-only outputs are core-bound derived artifacts.
   Projection-only instruments must stay outside native cUSE estimation.
   Their projected loadings must read durable core outputs, refresh only on the core-package cadence, persist once per active core package, expose `projection_asof = core_state_through_date`, and be read by serving rather than recomputed opportunistically.
   Missing projected outputs for the active core package must surface explicit degraded/unavailable state instead of silent omission.

13. cPAR stays parallel to cUSE4 and keeps its own owned surfaces.
   Pure cPAR logic belongs in `backend/cpar/*`.
   cPAR integration still belongs in the normal repo layers.
   Current cPAR slices must not reuse cUSE4 serving-payload or runtime-state surfaces by implication.

14. Cloud serve surfaces stay stateless and do not own refresh execution.
   `backend/services/refresh_control_service.py` is the reviewed application-facing control surface for refresh routes.
   `backend/services/refresh_manager.py` remains the reviewed process-local execution owner for local thread-based lifecycle compatibility.
   Serve-facing readers must use the persisted refresh-status surface and must not reconcile worker ownership as though they own the control process.

## Existing Guardrails

The repository already enforces several of these with lightweight tests in [test_architecture_boundaries.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/tests/test_architecture_boundaries.py):
- routes may not import `backend.data` directly
- `cuse4_operator_status_service.py` and the legacy `operator_status_service.py` shim may not import `backend.orchestration.run_model_pipeline`
- new `shared.py`, `common.py`, and vague `*manager.py` files are rejected under `backend/`

## What These Guardrails Prevent

- route-to-data leakage returning through later edits
- operator-service/orchestrator coupling regressing
- visual structure drift through vague catch-all modules
- new hidden path-retargeting helpers creeping in through convenience edits
- serving-only refreshes silently advancing the stable core package
- serving-time price logic contaminating canonical model-estimation history
- user-facing and developer-facing semantics drifting back to vague compatibility fields
- serve-only processes mutating shared refresh state as though they owned the control-plane worker

## Low-Overhead Maintenance Rule

When adding a new module or feature:
- place it near the surface it primarily serves
- prefer extending an existing coherent owner over creating a vague new module
- if a new exception to the invariants is truly necessary, document it here and in `dependency-rules.md`
