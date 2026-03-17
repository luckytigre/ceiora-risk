# Neon Main Platform Plan

Date: 2026-03-16
Owner: Codex
Status: Canonical focused execution plan

Current simplification pass:

- `docs/NEON_LEAN_CONSOLIDATION_PLAN.md` is the governing file for the current lean-consolidation work so the Neon migration does not keep accumulating duplicated storage-policy code.

## Purpose

This file is the focused execution plan for the actual platform goal:

- Neon is the main durable database for the app
- the app is portable across machines and cloud workers
- the app can serve and rebuild without depending on one local SQLite runtime
- local SQLite remains only:
  - the direct LSEG landing zone
  - the optional deep archive
  - explicitly temporary scratch where still justified

This plan is narrower than the broader migration plan. It is not just about adding missing Neon tables. It is about removing architectural ambiguity so Neon becomes the real operating authority.

## Checkpoint Log

### 2026-03-16: Active Execution Wave Finalized

Execution reference for the current wave:

- fix required-Neon fail-closed behavior in `backend/data/model_outputs.py`
- move `backend/data/serving_outputs.py` to Neon-first durable commit order
- introduce a Neon-backed runtime-state surface for operator and health truth
- move `risk_engine_meta`, `neon_sync_health`, and active snapshot pointer reads/writes onto that runtime-state surface with SQLite fallback only as transitional mirror
- update docs that still describe model outputs or serving payloads as SQLite-first

This execution wave is allowed to leave one major limitation open:

- core and cold-core still rebuild through a scratch SQLite workspace materialized from Neon

This execution wave is not allowed to leave these issues open:

- required Neon model-output failure still writing SQLite first
- serving payload persistence still committing SQLite before Neon
- operator and health runtime truth relying only on local SQLite when Neon is available
- stale docs still claiming model outputs are SQLite-first after the cutover

Planned code targets:

- `backend/config.py`
- `backend/data/model_outputs.py`
- `backend/data/serving_outputs.py`
- `backend/data/runtime_state.py`
- `backend/analytics/pipeline.py`
- `backend/orchestration/run_model_pipeline.py`
- `backend/api/routes/operator.py`
- `backend/main.py`
- targeted backend tests and canonical docs

Required validation for this wave:

- targeted pytest for model outputs, serving outputs, operator status, health route, and new runtime-state coverage
- compile check for touched backend modules
- one zoom-out grep pass for stale SQLite-first or outdated Neon-authority wording

Two-reviewer plan checkpoint:

- Reviewer A, architecture/runtime: approved the execution wave with one requirement that runtime-state scope stay narrow and focus first on operator/health truth keys instead of trying to replace every SQLite cache access in one cut.
- Reviewer B, data/test/docs: approved the execution wave with one requirement that serving payload cutover preserve optional SQLite mirror semantics for local diagnostics and that docs explicitly keep the scratch-workspace rebuild caveat visible.

Execution status:

- completed: required-Neon model-output failures now raise before any SQLite mirror write
- completed: durable serving payloads now write to Neon first and only mirror to SQLite after Neon succeeds or is optional
- completed: `risk_engine_meta`, `neon_sync_health`, and active snapshot pointer now have a Neon-backed `runtime_state_current` surface with local-ingest fallback to SQLite
- completed: health and operator routes now read those runtime-truth keys from the Neon-backed runtime-state surface first
- completed: docs now describe model outputs and serving payloads as Neon-first and keep the scratch-rebuild caveat explicit
- still open: rebuild execution remains Neon-backed scratch SQLite rather than direct Neon-native rebuild execution

Validation completed:

- compile check passed for touched backend modules
- targeted pytest passed for:
  - `backend/tests/test_model_outputs_neon_primary.py`
  - `backend/tests/test_serving_outputs.py`
  - `backend/tests/test_runtime_state.py`
  - `backend/tests/test_operator_status_route.py`
  - `backend/tests/test_health_neon_sync_signal.py`
- broader contract pytest passed for:
  - `backend/tests/test_operating_model_contract.py`
  - `backend/tests/test_refresh_profiles.py`
  - `backend/tests/test_cache_publisher_service.py`
  - `backend/tests/test_cloud_auth_and_runtime_roles.py`

Two-reviewer implementation checkpoint:

- Reviewer A, architecture/runtime: approved the implementation with the note that runtime-state migration is intentionally narrow and still does not eliminate the SQLite scratch rebuild dependency.
- Reviewer B, data/test/docs: approved the implementation with the note that cross-store publish is still non-atomic and should remain listed as an open limitation until rebuild/runtime cutover is deeper.

### 2026-03-16: Workstream 2 First Authority Cutover Implemented

Completed in code:

- `backend/data/model_outputs.py` now computes factor-return incremental reload windows from Neon when Neon model-output writes are enabled
- durable model-output persistence now writes to Neon first when Neon is configured
- local SQLite model-output persistence now acts as a secondary mirror instead of the only durable target
- required Neon model-output writes now fail closed instead of silently leaving SQLite as hidden authority
- non-required Neon failures now report fallback authority honestly instead of still claiming Neon authority

Validation completed:

- compile check passed for `backend/data/model_outputs.py`
- targeted pytest slice passed for:
  - `backend/tests/test_model_outputs_neon_primary.py`
  - `backend/tests/test_model_outputs_local_regression.py`

Still pending:

- runtime state is still largely SQLite-backed
- normal rebuild lanes still depend on SQLite scratch/workspace paths
- operator and docs still describe a transitional model because those remaining dependencies are real

## Governing Rule

Neon must be treated as the main platform only when all of the following are true:

- durable writes land in Neon first
- a fresh worker can recover the active runtime from Neon alone
- ordinary serving reads and ordinary rebuild reads can run from Neon alone
- operator truth surfaces describe Neon as authority because it actually is authority
- local SQLite removal after publish would not break ordinary operation

If any one of those is false, Neon is still a partial mirror, not the main platform.

## Desired End State

When this plan is complete:

- Neon is the durable authority for:
  - source operating window
  - holdings and serving payloads
  - model outputs
  - runtime state needed for refresh and operator truth
  - rebuild inputs for normal lanes
- local SQLite is no longer required for:
  - ordinary dashboard serving
  - ordinary serving refresh
  - ordinary `core-weekly`
  - ordinary `cold-core`
- local SQLite is still allowed for:
  - direct LSEG ingest
  - deep archive beyond Neon retention
  - explicit offline tooling
  - clearly bounded temporary scratch while storage-sensitive numerics are still being ported

## Non-Negotiable Constraints

1. Do not leave split authority in place after each phase.
2. Do not allow docs or UI copy to overstate Neon authority ahead of implementation.
3. Do not leave hidden SQLite dependencies in ordinary runtime paths.
4. Do not make Neon optional for surfaces that claim to be Neon-authoritative.
5. Do not close the program until a fresh worker audit proves standalone operation.

## Review Model

Every major checkpoint must include independent reviews across five tracks:

### Architecture Reviewer

Checks:

- authority boundaries
- whether business logic still owns storage branching
- whether local SQLite still acts as hidden authority

Required output:

- findings only
- file references
- approval or rejection

### Data Reviewer

Checks:

- schema correctness
- retention correctness
- migration safety
- replay and backfill behavior
- parity blind spots

Required output:

- findings only
- data-loss or stale-data risks

### Runtime Reviewer

Checks:

- cold-start portability
- refresh behavior
- rebuild behavior
- failure handling
- degraded-mode correctness

Required output:

- findings only
- portability blockers

### Frontend and Operator Reviewer

Checks:

- frontend copy about source truth, snapshots, and Neon authority
- operator status wording
- any user-visible claim that contradicts actual storage behavior

Required output:

- findings only
- claim-versus-implementation mismatches

### Test and Docs Reviewer

Checks:

- missing tests
- stale architecture docs
- stale playbooks
- stale generated examples or audit artifacts used as references

Required output:

- findings only
- missing evidence and stale references

## Program Structure

This plan should be executed in six workstreams that close together, not as isolated code changes.

## Workstream 1: Full Authority Audit

### Goal

Produce one exact list of everything that still prevents Neon from being the main platform.

### Concrete Tasks

1. Inventory all SQLite reads and writes across:
   - `backend/data/`
   - `backend/services/`
   - `backend/orchestration/`
   - `backend/analytics/`
   - `backend/risk_model/`
   - frontend code that describes storage authority
   - docs and playbooks that describe storage authority
2. Classify each dependency as:
   - `ingest_only`
   - `deep_archive_only`
   - `runtime_state`
   - `durable_model_write`
   - `rebuild_input`
   - `scratch_only`
   - `stale_claim`
3. Build one explicit “Neon intention drift” checklist.

### Exit Criteria

- every remaining SQLite dependency is classified
- every mismatched claim in frontend/docs/operator wording is listed

## Workstream 2: Neon-First Durable Writes

### Goal

Move all durable operational writes to Neon first so Neon becomes the true persistent home of the app.

### Scope

- model outputs
- runtime state needed for operation
- serving payloads and serving metadata
- any run metadata needed to reconstruct state

### Concrete Tasks

1. Convert durable model-output persistence to Neon-first:
   - `backend/data/model_outputs.py`
2. Introduce explicit store-backed interfaces where useful:
   - `ModelStore`
   - `RuntimeStateStore`
3. Keep SQLite mirror writes only as secondary transitional behavior.
4. Make successful run completion depend on successful Neon durable writes.
5. Fail closed if Neon durable persistence fails.

### Suggested Refactor

- `backend/data/model_store.py`
- `backend/data/runtime_state_store.py`
- `backend/data/stores/model_store_neon.py`
- `backend/data/stores/model_store_sqlite.py`
- `backend/data/stores/runtime_state_neon.py`
- `backend/data/stores/runtime_state_sqlite.py`

### Exit Criteria

- Neon is the first durable write target for ordinary operation
- SQLite is no longer the write authority for model outputs or runtime state

## Workstream 3: Neon-Authoritative Runtime Recovery

### Goal

Allow a fresh process on a different machine to recover all ordinary runtime state from Neon.

### Concrete Tasks

1. Inventory everything currently read via `backend/data/sqlite.py`.
2. Move operationally required state into Neon-backed storage.
3. Keep only explicitly local or ephemeral cache state in SQLite.
4. Update serving refresh and operator routes so they rely on Neon-backed truth surfaces for normal recovery.
5. Remove silent fallbacks that fabricate success from missing local cache.

### Exit Criteria

- a fresh worker with Neon access can recover serving/runtime state
- missing local SQLite files do not break ordinary runtime after publish

## Workstream 4: Neon-Authoritative Rebuild Inputs

### Goal

Make normal rebuild lanes read from Neon as their real authority.

### Concrete Tasks

1. Replace SQLite-first rebuild assumptions in orchestration paths.
2. Narrow or remove scratch workspace materialization for ordinary lanes.
3. Port storage-sensitive stages so they consume Neon-backed stores rather than local durable tables.
4. Keep scratch only when explicitly needed for compute, not for authority.
5. Make operator-facing readiness checks verify Neon sufficiency for rebuilds.

### Exit Criteria

- normal rebuild lanes do not require local SQLite authority
- any remaining scratch path is clearly marked non-authoritative

## Workstream 5: Repo-Wide Intent Alignment

### Goal

Ensure code, frontend, operator surfaces, and docs all describe the same storage contract.

### Concrete Tasks

1. Review operator wording in:
   - `backend/api/routes/operator.py`
2. Review frontend wording in:
   - `frontend/src/app/exposures/page.tsx`
   - `frontend/src/app/positions/page.tsx`
   - any other storage-truth-facing UI
3. Review canonical docs:
   - `docs/ARCHITECTURE_AND_OPERATING_MODEL.md`
   - `docs/OPERATIONS_PLAYBOOK.md`
   - `docs/NEON_AUTHORITATIVE_REBUILD_PLAN.md`
   - `docs/NEON_STANDALONE_EXECUTION_PLAN.md`
4. Flag or remove stale checked-in generated artifacts that misrepresent current Neon coverage.
5. Standardize these phrases across the repo:
   - “Neon is the main durable operating database”
   - “local SQLite is the ingest/archive reservoir”
   - “scratch is not authority”

### Exit Criteria

- no user-facing or operator-facing surface materially contradicts implementation
- no canonical doc still describes Neon as optional mirror-only infrastructure

## Workstream 6: Final Standalone Proof

### Goal

Prove the app is portable and no longer bound to one machine except for LSEG ingest.

### Required Proofs

1. Fresh-worker serve proof:
   - no local SQLite bootstrap required
   - dashboard serves from Neon-backed truth
2. Fresh-worker rebuild proof:
   - normal rebuild lanes run from Neon-backed inputs
3. Failure proof:
   - Neon write failure fails closed
   - parity failure is visible
   - missing local SQLite does not silently redirect authority
4. Operator proof:
   - operator status and docs correctly describe final authority

### Exit Criteria

- the app can be moved to another machine and operate from Neon after publish
- the only machine-bound function left is direct LSEG ingest and optional archive maintenance

## Phase Order

Execute in this order:

1. Workstream 1 first
2. Workstreams 2 and 3 together
3. Workstream 4 next
4. Workstream 5 as part of every PR, then a full pass near the end
5. Workstream 6 only after all prior work is substantively complete

## Checkpoint Plan

### Checkpoint A: Authority Map Complete

Required:

- full SQLite dependency map
- full intent-drift list

### Checkpoint B: Durable Writes Neon-First

Required:

- Neon-first model writes
- Neon-first runtime state writes where operationally required
- tests proving fail-closed writes

### Checkpoint C: Portable Runtime

Required:

- fresh-worker serving recovery from Neon
- operator truth aligned with actual runtime

### Checkpoint D: Portable Rebuild

Required:

- ordinary rebuild lanes read from Neon
- scratch no longer acts as authority

### Checkpoint E: Repo Alignment

Required:

- frontend wording audited
- docs audited
- stale artifacts reviewed

### Checkpoint F: Final Multi-Review and Fix Loop

Required:

- all five reviewers run independently
- every accepted issue fixed
- final zoom-out performed

## Final Zoom-Out Audit

Before declaring success, explicitly answer these questions:

1. What breaks if the local runtime SQLite files vanish after a successful publish?
2. What breaks if the app starts on a fresh worker with only Neon configured?
3. Which codepaths still open SQLite during ordinary serving?
4. Which codepaths still open SQLite during ordinary rebuilds?
5. Which docs or UI surfaces still speak as if SQLite is primary?
6. Which generated artifacts could confuse future audits?

The program is not done until those answers are short, explicit, and consistent with the target architecture.

## Acceptance Standard

This plan is complete only when the following statement is true without caveat:

“After local LSEG ingest publishes forward, Neon is the main durable platform for the app. Serving, holdings, runtime recovery, and ordinary rebuilds can run from Neon without dependence on one specific local SQLite machine.”
