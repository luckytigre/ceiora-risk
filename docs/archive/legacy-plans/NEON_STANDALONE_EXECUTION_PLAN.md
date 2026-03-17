# Neon Standalone Execution Plan

Date: 2026-03-16
Owner: Codex
Status: In progress

## Purpose

This file converts the higher-level migration intent into a concrete execution program.

It is the canonical file to reference while implementing the Neon-first standalone architecture:

- Neon becomes the durable operating database for runtime and rebuilds
- local SQLite remains the LSEG ingest landing zone and optional deep archive
- ordinary rebuild/runtime work no longer depends on local SQLite authority

This plan is intentionally execution-oriented:

- each phase maps to concrete code modules
- each phase has explicit review gates
- each phase has exit criteria
- each phase has a zoom-out audit so hidden SQLite dependencies are not missed

Companion plan:

- `docs/NEON_MAIN_PLATFORM_PLAN.md` is the stricter Neon-authority contract for making the app portable and Neon-first across backend, frontend, runtime state, and operator truth.

## Checkpoint Log

### 2026-03-16: Phase 1 Foundation Implemented

Completed in code:

- Neon canonical schema now defines:
  - `model_factor_covariance_daily`
  - `model_specific_risk_daily`
  - `model_run_metadata`
- Neon canonical schema now carries a forward-compatible `run_id` column on `model_factor_returns_daily`
- broad SQLite-to-Neon sync coverage now includes the durable model tables above
- Neon prune coverage now includes `model_run_metadata`
- Neon rebuild readiness now fails closed when the durable model tables are missing
- bounded Neon parity audit now covers covariance, specific risk, and run metadata

Validation completed:

- targeted compile check passed for touched modules
- targeted pytest slice passed for:
  - `backend/tests/test_neon_authority.py`
  - `backend/tests/test_neon_parity_value_checks.py`
  - `backend/tests/test_neon_stage2_model_tables.py`

Still pending before the broader migration checkpoint can be closed:

- model outputs now write Neon-first, but the broader runtime-state and rebuild cutovers are still incomplete
- serving payloads now write Neon-first, but publish still spans Neon plus local SQLite mirror stores
- Neon-authoritative rebuild execution still depends on SQLite scratch/workspace paths

## Non-Negotiable End State

When this plan is complete:

- Neon is the durable authority for source-of-truth operating data after publish/sync
- Neon is the durable authority for model outputs and runtime state
- `serve-refresh`, `core-weekly`, and `cold-core` can run on a Neon-connected worker without depending on a preexisting local SQLite warehouse
- local SQLite is used only for:
  - LSEG ingest landing
  - optional deep archive
  - explicitly temporary scratch, and only where still documented

## Execution Protocol

The implementation must follow these rules:

1. No phase closes without an explicit review checkpoint.
2. Every major storage change lands as dual-write or dual-read before cutover, unless there is a strong reason not to.
3. Every cutover removes or narrows a fallback; fallbacks are not allowed to accumulate indefinitely.
4. Every phase updates docs in the same PR series.
5. Every checkpoint includes an explicit zoom-out audit for hidden SQLite authority or stale assumptions.
6. Final acceptance requires a fresh-process, Neon-only runtime/rebuild audit.

## Independent Review Model

At each marked checkpoint, run independent reviews across these tracks:

### Architecture Reviewer

Scope:

- authority boundaries
- module ownership
- accidental backend branching in business logic
- whether SQLite is still leaking into ordinary runtime/rebuild work

Required output:

- findings only, ordered by severity
- file references
- exact contract violations
- explicit approval or rejection for the checkpoint

### Data Reviewer

Scope:

- schema correctness
- migration safety
- parity coverage
- retention semantics
- backfill and replay behavior

Required output:

- findings only
- schema or migration risks
- parity blind spots
- data-loss or stale-data risk assessment

### Runtime Reviewer

Scope:

- cold start
- refresh orchestration
- cloud-serve behavior
- failure handling
- stale local-state dependence

Required output:

- findings only
- operator-impacting failure modes
- broken standalone assumptions

### API and Contract Reviewer

Scope:

- route semantics
- payload contracts
- operator truth surfaces
- fallback behavior
- user-visible regressions

Required output:

- findings only
- contract drift
- route fallback inconsistencies

### Test and Docs Reviewer

Scope:

- missing tests
- stale docs
- misleading migration wording
- runbook gaps

Required output:

- findings only
- missing test coverage
- docs that contradict implemented behavior

## Global Completion Checklist

- [ ] Neon schema fully covers durable source, model, runtime, and serving tables
- [ ] Model outputs write to Neon first
- [ ] Runtime state is recoverable from Neon
- [ ] Factor-return and risk-model stages do not require SQLite durable authority
- [ ] Scratch SQLite workspace is removed from ordinary Neon-authoritative rebuilds
- [ ] Local SQLite usage is limited to ingest/archive or explicit scratch only
- [ ] Operator/status surfaces accurately describe the final architecture
- [ ] Final multi-review passes with all critical and high findings fixed

## Phase 0: Baseline Audit and Tracker Setup

### Goal

Create one authoritative map of all SQLite dependencies before modifying execution paths.

### Deliverables

- SQLite dependency inventory
- authority classification table
- migration tracker section in docs
- review template prompts stored in this file

### Concrete Tasks

1. Inventory all `sqlite3.connect(...)` usage under:
   - `backend/analytics/`
   - `backend/data/`
   - `backend/orchestration/`
   - `backend/risk_model/`
   - `backend/services/`
   - `backend/universe/`
2. Classify each SQLite touchpoint as:
   - `durable_authority`
   - `runtime_state`
   - `scratch_only`
   - `local_ingest_only`
   - `legacy_accidental_dependency`
3. Add a storage authority matrix to:
   - `docs/ARCHITECTURE_AND_OPERATING_MODEL.md`
4. Add a tracker note to:
   - `docs/NEON_AUTHORITATIVE_REBUILD_PLAN.md`

### Primary Files

- `backend/config.py`
- `backend/data/sqlite.py`
- `backend/data/core_reads.py`
- `backend/data/model_outputs.py`
- `backend/data/serving_outputs.py`
- `backend/orchestration/run_model_pipeline.py`
- `backend/risk_model/daily_factor_returns.py`
- `docs/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/NEON_AUTHORITATIVE_REBUILD_PLAN.md`

### Checkpoint Review

- Architecture reviewer
- Runtime reviewer
- Test and docs reviewer

### Exit Criteria

- no ambiguous storage roles remain undocumented
- every remaining SQLite dependency is classified

## Phase 1: Complete Neon Durable Schema

### Goal

Make Neon structurally capable of holding the full durable model and runtime contract.

### Deliverables

- Neon DDL for:
  - `model_factor_covariance_daily`
  - `model_specific_risk_daily`
  - `model_run_metadata`
- schema alignment decision and implementation for `model_factor_returns_daily`
- index and retention coverage for all durable model tables

### Concrete Tasks

1. Extend `docs/migrations/neon/NEON_CANONICAL_SCHEMA.sql` with the three missing durable model tables.
2. Decide and implement whether Neon `model_factor_returns_daily` should include `run_id`.
3. Ensure schema ownership is not split between ad hoc DDL fragments and migration SQL.
4. Extend schema-ensure logic in:
   - `backend/services/neon_mirror.py`
5. Extend Neon readiness validation in:
   - `backend/services/neon_authority.py`
6. Extend prune coverage for all durable model tables.

### Suggested Refactor

If the schema logic is getting crowded, split it into:

- `backend/data/stores/schema_source.py`
- `backend/data/stores/schema_model.py`
- `backend/data/stores/schema_runtime.py`

### Primary Files

- `docs/migrations/neon/NEON_CANONICAL_SCHEMA.sql`
- `backend/services/neon_mirror.py`
- `backend/services/neon_authority.py`
- optional new schema helper modules

### Checkpoint Review

- Data reviewer
- Architecture reviewer
- Test and docs reviewer

### Exit Criteria

- Neon has the full durable model schema
- readiness checks fail closed if the schema is incomplete

## Phase 2: Neon-First Durable Model Output Persistence

### Goal

Move durable model outputs from SQLite-first persistence to Neon-first persistence.

### Deliverables

- Neon-native model output writer
- optional SQLite mirror writer
- run success gated on Neon write success

### Concrete Tasks

1. Extract persistence logic from `backend/data/model_outputs.py` behind a store abstraction.
2. Create:
   - `backend/data/model_store.py`
   - `backend/data/stores/model_store_neon.py`
   - `backend/data/stores/model_store_sqlite.py`
3. Port upsert/delete behavior for:
   - factor returns
   - covariance
   - specific risk
   - run metadata
4. Keep SQLite writes only as secondary mirrors during transition.
5. Update orchestrator and pipeline callers so Neon is the primary durable target.
6. Add parity and migration tests for all four model tables.

### Optimization and Cleanup

- consolidate duplicate schema assumptions
- remove SQLite-only DDL from ordinary execution paths where Neon is primary
- keep rolling-window replacement logic explicit and centralized

### Primary Files

- `backend/data/model_outputs.py`
- `backend/analytics/pipeline.py`
- `backend/orchestration/run_model_pipeline.py`
- new model store modules
- tests for model-output persistence

### Checkpoint Review

- Architecture reviewer
- Data reviewer
- Runtime reviewer

### Exit Criteria

- successful core runs write durable model outputs to Neon first
- rebuild success is not declared if Neon model-output persistence fails

## Phase 3: Move Runtime State Off Local SQLite Authority

### Goal

Make runtime state recoverable from Neon so a fresh worker can operate without local cache dependence.

### Deliverables

- Neon-backed runtime state store
- snapshot or active-state contract preserved in Neon
- local cache narrowed to optional mirror or scratch

### Concrete Tasks

1. Inventory and migrate runtime-state keys currently written through `backend/data/sqlite.py`, including:
   - `risk_engine_cov`
   - `risk_engine_specific_risk`
   - `risk_engine_meta`
   - `neon_sync_health`
   - snapshot pointers and publish metadata
2. Create:
   - `backend/data/runtime_state_store.py`
   - `backend/data/stores/runtime_state_neon.py`
   - `backend/data/stores/runtime_state_sqlite.py`
3. Update:
   - `backend/analytics/services/cache_publisher.py`
   - `backend/analytics/refresh_policy.py`
   - `backend/analytics/pipeline.py`
   - operator/runtime status surfaces
4. Preserve serving payload durability in `backend/data/serving_outputs.py`, but normalize metadata/state around Neon.

### Optimization and Cleanup

- remove repeated direct `cache_get/cache_set` usage from business logic
- centralize runtime-state reads and writes
- reduce split-brain between serving payload durability and runtime cache durability

### Primary Files

- `backend/data/sqlite.py`
- `backend/data/serving_outputs.py`
- `backend/analytics/services/cache_publisher.py`
- `backend/analytics/refresh_policy.py`
- `backend/analytics/pipeline.py`
- `backend/api/routes/operator.py`

### Checkpoint Review

- Runtime reviewer
- API and contract reviewer
- Architecture reviewer

### Exit Criteria

- fresh cloud worker can recover normal runtime state from Neon
- ordinary runtime surfaces no longer depend on preexisting local cache files

## Phase 4: Read Path Refactor and Authority Centralization

### Goal

Eliminate backend-specific branching from ordinary read paths and make authority selection explicit.

### Deliverables

- read-path abstractions for source/model/runtime data
- centralized authority routing
- reduced direct SQLite schema probing in business modules

### Concrete Tasks

1. Refactor `backend/data/core_reads.py` behind a source-read abstraction.
2. Refactor `backend/data/history_queries.py` to stop owning backend-specific storage decisions.
3. Identify and isolate remaining SQLite schema probes in:
   - analytics services
   - risk modules
   - universe modules
4. Move backend-specific inspection or SQL differences behind store implementations.
5. Tighten operator/status descriptions so authority messaging matches the code.

### Suggested Module Shape

- `backend/data/source_store.py`
- `backend/data/runtime_store.py`
- `backend/data/model_store.py`
- `backend/data/scratch_store.py`

### Primary Files

- `backend/data/core_reads.py`
- `backend/data/history_queries.py`
- `backend/api/routes/operator.py`
- read-path tests

### Checkpoint Review

- Architecture reviewer
- API and contract reviewer
- Test and docs reviewer

### Exit Criteria

- ordinary read modules no longer choose between Neon and SQLite internally in scattered ways
- authority selection is expressed in one place

## Phase 5: Risk-Stage Extraction and Store-Backed Port

### Goal

Remove SQLite durable-authority assumptions from factor returns, covariance, and specific-risk stages.

### Deliverables

- risk stage split into:
  - load
  - compute
  - writeback
- store-backed input and output layers
- explicit distinction between durable artifacts and scratch artifacts

### Concrete Tasks

1. Refactor `backend/risk_model/daily_factor_returns.py` into storage-separated components.
2. Refactor covariance and specific-risk builders to consume store-backed inputs rather than SQLite-only cache tables.
3. Decide which of these remain scratch versus durable:
   - residual history
   - factor-return metadata
   - eligibility summaries
4. If any scratch remains, move it behind an explicit scratch interface.
5. Update orchestrator and analytics pipeline to consume the new interfaces.

### Optimization and Cleanup

- bound large data loads from Neon rather than copying whole retained windows if not needed
- reduce repetitive SQLite open/close patterns
- keep numerical code unchanged where practical, moving only load/write concerns

### Primary Files

- `backend/risk_model/daily_factor_returns.py`
- `backend/risk_model/covariance.py`
- `backend/risk_model/specific_risk.py`
- `backend/risk_model/eligibility.py`
- `backend/analytics/pipeline.py`

### Checkpoint Review

- Architecture reviewer
- Data reviewer
- Runtime reviewer

### Exit Criteria

- factor-return and risk-model stages do not require SQLite durable authority
- any remaining scratch is explicit and documented

## Phase 6: Remove Neon-Backed Scratch SQLite Workspace

### Goal

Remove workspace materialization from the ordinary Neon-authoritative rebuild path.

### Deliverables

- direct Neon-backed execution for `core-weekly` and `cold-core`
- removal or strong narrowing of workspace mirroring logic

### Concrete Tasks

1. Replace `prepare_neon_rebuild_workspace(...)` based execution with direct Neon-backed store execution.
2. Remove path-swapping and workspace routing from:
   - `backend/orchestration/run_model_pipeline.py`
3. Narrow or delete:
   - workspace copy-back logic
   - local mirror sync of derived outputs
4. Retain only explicit local-ingest/archive behavior.

### Primary Files

- `backend/services/neon_authority.py`
- `backend/orchestration/run_model_pipeline.py`
- related tests and docs

### Checkpoint Review

- Architecture reviewer
- Runtime reviewer
- Data reviewer

### Exit Criteria

- ordinary Neon rebuilds do not require copying retained datasets into SQLite first
- rebuild profiles can run directly from Neon-backed stores

## Phase 7: Final Cutover and Legacy Deletion

### Goal

Finalize the standalone Neon-first contract and remove stale migration scaffolding.

### Deliverables

- simplified runtime and rebuild configuration
- dead fallback removal
- final docs and runbook cleanup

### Concrete Tasks

1. Delete or narrow obsolete local-authority fallbacks.
2. Remove dead migration helpers and duplicated schema code.
3. Default configuration and docs to Neon-first semantics.
4. Re-run route and operator/status audits for wording drift.
5. Reconcile tests with final architecture and remove migration-era assumptions.

### Primary Files

- `backend/config.py`
- `backend/orchestration/run_model_pipeline.py`
- `backend/api/routes/operator.py`
- `docs/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/NEON_AUTHORITATIVE_REBUILD_PLAN.md`
- `docs/OPERATIONS_PLAYBOOK.md`

### Checkpoint Review

- All five reviewers

### Exit Criteria

- local SQLite role is narrow and explicit
- no ordinary runtime/rebuild path depends on local SQLite authority

## Required Review Checkpoints

Run these explicit checkpoints:

- Checkpoint A: after Phase 0
- Checkpoint B: after Phase 2
- Checkpoint C: after Phase 3
- Checkpoint D: after Phase 5
- Checkpoint E: after Phase 6
- Final checkpoint: after Phase 7

Each checkpoint must include:

1. independent review outputs from all required tracks
2. fixes for all critical and high findings
3. a zoom-out audit
4. doc update confirmation

## Zoom-Out Audit Template

At every checkpoint, perform this audit:

### Authority Audit

- where is durable authority for source data?
- where is durable authority for model data?
- where is durable authority for runtime state?
- where is durable authority for serving payloads?

### Hidden Dependency Audit

- any remaining direct `sqlite3.connect(...)` in ordinary runtime/rebuild modules?
- any route still depending on local cache when it should not?
- any fallback that silently changes authority instead of failing closed?

### Cold-Start Audit

- can a fresh process recover from Neon only?
- what still assumes local files already exist?

### Operator Audit

- does `/api/operator/status` describe the real authority?
- do docs and runtime warnings match actual behavior?

### Performance Audit

- any unnecessary full-table copies?
- any repeated materialization that should become bounded query loads?

### Delete-or-Narrow Audit

- which migration helpers can be deleted now?
- which fallbacks must be narrowed before the next phase?

## Final Multi-Agent Review and Fix Loop

Before declaring the migration complete:

1. Run a final independent review across all five tracks.
2. Collect findings only, ordered by severity.
3. Fix all critical and high findings.
4. Re-run the final independent review.
5. Perform one final zoom-out audit.

## Final Acceptance Criteria

The migration is complete only if all of the following are true:

- Neon is the durable authority for ordinary runtime and rebuild work
- local SQLite is not required for ordinary serving or rebuild execution
- local SQLite remains only an ingest/archive system unless explicitly invoked
- the operator and architecture docs describe the final reality
- tests prove cold-start and standalone behavior on Neon-backed execution

## Execution Status Board

Phase status:

- [ ] Phase 0 complete
- [ ] Phase 1 complete
- [ ] Phase 2 complete
- [ ] Phase 3 complete
- [ ] Phase 4 complete
- [ ] Phase 5 complete
- [ ] Phase 6 complete
- [ ] Phase 7 complete

Checkpoint status:

- [ ] Checkpoint A approved
- [ ] Checkpoint B approved
- [ ] Checkpoint C approved
- [ ] Checkpoint D approved
- [ ] Checkpoint E approved
- [ ] Final checkpoint approved

Final readiness:

- [ ] standalone runtime on Neon proven
- [ ] standalone rebuild on Neon proven
- [ ] local SQLite narrowed to ingest/archive
- [ ] docs and runbooks aligned
