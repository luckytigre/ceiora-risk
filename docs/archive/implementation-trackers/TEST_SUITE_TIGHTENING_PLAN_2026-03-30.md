# Test Suite Tightening Plan

Date: 2026-03-30
Status: Active engineering cleanup tracker; not part of the operational docs surface
Owner: Codex

## Purpose

This document turns the current backend test cleanup into an ordered execution
plan.

Phase grouping in this document is logical grouping, not strict execution
order. After the current local extraction slice is committed, the safest next
full mega-file split is the `test_core_reads.py` price contract split before
returning to the remaining `test_operating_model_contract.py` clusters.

The goal is not to "simplify tests" by deleting protection. The goal is to:

- reduce oversized test files
- reduce misplaced coverage
- keep runtime and migration contracts explicit
- lower the cost of future repo cleanup work
- preserve the existing behavioral assertions while making the suite easier to
  extend and review

## Current Problems

The current backend suite is valuable, but a small number of files carry too
much coupling:

- `backend/tests/test_operating_model_contract.py`
- `backend/tests/test_core_reads.py`
- `backend/tests/test_security_master_lineage.py`
- `backend/tests/test_universe_migration_scaffolding.py`
- `backend/tests/test_refresh_profiles.py`
- `backend/tests/test_cpar_source_reads.py`

Observed issues:

- unrelated concerns are packed into the same file
- repeated monkeypatch bundles restub the same orchestration seams inline
- repeated SQLite/bootstrap setup is hand-written in many tests
- the shared fixture layer is too thin to support structural cleanup
- broad contract tests are mixed with narrower owner-level tests

## Guardrails

- Do not change product behavior as part of test cleanup slices.
- Do not delete historical compatibility coverage until a narrower replacement
  exists and is green.
- Do not put new autouse behavior into `backend/tests/conftest.py` unless the
  effect is truly suite-wide.
- Prefer dedicated local support modules over generic global helpers.
- Split test files by contract or owner boundary, not by arbitrary line count.
- Keep one rollback-safe slice per commit.

## Validation Minimums

Every slice must run:

- `git diff --check -- <touched paths>`
- the full pytest bundle for the touched files

If a slice adds a helper module under `backend/tests/support/`, the validation
bundle must also include every touched test file that imports that helper.

If a slice moves tests between files, the validation bundle must include both:

- the new files
- the residual original file

until the original file no longer owns any overlapping coverage.

## Phase 1: Disassemble `test_operating_model_contract.py`

This file is too broad, but it should not be attacked in one rewrite.

### Slice 1

Scope:
- move clearly mis-housed unit tests out of
  `backend/tests/test_operating_model_contract.py`

Files:
- add `backend/tests/test_positions_store_contract.py`
- add `backend/tests/test_risk_views_positions_contract.py`
- add `backend/tests/test_holdings_runtime_state_contract.py`
- trim `backend/tests/test_operating_model_contract.py`

Owned contracts:
- `backend.portfolio.positions_store`
- `backend.analytics.services.risk_views`
- `backend.services.holdings_runtime_state`

Do not touch in this slice:
- the autouse persisted-model-state fixture in
  `backend/tests/test_operating_model_contract.py`
- `pipeline.run_refresh(...)` coverage
- `run_model_pipeline(...)` coverage
- `refresh_manager` coverage
- production code
- `backend/tests/support/*`

Validation:
- `pytest -q backend/tests/test_positions_store_contract.py backend/tests/test_risk_views_positions_contract.py backend/tests/test_holdings_runtime_state_contract.py backend/tests/test_operating_model_contract.py`

Rollback boundary:
- pure test-file extraction only

### Slice 2

Execution order note:
- do not execute this immediately after Slice 1 if the safer
  `test_core_reads.py` price split is still pending

Goal:
- extract `run_model_pipeline` and `_run_stage` orchestration contracts from
  `backend/tests/test_operating_model_contract.py`

Files:
- add `backend/tests/test_run_model_pipeline_contract.py`
- trim `backend/tests/test_operating_model_contract.py`

Planned moved coverage:
- `run_model_pipeline(...)` status and stage bookkeeping
- `resolved_as_of_date(...)`
- `source-daily` default as-of behavior
- `_run_stage(..., stage="serving_refresh")` backend/authority selection
- stage runtime detail reporting

Do not touch in this slice:
- `pipeline.run_refresh(...)` tests
- `refresh_manager` tests
- production code

Validation:
- `pytest -q backend/tests/test_run_model_pipeline_contract.py backend/tests/test_operating_model_contract.py`

Rollback boundary:
- pure test-file extraction for orchestration contract coverage

### Slice 3

Goal:
- extract `refresh_manager` worker/lifecycle contracts from
  `backend/tests/test_operating_model_contract.py`

Files:
- add `backend/tests/test_refresh_manager_contract.py`
- trim `backend/tests/test_operating_model_contract.py`

Planned moved coverage:
- `_run_in_background(...)`
- `start_refresh(...)`
- pending/failed status transitions
- stage callback state propagation

Do not touch in this slice:
- `pipeline.run_refresh(...)` tests
- production code

Validation:
- `pytest -q backend/tests/test_refresh_manager_contract.py backend/tests/test_operating_model_contract.py`

Rollback boundary:
- pure test-file extraction for refresh-manager lifecycle coverage

### Slice 4

Goal:
- stabilize the remaining `pipeline.run_refresh(...)` contract file and add a
  dedicated local support module only if the repeated patch bundles are still
  obscuring the tests

Files:
- `backend/tests/test_operating_model_contract.py`
- optional: `backend/tests/support/pipeline_contract.py`

Allowed extraction:
- only helpers that serve this contract family
- no new suite-wide autouse fixtures

Validation:
- `pytest -q backend/tests/test_operating_model_contract.py`

Rollback boundary:
- one contract family only

## Phase 2: Split `test_core_reads.py`

`backend/tests/test_core_reads.py` is the safest first full mega-file split on
the SQLite/read-facade side because it stays on one ownership seam.

### Slice 5

Goal:
- split latest-price and registry-first price contract coverage

Files:
- add `backend/tests/test_core_reads_prices.py`
- trim `backend/tests/test_core_reads.py`

Coverage to move:
- latest-price reads
- registry-first price authority behavior
- fail-closed price behavior when registry/current-state surfaces are missing

Validation:
- `pytest -q backend/tests/test_core_reads_prices.py backend/tests/test_core_reads.py`

### Slice 6A

Goal:
- split latest-fundamentals coverage

Files:
- add `backend/tests/test_core_reads_fundamentals.py`
- trim `backend/tests/test_core_reads.py`

Validation:
- `pytest -q backend/tests/test_core_reads_fundamentals.py backend/tests/test_core_reads.py`

### Slice 6B

Goal:
- split raw-cross-section contracts only

Files:
- add `backend/tests/test_core_reads_cross_section.py`
- trim `backend/tests/test_core_reads.py`

Validation:
- `pytest -q backend/tests/test_core_reads_cross_section.py backend/tests/test_core_reads.py`

### Slice 6C

Goal:
- split source-date and backend-selection contracts

Files:
- add `backend/tests/test_core_reads_source_dates.py`
- trim `backend/tests/test_core_reads.py`

Validation:
- `pytest -q backend/tests/test_core_reads_source_dates.py backend/tests/test_core_reads.py`

## Phase 3: Add Narrow SQLite/Bootstrap Support

### Slice 7

Goal:
- introduce a narrow SQLite runtime helper and migrate its first consumer in the
  same commit

Files:
- add `backend/tests/support/sqlite_runtime.py`
- trim `backend/tests/test_security_registry_sync.py`

Allowed contents:
- `ensure_cuse4_schema(...)` wrappers
- SQLite temp DB creation helpers
- narrow row-fetch helpers for temp test DBs

Forbidden contents:
- generic monkeypatch registries
- global suite fixtures
- unrelated consumer rewrites outside `test_security_registry_sync.py`

Validation:
- `pytest -q backend/tests/test_security_registry_sync.py`

### Slice 8

Goal:
- add registry-seed builders and migrate the first consumer in the same commit

Files:
- add `backend/tests/support/registry_seed_builders.py`
- trim `backend/tests/test_security_master_seed_hygiene.py`

Allowed contents:
- tiny registry/current-state seed CSV writers
- current-state row builders for registry/policy/taxonomy/compat surfaces

Forbidden contents:
- suite-wide fixtures
- unrelated consumer rewrites outside `test_security_master_seed_hygiene.py`

Validation:
- `pytest -q backend/tests/test_security_master_seed_hygiene.py`

### Slice 9

Goal:
- migrate additional registry/bootstrap consumers onto the new narrow support
  helpers only after the first consumer slices are green

Files:
- `backend/tests/test_security_registry_sync.py`
- `backend/tests/test_security_master_seed_hygiene.py`
- optional narrow extensions to `backend/tests/support/sqlite_runtime.py`
- optional narrow extensions to `backend/tests/support/registry_seed_builders.py`

Validation:
- `pytest -q backend/tests/test_security_registry_sync.py backend/tests/test_security_master_seed_hygiene.py`

## Phase 4: Migrate the SQLite/Registry Hotspots

### Slice 10

Goal:
- extract `source_observation` contracts from
  `test_universe_migration_scaffolding.py`

Planned files:
- `backend/tests/test_source_observation_contract.py`

Validation:
- `pytest -q backend/tests/test_source_observation_contract.py backend/tests/test_universe_migration_scaffolding.py`

### Slice 11

Goal:
- extract `runtime_rows` migration contracts from
  `test_universe_migration_scaffolding.py`

Planned files:
- `backend/tests/test_runtime_rows_migration_contract.py`

Validation:
- `pytest -q backend/tests/test_runtime_rows_migration_contract.py backend/tests/test_universe_migration_scaffolding.py`

### Slice 12

Goal:
- extract taxonomy/materialization contracts from
  `test_universe_migration_scaffolding.py`

Planned files:
- `backend/tests/test_taxonomy_materialization_contract.py`

Validation:
- `pytest -q backend/tests/test_taxonomy_materialization_contract.py backend/tests/test_universe_migration_scaffolding.py`

### Slice 13

Goal:
- split `test_security_master_lineage.py` bootstrap/seed-sync contracts

Planned files:
- `backend/tests/test_security_master_bootstrap_contract.py`

Validation:
- `pytest -q backend/tests/test_security_master_bootstrap_contract.py backend/tests/test_security_master_lineage.py`

### Slice 14

Goal:
- split `test_security_master_lineage.py` LSEG ingest/backfill lineage

Planned files:
- `backend/tests/test_security_master_lseg_lineage.py`

Validation:
- `pytest -q backend/tests/test_security_master_lseg_lineage.py backend/tests/test_security_master_lineage.py`

### Slice 15

Goal:
- split `test_security_master_lineage.py` raw-cross-section lineage

Planned files:
- `backend/tests/test_security_master_raw_history_lineage.py`

Validation:
- `pytest -q backend/tests/test_security_master_raw_history_lineage.py backend/tests/test_security_master_lineage.py`

### Slice 16

Goal:
- split `test_security_master_lineage.py` legacy export/backfill compatibility

Planned files:
- `backend/tests/test_security_master_legacy_compat.py`

Validation:
- `pytest -q backend/tests/test_security_master_legacy_compat.py backend/tests/test_security_master_lineage.py`

## Phase 5: Split Refresh/Neon Integration Hotspots

### Slice 17

Goal:
- split `backend/tests/test_refresh_profiles.py` profile-catalog and
  import/runtime-entry contracts

Planned files:
- `backend/tests/test_refresh_profile_catalog.py`

Validation:
- `pytest -q backend/tests/test_refresh_profile_catalog.py backend/tests/test_refresh_profiles.py`

### Slice 18

Goal:
- split `backend/tests/test_refresh_profiles.py` source-sync contracts

Planned files:
- `backend/tests/test_refresh_source_sync_contract.py`

Validation:
- `pytest -q backend/tests/test_refresh_source_sync_contract.py backend/tests/test_refresh_profiles.py`

### Slice 19

Goal:
- split `backend/tests/test_refresh_profiles.py` Neon-readiness contracts

Planned files:
- `backend/tests/test_refresh_neon_readiness_contract.py`

Validation:
- `pytest -q backend/tests/test_refresh_neon_readiness_contract.py backend/tests/test_refresh_profiles.py`

### Slice 20

Goal:
- split `backend/tests/test_refresh_profiles.py` serving-refresh-stage contracts

Planned files:
- `backend/tests/test_serving_refresh_stage_contract.py`

Validation:
- `pytest -q backend/tests/test_serving_refresh_stage_contract.py backend/tests/test_refresh_profiles.py`

### Slice 21

Goal:
- split `backend/tests/test_cpar_source_reads.py` by package-source contract
  families if it still blocks cleanup work after the earlier slices

Validation:
- `pytest -q backend/tests/test_cpar_source_reads.py`

## Commit Strategy

- keep commits code-first and rollback-safe
- fold updates to this file into the relevant code slice when the slice status
  or scope changes materially
- keep each code slice rollback-safe and path-scoped

## Out Of Scope

- production-code refactors
- test deletion for speed alone
- broad pytest marker redesign before the oversized files are split
- moving active cleanup notes to archive before the execution track is complete
