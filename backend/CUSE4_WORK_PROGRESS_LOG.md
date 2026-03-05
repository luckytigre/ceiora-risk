# cUSE4 Backend Work Progress Log

## 2026-03-03

### Entry 01 - Audit and Orientation
- Read `user notes/cUSE4_engine_spec.md` end-to-end and captured locked decisions.
- Audited backend runtime flow from API routes to refresh manager and analytics pipeline.
- Audited current modeling modules (`barra/*`) for eligibility, factor returns, covariance, specific risk, and risk attribution.
- Audited schema + ingestion scripts (`db/*`, `scripts/*`) and verified LSEG + trading calendar code paths to preserve.
- Snapshotted source table counts in `backend/data.db` and cache status in `backend/cache.db`.
- Confirmed identity continuity risk in universe table (1,311 synthetic `RIC::` IDs), consistent with the spec.

### Entry 02 - Planning
- Created `backend/CUSE4_BACKEND_RECONSTRUCTION_PLAN.md` with:
  - current-state audit,
  - target architecture,
  - phased implementation plan,
  - QA gates,
  - commit sequencing.
- Defined a non-breaking migration approach that keeps existing API contracts while replacing internals incrementally.

### Next Active Task
- Implement Phase A foundation code:
  - create `backend/cuse4/` package,
  - add canonical cUSE4 schema management,
  - add bootstrap pipeline from legacy tables,
  - begin ESTU membership persistence.

### Entry 03 - Phase A Foundation Code Implemented
- Added new cUSE4 package modules:
  - `backend/cuse4/schema.py` for canonical cUSE4 tables:
    - `security_master`
    - `fundamentals_history`
    - `trbc_industry_country_history`
    - `estu_membership_daily`
  - `backend/cuse4/bootstrap.py` for legacy-to-cUSE4 source bootstrapping.
  - `backend/cuse4/settings.py` for versioned profile + ESTU policy knobs.
  - `backend/cuse4/estu.py` for ESTU construction and per-security drop-reason persistence.
- Added CLI scripts:
  - `backend/scripts/bootstrap_cuse4_source_tables.py`
  - `backend/scripts/build_cuse4_estu_membership.py`
- Added Makefile shortcuts:
  - `make cuse4-bootstrap`
  - `make cuse4-estu`

### Entry 04 - Runtime Integration and Observability
- Integrated cUSE4 foundation maintenance into refresh pipeline (`backend/analytics/pipeline.py`):
  - optional bootstrap + ESTU build during refresh,
  - persisted summary under cache key `cuse4_foundation`,
  - included cUSE4 foundation payload in `refresh_meta` and refresh response.
- Added diagnostics visibility in `GET /api/data/diagnostics`:
  - table stats for all new cUSE4 tables,
  - cached `cuse4_foundation` payload.
- Added config flags:
  - `CUSE4_ENABLE_ESTU_AUDIT` (default true)
  - `CUSE4_AUTO_BOOTSTRAP` (default true)
- Updated `user notes/OPERATIONS_PLAYBOOK.md` with new cUSE4 commands and cache key.

### Entry 05 - Validation Results
- Bootstrap command run:
  - `python3 backend/scripts/bootstrap_cuse4_source_tables.py --db-path backend/data.db`
  - Result:
    - `security_master_rows=4113`
    - `fundamentals_history_rows=283492`
    - `trbc_industry_country_history_rows=147036`
- ESTU build command run:
  - `python3 backend/scripts/build_cuse4_estu_membership.py --db-path backend/data.db`
  - Result:
    - `rows_written=4113`
    - `estu_count=2292`
    - `drop_reason_counts` captured and persisted.
- End-to-end refresh validation run:
  - `run_refresh(mode='light')`
  - Returned `status='ok'`, `cuse4_foundation_status='ok'`, and preserved normal portfolio/risk cache generation.

### Entry 06 - Universe Constrained to User Holdings XLSX
- Added `backend/data.db` raw candidate table:
  - `universe_candidate_holdings`
- Synced from `/Users/shaun/Dropbox (Personal)/040 - Creating/barra-dashboard/Universe Candidates` and enforced universe-only scope:
  - `ticker_ric_map` replaced from candidate set
  - `universe_eligibility_summary` replaced from candidate set
  - `universe_constituent_snapshots` replaced from candidate set
- Updated cUSE4 bootstrap behavior:
  - if `universe_candidate_holdings` is present, `security_master` is built from those tickers only.
- Rebuilt cUSE4 source + ESTU after sync.
- Resulting constrained counts:
  - `security_master`: 2,871
  - `ticker_ric_map`: 2,871
  - `universe_eligibility_summary` distinct tickers: 2,871
  - `estu_membership_daily` latest date rows: 2,871

### Entry 07 - Universe Creation Tooling Removed (Per User Request)
- Deleted universe-construction scripts and schema helpers:
  - `backend/scripts/build_universe_eligibility_lseg.py`
  - `backend/scripts/reset_and_rebuild_source_of_truth.py`
  - `backend/scripts/sync_universe_from_holdings_xlsx.py`
  - `backend/db/universe_schema.py`
  - `backend/universe_eligibility_summary_sanity.csv`
- Removed Makefile targets and playbook references tied to universe creation/rebuild flows.

### Entry 08 - RIC Source Alignment + HQ Country Backfill
- Verified current RIC source path and corrected mapping authority:
  - Reseeded `ticker_ric_map` directly from `universe_candidate_holdings` (deterministic canonical pick per ticker by latest `as_of_date`, then largest absolute weight).
  - Synced `universe_eligibility_summary.current_ric` to match reseeded map.
- Patched LSEG ingest to request and store headquarters country code:
  - Added `TR.HQCountryCode` to pull fields in `backend/scripts/download_data_lseg.py`.
  - Added country label resolution for LSEG response (`Country ISO Code of Headquarters`).
  - Added `hq_country_code` persistence to `trbc_industry_history`.
- Re-ran multi-date full backfill for universe scope (2,871 names) through:
  - `2016-12-30`, `2021-12-31`, `2024-12-31`, `2026-02-27`.
  - Skipped `2026-03-03` per user instruction.
- Rebuilt canonical cUSE4 source tables after backfill:
  - `security_master`
  - `fundamentals_history`
  - `trbc_industry_country_history`
- Validation highlights after fix:
  - `trbc_industry_country_history` now has strong country fill (`hq_country_code` present on 2,743 / 2,871 names at each refreshed snapshot date).
  - ESTU drop reason `missing_trbc` is eliminated on latest refreshed date (`2026-02-27`), confirming the country-field fix propagated correctly.

### Entry 09 - Canonical TRBC Direct Sync + Sample Test Discipline
- Updated `download_data_lseg.py` to sync TRBC rows directly into canonical `trbc_industry_country_history` during ingest (in addition to legacy staging), keyed by `sid`.
- Scoped canonical sync to current ingest `job_run_id` so sample tests only touch sampled names.
- Added HQ country label compatibility for LSEG response (`Country ISO Code of Headquarters`).
- Confirmed with sample run (`AAPL, MSFT`) that canonical sync reports `trbc_rows_synced_canonical = 2` and no universe expansion occurred.
- Re-confirmed `ticker_ric_map` remains constrained to holdings universe (`2,871` rows/tickers/RICs).

### Entry 10 - Canonical Non-Redundant Schema Migration Executed
- Implemented canonical table naming and write-path refactor:
  - `security_prices_eod`
  - `security_fundamentals_pit`
  - `security_classification_pit`
- Updated cUSE4 schema constants and ESTU read path to use canonical tables.
- Rewrote LSEG ingest (`backend/scripts/download_data_lseg.py`) to:
  - derive ingest universe from `security_master` only,
  - use `sid` identity directly,
  - write directly into canonical time-series tables,
  - remove dependency on `ticker_ric_map` and legacy staging writes.
- Added migration/backfill runner (`backend/scripts/migrate_to_canonical_timeseries.py`) with date-chunk batching and transaction boundaries to avoid long stuck operations.
- Ran full backfill with `--drop-deprecated`:
  - fundamentals backfilled: 11,484 rows
  - classification backfilled: 11,484 rows
  - prices backfilled: 4,967,121 rows
- Dropped deprecated physical tables:
  - `ticker_ric_map`, `fundamental_snapshots`, `trbc_industry_history`, `prices_daily`, `fundamentals_history`, `trbc_industry_country_history`, `universe_candidate_holdings`
- Recreated legacy names as read-only compatibility views (no duplicate persisted storage).
- Post-migration validation:
  - canonical row counts: `security_master=2871`, `security_prices_eod=4967121`, `security_fundamentals_pit=11484`, `security_classification_pit=11484`
  - duplicate key checks: zero on all canonical time-series PKs
  - ESTU build on latest backfilled date (`2026-02-27`) succeeded (`estu_count=1930`)

### Entry 11 - Rollback Extension to 2012 + Price Recovery
- Executed PIT backfill extension (quarterly) for full universe to include pre-2016 history:
  - date range loaded: `2012-03-30` through `2016-12-30`
  - table outcomes:
    - `security_fundamentals_pit = 66,033` rows (`2,871 x 23`)
    - `security_classification_pit = 66,033` rows (`2,871 x 23`)
- Executed daily price backfill extension:
  - initial run range: `2012-01-01` through `2016-02-15`
  - produced expected 2012-2016 coverage but surfaced a script-side truncation bug.
- Root cause identified:
  - `backfill_prices_range_lseg.py` used a global cleanup step deleting all rows with `date > end_date` at the end of the run.
  - when run with `end_date=2016-02-15`, it unintentionally removed post-2016 prices.
- Implemented fix in script:
  - removed global post-run delete behavior.
  - added per-row date bound filter so only in-range rows are upserted.
- Performed full recovery by re-pulling from LSEG directly in manageable windows:
  - recovery range: `2016-02-13` through `2026-03-03`
  - `rows_upserted=5,398,488`, `failed_batches=0`
- Final canonical audit after recovery and cleanup:
  - `security_prices_eod`: `6,847,526` rows, `2,871` distinct SIDs, date range `2012-01-03` to `2026-03-03`
  - `security_fundamentals_pit`: `66,033` rows, date range `2012-03-30` to `2026-02-27`
  - `security_classification_pit`: `66,033` rows, date range `2012-03-30` to `2026-02-27`
  - removed spillover rows before `2012-01-01` from prices (`10` rows deleted).

### Entry 12 - Universe Extension from 2026-03-03 Holdings + Targeted Backfill
- Ingested `/Users/shaun/Downloads/Derived Holdings 2026-03-03.xlsx` (`RIC` column), normalized and deduped.
- Universe merge outcome:
  - workbook unique RICs: `365`
  - new vs existing `security_master`: `148`
  - inserted into `security_master`: `148`
  - eligible universe size: `3,019` (was `2,871`)
- Added targeted RIC-subset support to ingest/backfill tooling:
  - `download_data_lseg.py`: added `--rics` filter (subset by canonical `security_master.ric`)
  - `backfill_pit_history_lseg.py`: added `--rics` passthrough
  - `backfill_prices_range_lseg.py`: added `--rics` subset filter
- Backfill execution for newly added RICs only:
  - PIT dates backfilled: all existing `23` snapshot dates (`2012-03-30` ... `2026-02-27`) with zero failures
  - daily prices backfilled range: `2012-01-01` ... `2026-03-03`
  - `price_rows_upserted`: `359,331`, `failed_batches`: `0`
- Post-run validation for new 148 names:
  - fundamentals/classification PIT coverage: `148/148` on all 23 dates
  - prices coverage range: `2012-01-03` ... `2026-03-03` across all 148 SIDs
  - random spot checks completed across mixed RIC patterns (including suffix/caret variants).

### Entry 13 - Canonical Analytics Read Refactor + Current Snapshot Policy
- Rewired primary analytics reads away from legacy compatibility views:
  - `backend/db/postgres.py` now loads latest fundamentals from `security_fundamentals_pit` joined to `security_master`, with TRBC overlay from `security_classification_pit`.
  - latest prices now load from `security_prices_eod` joined to `security_master`.
  - source-date resolver now uses canonical PIT max date.
- Rewired raw factor cross-section builder to canonical source-of-truth:
  - `backend/barra/raw_cross_section_history.py` reads prices/fundamentals/classification from `security_*` tables only.
  - retained expected downstream column names (`book_value`, `return_on_equity`, etc.) via canonical->model mapping.
  - added operating-margin fallback for profitability raw descriptor when gross profit is unavailable.
- Rewired eligibility market-cap/TRBC panels to canonical tables first:
  - `backend/barra/eligibility.py` now pulls market cap + TRBC panels from `security_fundamentals_pit` / `security_classification_pit` and uses `universe_cross_section_snapshot` only as fallback.
- Implemented snapshot table policy to prevent unnecessary historical bloat:
  - `backend/db/cross_section_snapshot.py` now supports `mode=current|full`.
  - `mode=current` keeps latest row per eligible `security_master` ticker; `mode=full` retains historical rows.
  - refresh pipeline now uses configurable `CROSS_SECTION_SNAPSHOT_MODE` (default `current`).
- Updated diagnostics endpoint to foreground canonical source tables:
  - `security_fundamentals_pit`, `security_classification_pit`, `security_prices_eod`.
- Validation:
  - static compile checks passed for all touched modules.
  - canonical read-path smoke test passed (`load_source_dates`, `load_fundamental_snapshots`, `load_latest_prices`).
  - write-path smoke tests for snapshot rebuild were partially blocked by concurrent long-running monthly fundamentals backfill lock contention; no destructive action taken.

### Entry 14 - RIC Physical Key Migration + Runtime Cutover
- Executed one-time physical key migration on `backend/data.db` to move canonical time-series tables from `sid` to `ric` storage keys.
- Backup created before migration:
  - `backend/data.db.pre_ric_keys_20260304T071709Z.bak`
- Canonical table row parity validated after migration:
  - `security_prices_eod`: `7,092,089 -> 7,092,089`
  - `security_fundamentals_pit`: `513,230 -> 513,230`
  - `security_classification_pit`: `513,230 -> 513,230`
  - `estu_membership_daily`: `2,871 -> 2,871`
- Post-migration schema now physically RIC-keyed:
  - `security_prices_eod` PK `(ric, date)`
  - `security_fundamentals_pit` PK `(ric, as_of_date, stat_date)`
  - `security_classification_pit` PK `(ric, as_of_date)`
  - `estu_membership_daily` PK `(date, ric)`
- Added/verified canonical uniqueness/indexing:
  - `ux_security_master_ric` unique index on `security_master(ric)`
- Refactored active runtime and ingest/backfill modules to remove `sid` joins in canonical paths and use `ric` joins/writes.
- Smoke checks after cutover:
  - `postgres.load_latest_prices()` returned `3019` rows.
  - `postgres.load_latest_fundamentals()` returned `3019` rows.
  - `daily_factor_returns._load_prices()` returned `6,977,698` rows.
- Removed one-time migration utility from active scripts after successful execution:
  - deleted `backend/scripts/migrate_canonical_keys_to_ric.py`.

### Entry 15 - Phase 2 Output Table Normalization Setup
- Added new relational Layer-B output repository module:
  - `backend/db/model_outputs.py`
- Added/managed new output tables in `backend/data.db`:
  - `model_factor_returns_daily`
  - `model_specific_residuals_daily`
  - `model_factor_covariance_daily`
  - `model_specific_risk_daily`
  - `model_run_metadata`
- Implemented table schema ensures + indexes + idempotent upsert writers.
- Integrated refresh pipeline dual-write:
  - `backend/analytics/pipeline.py` now generates a `run_id` per refresh and persists model outputs to `model_*` tables.
  - Existing cache writes remain in place as secondary acceleration layer.
  - New cache key `model_outputs_write` stores the latest relational-write status payload.
- Added run-manifest persistence:
  - `model_run_metadata` now records refresh mode/status, run params, source-date lineage, risk engine state, and row-count summaries.
- Validation:
  - Python compile checks passed for `pipeline.py` and `model_outputs.py`.
  - Direct persistence smoke test passed and then test rows were removed to keep production tables clean.

### Entry 16 - Phase 3 Orchestrator Profiles + Stage Checkpoints
- Added run-stage persistence module:
  - `backend/db/job_runs.py`
  - New table in `backend/data.db`: `job_run_status`
  - Captures per-stage state (`running/completed/skipped/failed`) with timestamps, details JSON, and errors.
- Added profile-driven orchestrator:
  - `backend/jobs/run_model_pipeline.py`
  - Stage sequence:
    1. `ingest`
    2. `feature_build`
    3. `estu_audit`
    4. `factor_returns`
    5. `risk_model`
    6. `serving_refresh`
- Added profile semantics to match cadence requirements:
  - `daily-fast`: skip weekly core recompute path.
  - `daily-with-core-if-due`: run core only when due.
  - `weekly-core`: always run core recompute path.
- Added resume/partial execution support:
  - `--resume-run-id`
  - `--from-stage`
  - `--to-stage`
  - `--force-core`
- Added script wrapper CLI:
  - `backend/scripts/run_model_pipeline.py`
- Extended refresh pipeline for orchestrator compatibility:
  - `run_refresh(...)` now accepts optional skips:
    - `skip_snapshot_rebuild`
    - `skip_cuse4_foundation`
    - `skip_risk_engine`
  - Guardrail added: if `skip_risk_engine=True` but risk cache is missing, refresh fails fast.
- Validation:
  - compile checks passed for `job_runs.py`, `run_model_pipeline.py`, and updated `pipeline.py`.
  - checkpoint/resume behavior verified on targeted lightweight stage runs.
  - temporary demo checkpoint rows removed after validation.

### Entry 17 - Additional Refactor QA + API/Manager Orchestrator Cutover
- Conducted additional no-loose-ends audit checks on active backend code (excluding `_archive` and docs):
  - no legacy compatibility table-name references remain in active runtime/ops code.
  - no active canonical joins still anchored on `sid` for canonical time-series paths.
  - `backend/data.db` compatibility views count remains `0`.
  - canonical time-series tables confirmed to have zero `sid` columns.
- Added API/background cutover to profile orchestrator:
  - `backend/services/refresh_manager.py` now resolves profiles and executes `run_model_pipeline(...)` in background.
  - `backend/routes/refresh.py` now supports orchestrator parameters and keeps backward-compatible mode mapping.
- Backward-compatible behavior kept:
  - `mode=light` maps to profile `daily-fast`.
  - `mode=full` maps to profile `daily-with-core-if-due`.
  - `force_risk_recompute=true` maps to `force_core=true`.
- Added input validation guards:
  - invalid `profile` or stage names now return request validation errors (400 via route wrapper).
- Additional smoke tests executed:
  - refresh manager background run with `daily-fast` + `ingest` stage-only path.
  - mode-mapping tests for `light` and `full` without explicit profile.
  - orchestrator due-policy test for `daily-with-core-if-due` (`factor_returns`/`risk_model` skipped when not due).
- Cleanup performed after tests:
  - removed test rows from `job_run_status`.
  - cleared transient `refresh_status` cache key used in process-local tests.
- Documentation updated for current architecture and operations:
  - `user notes/cUSE4_Backend_Execution_Plan_2026-03-04.md`
  - `user notes/OPERATIONS_PLAYBOOK.md`

### Entry 18 - Full Audit Remediation (All 8 Findings Closed)
- Completed remediation pass for full audit findings across code, data integrity, statistical soundness, and usability.

1) **Style-score build failure fixed (critical)**
- `backend/barra/raw_cross_section_history.py`
  - Removed invalid `orth_rules` argument when calling `assemble_full_style_scores(...)`.
  - Removed broad swallow (`except Exception: continue`) for style-score build path.
  - Re-keyed raw builder internals to `ric` grouping/merges to prevent ticker collisions.

2) **Zero-row false-success fixed (critical)**
- `backend/db/model_outputs.py`
  - Added quality gate: `factor_returns`, `specific_residuals`, `covariance`, and `specific_risk` must all be non-zero for `status=ok` writes.
  - On gate failure, metadata is persisted with `status=failed`, `error_type=quality_gate_failed`, then a runtime error is raised.
- `backend/analytics/pipeline.py`
  - Relational persistence failures now fail the refresh run (no silent success).
- `backend/jobs/run_model_pipeline.py`
  - `factor_returns` and `risk_model` stages now fail if outputs are empty.
- `backend/routes/risk.py`
  - `/api/risk` now rejects incomplete risk cache payloads (non-empty factors/matrix/specific risk required).

3) **Price ingest/backfill field quality upgraded (high)**
- `backend/scripts/download_data_lseg.py`
  - Added richer price field pulls and writes: `open/high/low/close/volume/currency` where available.
- `backend/scripts/backfill_prices_range_lseg.py`
  - Added robust multi-field history pull with fallback field sets.
  - Added warning suppression for noisy third-party dataframe warnings.
- Ran targeted backfill window:
  - `2026-02-01` to `2026-03-03`
  - `rows_upserted=53,779`, `failed_batches=0`.

4) **Identifier consistency hardened (high)**
- `barra_raw_cross_section_history` physical key migrated to `(ric, as_of_date)` with `ticker` retained as attribute.
- `backend/barra/eligibility.py`
  - Structural eligibility context and panels now RIC-indexed for core model logic.
- `backend/barra/daily_factor_returns.py`
  - Returns/residual pipeline now computes on RIC keys.
  - Cache residual schema migrated to `(date, ric)` primary key with `ticker` retained.
- `backend/barra/specific_risk.py`
  - Specific-risk engine now computes by `ric` and carries `ticker` metadata.
- `backend/db/model_outputs.py`
  - Relational residual/specific-risk tables now keyed by `ric` (`ticker` retained as metadata).

5) **Synthetic PermID/SID dependency removed (high)**
- `backend/cuse4/schema.py`
  - `security_master` migrated to `ric` primary key schema.
  - `sid`/`permid` retained as optional metadata columns (not identity requirement).
  - Migration normalizes synthetic placeholders (`permid==ric`, `PERMID::ric`, `RIC::ric`) to `NULL`.
- `backend/cuse4/estu.py`
  - Removed `real_permid`/`missing_real_permid` gating from ESTU eligibility.

6) **Orchestrator ingest stage no longer hardcoded skip (medium)**
- `backend/jobs/run_model_pipeline.py`
  - `ingest` stage now always executes bootstrap checks.
  - Optional LSEG ingest is available behind config flag (`ORCHESTRATOR_ENABLE_INGEST=true`).
- `backend/config.py`
  - Added `ORCHESTRATOR_ENABLE_INGEST` and `ORCHESTRATOR_INGEST_SHARD_COUNT`.
- Validated stage-only run returns completed `bootstrap_only` mode when ingest disabled.

7) **Automated tests added (medium)**
- Added `backend/tests/` suite and `conftest.py` path bootstrap.
- New regression tests cover:
  - security_master migration to RIC PK + synthetic ID cleanup,
  - raw cross-section schema rekey behavior,
  - model output quality gate,
  - ingest stage non-skip behavior,
  - residual cache backward compatibility for RIC fallback.
- Test result: `5 passed`.

8) **SQLite bloat remediation implemented (medium)**
- Added maintenance script:
  - `backend/scripts/compact_sqlite_databases.py`
- Executed compaction:
  - `data.db`: reclaimed `186,028,032` bytes
  - `cache.db`: reclaimed `56,107,008` bytes
- Post-check: `PRAGMA quick_check` = `ok` on both DBs.

**Post-remediation runtime validation**
- Rebuilt raw cross-sections after schema/key migration:
  - full weekly rebuild: `rows_upserted=1,429,280`, `dates_processed=717`
  - targeted refresh: `rows_upserted=12,869`, `dates_processed=5`
- Recomputed core model pipeline:
  - factor returns rows loaded: `253,145`
  - factor count: `73`
  - specific risk count: `2,578`
  - latest factor-return date: `2026-03-03`
- Serving refresh passed with relational writes:
  - `model_factor_returns_daily=253,145`
  - `model_specific_residuals_daily=6,966,880`
  - `model_factor_covariance_daily=5,329` per as-of date
  - `model_specific_risk_daily=2,578` per as-of date

**Coverage outcomes after fixes**
- `barra_raw_cross_section_history` recent style completeness:
  - ~`99.88%` to `99.92%` on most recent dates.
- `daily_universe_eligibility_summary` recent regression coverage:
  - ~`99.45%` to `99.76%` on latest dates.
- `security_prices_eod` recent field coverage (`date >= 2026-02-01`):
  - `volume ~99.79%`
  - `currency ~99.82%`

### Entry 19 - Full Price Metrics Overwrite Backfill (2026-03-04)
- User-directed full overwrite of `security_prices_eod` to backfill non-close metrics (`open/high/low/volume/currency`) across full history.
- Actions:
  - Cleared existing `security_prices_eod`.
  - Ran full-history backfill with batch/window controls:
    - `python3 backend/scripts/backfill_prices_range_lseg.py --db-path backend/data.db --start-date 2012-01-03 --end-date 2026-03-03 --ticker-batch-size 500 --days-per-window 365 --max-retries 3 --sleep-seconds 2.0`
- Run result:
  - `status=ok`
  - `rows_upserted=7,279,119`
  - `batch_calls=105`
  - `failed_batches=0`
  - One transient UDF timeout occurred and recovered within retry flow.

Post-overwrite table state (`security_prices_eod`):
- `rows=7,070,167`
- date range: `2012-01-03` to `2026-03-03`
- priced RICs: `2,675` of `3,019` eligible universe names (`88.61%` breadth)
- overall field coverage:
  - `open/high/low/close`: `99.94%`
  - `volume`: `99.96%`
  - `currency`: `92.07%`
- recent field coverage (`date >= 2026-02-01`):
  - `open/high/low/close`: `99.88%`
  - `volume`: `99.97%`
  - `currency`: `100.00%`

### Entry 20 - Removed Price Exchange Field End-to-End (2026-03-04)
- Removed `exchange` from canonical prices schema (`security_prices_eod`) and from all active ingest/backfill writers.
- Added schema migration logic in `backend/cuse4/schema.py` to rebuild legacy `security_prices_eod` tables and physically drop `exchange`.
- Refactored `backend/db/cross_section_snapshot.py` to remove `price_exchange` and added migration logic to physically drop that legacy snapshot column as well.
- Updated active documentation (`user notes/cUSE4_engine_spec.md`, `user notes/cUSE4_Backend_Execution_Plan_2026-03-04.md`) to reflect the new canonical price field set.

### Entry 21 - Clean Refresh + Snapshot Rebuild Completion (2026-03-04)
- Executed orchestrator ingest stage cleanly:
  - `python3 backend/scripts/run_model_pipeline.py --profile weekly-core --from-stage ingest --to-stage ingest --force-core`
  - Result: `status=ok`, `bootstrap_only`, canonical table counts validated.
- Rebuilt `universe_cross_section_snapshot` for latest cross section (`as_of_date=2026-03-03`) using a deterministic fast one-pass path keyed off latest PIT rows per base universe RIC.
  - Result: `rows_written=2530`, snapshot date range `2026-03-03` only.
- Executed remaining orchestrator stages:
  - `python3 backend/scripts/run_model_pipeline.py --profile weekly-core --from-stage estu_audit --to-stage serving_refresh --force-core`
  - Result: `status=ok`
    - `factor_return_rows_loaded=253145`
    - `factor_count=73`
    - `specific_risk_ticker_count=2578`
    - relational write status `ok` for model output tables.
- Marked previously terminated long-running attempt as aborted in `job_run_status` for audit clarity.

### Entry 22 - Full Optimization Sweep + Hardening (2026-03-04)
- Conducted a full professional optimization audit across schema, runtime paths, storage footprint, and orchestration flow.

Changes implemented:
- `backend/db/cross_section_snapshot.py`
  - migrated physical key of `universe_cross_section_snapshot` to `(ric, as_of_date)`.
  - removed migration ordering bug where `idx_universe_cross_section_snapshot_ric` could be created before `ric` existed on legacy tables.
  - hard migration rebuild now deduplicates by `(ric, as_of_date)`.
- `backend/cuse4/schema.py` and `backend/barra/raw_cross_section_history.py`
  - enforced redundant index cleanup and migration artifact cleanup.
- `backend/routes/data.py`
  - diagnostics source table list is now canonical-only.
- `backend/db/model_outputs.py`
  - kept incremental relational write behavior (latest-date slice only).
- `backend/tests/test_audit_fixes.py`
  - added snapshot migration regression test; test suite now `8 passed`.

Live DB migration + validation:
- Applied canonical schema ensure on `backend/data.db`.
- Verified:
  - `security_master__legacy_pre_ric_pk` absent.
  - active `security_master` indexes exist on `ticker`, `permid`, `sid`.
  - redundant indexes removed (`idx_security_prices_eod_ric_date`, `idx_security_fundamentals_pit_ric_asof`, `idx_security_classification_pit_ric_asof`, `idx_barra_raw_cross_section_history_ric`).
  - `universe_cross_section_snapshot` PK now `ric, as_of_date`.

Performance/refresh checks:
- Snapshot rebuild benchmark (`mode=current`):
  - `rows_upserted=3019`, elapsed `~8.85s`.
- Weekly-core orchestration (`feature_build -> serving_refresh`):
  - completed `status=ok`.
  - stage timings from job rows:
    - feature_build: `~9.24s`
    - estu_audit: `~7.35s`
    - factor_returns: `~56.75s`
    - risk_model: `~4.96s`
    - serving_refresh: `~221.72s`
- Daily-fast orchestration (`feature_build -> serving_refresh`):
  - completed `status=ok`.
  - total wall time `~45.4s`.

Storage optimization:
- Ran compaction script:
  - `python3 backend/scripts/compact_sqlite_databases.py backend/data.db backend/cache.db`
- Reclaimed:
  - `data.db`: `1,471,516,672` bytes
  - `cache.db`: `8,192` bytes
- Integrity checks:
  - `PRAGMA quick_check` = `ok` for both DBs.

Documentation updates:
- Added audit report: `backend/OPTIMIZATION_AUDIT_2026-03-04.md`.
- Updated execution plan status in `user notes/cUSE4_Backend_Execution_Plan_2026-03-04.md`.

### Entry 23 - Universe Expansion Baseline Update + Monthly PIT Completion (2026-03-04)
- Expanded/merged coverage universe from `Coverage Universe -6K.xlsx` into canonical `security_master`.
- Current canonical universe baseline:
  - `security_master` total rows: `5,819`
  - eligible rows (`classification_ok=1`, `is_equity_eligible=1`): `5,819`
  - distinct tickers: `4,956`
  - distinct RICs: `5,819`
- Completed monthly PIT backfill for the full 5,819-RIC universe:
  - `security_fundamentals_pit`: `989,230` rows
  - `security_classification_pit`: `989,230` rows
  - date range: `2012-01-31` to `2026-02-27`
  - distinct RICs present in both tables: `5,819`
- Current prices state (unchanged by this PIT-only completion step):
  - `security_prices_eod`: `7,070,167` rows
  - distinct priced RICs: `2,675`
  - date range: `2012-01-03` to `2026-03-03`
