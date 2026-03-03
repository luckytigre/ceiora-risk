# cUSE4 Backend Work Progress Log

## 2026-03-03

### Entry 01 - Audit and Orientation
- Read `cUSE4_engine_spec.md` end-to-end and captured locked decisions.
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
- Updated `OPERATIONS_PLAYBOOK.md` with new cUSE4 commands and cache key.

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
