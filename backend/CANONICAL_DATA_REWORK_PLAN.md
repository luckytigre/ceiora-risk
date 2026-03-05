# Canonical Data Rework Plan

Date: 2026-03-03
Owner: Codex

Latest baseline update (2026-03-04 ET):
- `security_master` now contains `5,819` eligible RICs (all `classification_ok=1` and `is_equity_eligible=1`).
- `security_fundamentals_pit` and `security_classification_pit` are monthly-complete for this universe through `2026-02-27`.

## Objective
Refactor ingest/backfill to a non-redundant relational model with:
- `security_master` (universe + identity only)
- `security_prices_eod` (canonical prices time-series)
- `security_fundamentals_pit` (canonical fundamentals time-series)
- `security_classification_pit` (canonical TRBC/country time-series)

Deprecated persisted tables to remove:
- `ticker_ric_map`
- `fundamental_snapshots`
- `trbc_industry_history`
- `prices_daily` (replaced by `security_prices_eod`)
- `fundamentals_history` (replaced by `security_fundamentals_pit`)
- `trbc_industry_country_history` (replaced by `security_classification_pit`)
- `universe_candidate_holdings`

## Execution Phases

### Phase 1 - Schema + Pipeline Refactor
- [x] Define target canonical table names and constraints.
- [x] Update cUSE4 schema constants to canonical names.
- [x] Update LSEG ingest to write directly to canonical time-series tables.
- [x] Remove runtime dependency on deprecated write paths.

### Phase 2 - Full Backfill (Clear + Reload)
- [x] Create migration/backfill runner with managed batching.
- [x] Clear target canonical time-series tables.
- [x] Backfill fundamentals (`security_fundamentals_pit`) from eligible `security_master` names.
- [x] Backfill classifications (`security_classification_pit`) from eligible `security_master` names.
- [x] Backfill prices (`security_prices_eod`) from eligible `security_master` names in manageable chunks.

### Phase 3 - Deprecation Cleanup
- [x] Drop deprecated tables.
- [x] Create compatibility views (read-only) for legacy readers where needed.
- [x] Verify no data duplication remains in persisted tables.

### Phase 4 - Validation
- [x] Row-count and coverage checks by date for all canonical time-series tables.
- [x] Validate ESTU build on latest backfilled date.
- [x] Confirm identity mapping sourced only from `security_master`.

## Progress Log

### 2026-03-03 18:10 ET
- Plan initialized.
- Current baseline verified:
  - `security_master`: 2,871 names, all equity-eligible.
  - `prices_daily`: 7,300,863 rows across 2,555 dates.
  - `fundamental_snapshots`: 11,484 rows (4 as-of dates).
  - `trbc_industry_history`: 11,484 rows (4 as-of dates).

### 2026-03-03 18:38 ET
- Refactored schema constants to canonical table names:
  - `security_fundamentals_pit`
  - `security_classification_pit`
  - `security_prices_eod`
- Rewrote `download_data_lseg.py` to ingest directly into canonical SID-keyed tables from `security_master`.
- Added `backend/scripts/migrate_to_canonical_timeseries.py`:
  - clears canonical time-series tables,
  - backfills in date chunks (`--date-chunk-size`),
  - drops deprecated physical tables,
  - recreates legacy names as read-only compatibility views.
- Executed full canonical backfill with cleanup:
  - `fundamentals_rows_backfilled`: 11,484
  - `classification_rows_backfilled`: 11,484
  - `prices_rows_backfilled`: 4,967,121
- Post-migration canonical row counts:
  - `security_master`: 2,871
  - `security_fundamentals_pit`: 11,484
  - `security_classification_pit`: 11,484
  - `security_prices_eod`: 4,967,121
- Validation:
  - Duplicate key checks: zero across all canonical time-series tables.
  - ESTU run successful for `2026-02-27` with `estu_count=1930`.

### 2026-03-03 22:45 ET
- Extended canonical PIT history backward to `2012-03-30` through `2016-12-30` for full universe (2,871 SIDs):
  - `security_fundamentals_pit`: now 66,033 rows (23 as-of snapshots x 2,871)
  - `security_classification_pit`: now 66,033 rows (23 as-of snapshots x 2,871)
- Extended daily price history backward to `2012-01-03`.
- Identified and fixed price backfill script safety bug:
  - previous version could globally delete rows `> end_date` during a range run.
  - patched `backfill_prices_range_lseg.py` to hard-filter returned rows to requested bounds and remove global truncation delete.
- Recovered accidentally truncated post-2016 segment by direct LSEG re-pull in manageable windows:
  - recovery window: `2016-02-13` to `2026-03-03`
  - `rows_upserted`: 5,398,488
  - `failed_batches`: 0
- Final post-recovery prices status:
  - `security_prices_eod`: 6,847,526 rows
  - distinct SIDs: 2,871
  - date range: `2012-01-03` to `2026-03-03`

### 2026-03-03 23:35 ET
- Extended coverage universe from `/Users/shaun/Downloads/Derived Holdings 2026-03-03.xlsx`:
  - workbook unique RICs: `365`
  - newly added to canonical `security_master`: `148`
  - total eligible universe: `3,019`
- Implemented targeted RIC-subset backfill controls:
  - `download_data_lseg.py` supports `--rics`
  - `backfill_pit_history_lseg.py` supports `--rics`
  - `backfill_prices_range_lseg.py` supports `--rics`
- Backfilled newly added RIC subset to align with existing historical coverage:
  - PIT snapshots: all `23` canonical dates backfilled successfully
  - prices: `2012-01-03` to `2026-03-03` for added subset
  - price subset upserts: `359,331` (0 failed batches)

### 2026-03-04 01:20 ET
- Refactored analytics read paths to canonical source tables (no legacy-view dependency in primary path):
  - `backend/db/postgres.py` now loads from:
    - `security_fundamentals_pit` + `security_classification_pit` + `security_master`
    - `security_prices_eod` + `security_master`
  - legacy compatibility views remain only as fallback.
- Refactored factor cross-section builder input sources:
  - `backend/barra/raw_cross_section_history.py` now reads canonical:
    - prices from `security_prices_eod`
    - fundamentals from `security_fundamentals_pit`
    - classification from `security_classification_pit`
    - ticker identity via `security_master`.
- Refactored eligibility panel loaders to canonical tables first:
  - `backend/barra/eligibility.py` now sources market cap and TRBC panels from canonical PIT tables and uses snapshot table only as fallback.
- Implemented current-snapshot materialization policy for `universe_cross_section_snapshot`:
  - `backend/db/cross_section_snapshot.py` supports:
    - `mode=current` (default): latest row per eligible ticker only
    - `mode=full`: historical rows by `(ticker, as_of_date)`
  - base universe is constrained to `security_master` eligible names (`classification_ok=1`, `is_equity_eligible=1`).
- Wired policy into refresh pipeline:
  - `backend/config.py`: `CROSS_SECTION_SNAPSHOT_MODE` env (`current` default).
  - `backend/analytics/pipeline.py`: passes configured mode into snapshot rebuild.
- Updated diagnostics payload to foreground canonical source tables:
  - `security_fundamentals_pit`, `security_classification_pit`, `security_prices_eod`.
