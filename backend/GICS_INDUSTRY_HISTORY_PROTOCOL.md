# GICS Industry History Protocol

## Goal
Ensure every cross-section (`ticker`, `as_of_date`) has a point-in-time industry group for factor regressions.

## Data Contract
- Table: `gics_industry_history`
- Grain: one row per (`ticker`, `as_of_date`)
- Primary key: (`ticker`, `as_of_date`)
- Required columns:
  - `ticker`
  - `as_of_date`
  - `gics_industry_group`
  - `trbc_economic_sector`
  - `source`
  - `job_run_id`
  - `updated_at`

## Operational Flow
1. Run historical backfill once (or when universe coverage changes):
   - `python backend/scripts/backfill_gics_history_lseg.py --db-path backend/data.db`
2. This script:
   - Upserts `gics_industry_history` from LSEG at each `barra_exposures.as_of_date`
   - Syncs `barra_exposures.gics_industry_group` from history (exact date + ticker-wise fill)
3. For ongoing daily updates, `download_data_lseg.py` now also writes current-day rows into `gics_industry_history`.
4. Recompute analytics cache:
   - `python -c "from analytics.pipeline import run_refresh; print(run_refresh())"` (from `backend/`)

## Modeling Behavior
- `daily_factor_returns.py` now prefers `gics_industry_history` by exact `as_of_date` when building each exposure snapshot.
- If history is absent for a date/ticker, it falls back to `barra_exposures.gics_industry_group`, then `Unmapped`.
