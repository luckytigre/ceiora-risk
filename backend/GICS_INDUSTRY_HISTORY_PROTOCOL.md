# GICS Industry History Protocol

## Goal
Ensure every cross-section (`ticker`, `as_of_date`) has a point-in-time LSEG industry classification for factor regressions.

## Data Contract
- Table: `gics_industry_history`
- Grain: one row per (`ticker`, `as_of_date`)
- Primary key: (`ticker`, `as_of_date`)
- Source of truth: LSEG `TR.TRBCIndustryGroup` (stored in `gics_industry_group` column for backward compatibility)
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
   - Resolves ticker->RIC using multi-suffix probing and caches in `ticker_ric_map`
   - Upserts `gics_industry_history` from LSEG `TRBCIndustryGroup` at each `barra_exposures.as_of_date`
   - Syncs `barra_exposures.gics_industry_group` from history (exact date + ticker-wise fill)
3. For ongoing daily updates, `download_data_lseg.py` also:
   - Reuses `ticker_ric_map` and refreshes unresolved mappings
   - Writes current-day TRBC industry rows into `gics_industry_history`
4. Recompute analytics cache:
   - `python -c "from analytics.pipeline import run_refresh; print(run_refresh())"` (from `backend/`)

## Modeling Behavior
- `daily_factor_returns.py` now prefers `gics_industry_history` by exact `as_of_date` when building each exposure snapshot.
- If history is absent for a date/ticker, it falls back to `barra_exposures.gics_industry_group`, then `Unmapped`.
