# TRBC Industry History Protocol

## Goal
Ensure every cross-section (`ticker`, `as_of_date`) has a point-in-time LSEG TRBC industry classification for factor regressions.

## Data Contract
- Table: `trbc_industry_history`
- Grain: one row per (`ticker`, `as_of_date`)
- Primary key: (`ticker`, `as_of_date`)
- Source of truth: LSEG `TR.TRBCIndustryGroup` (stored in `trbc_industry_group`)
- Required columns:
  - `ticker`
  - `as_of_date`
  - `trbc_industry_group`
  - `trbc_economic_sector`
  - `source`
  - `job_run_id`
  - `updated_at`

## Operational Flow
1. Run historical backfill once (or when universe coverage changes):
   - `python backend/scripts/backfill_trbc_history_lseg.py --db-path backend/data.db`
2. This script:
   - Resolves ticker->RIC using multi-suffix probing and caches in `ticker_ric_map`
   - Upserts `trbc_industry_history` from LSEG `TRBCIndustryGroup` at each `barra_exposures.as_of_date`
   - Syncs `barra_exposures.trbc_industry_group` from history (exact date + ticker-wise fill)
3. For ongoing daily updates, `download_data_lseg.py` also:
   - Reuses `ticker_ric_map` and refreshes unresolved mappings
   - Writes current-day TRBC industry rows into `trbc_industry_history`
4. Recompute analytics cache:
   - `python -c "from analytics.pipeline import run_refresh; print(run_refresh())"` (from `backend/`)

## Modeling Behavior
- `daily_factor_returns.py` prefers `trbc_industry_history` by exact `as_of_date` when building each exposure snapshot.
- If history is absent for a date/ticker, it falls back to `barra_exposures.trbc_industry_group`, then `Unmapped`.
- `run_refresh()` loads fundamentals aligned to `exposures_asof` and overlays latest TRBC history up to that same date.
- `fundamental_snapshots.trbc_sector` and `fundamental_snapshots.trbc_industry_group` are convenience copies for UI/search and should not be treated as the canonical PIT source for regressions.
