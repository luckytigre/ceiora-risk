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
   - `python3 -m backend.scripts.backfill_trbc_history_lseg --db-path backend/data.db`
2. This script:
   - Resolves ticker->RIC using multi-suffix probing and caches in `ticker_ric_map`
   - Upserts `trbc_industry_history` from LSEG `TRBCIndustryGroup` at each `barra_raw_cross_section_history.as_of_date`
3. For ongoing daily updates, `download_data_lseg.py` also:
   - Reuses `ticker_ric_map` and refreshes unresolved mappings
   - Writes current-day TRBC industry rows into `trbc_industry_history`
4. Recompute analytics cache:
   - `python3 -c "from backend.analytics.pipeline import run_refresh; print(run_refresh())"` (from repo root)

## Modeling Behavior
- Structural eligibility is strict: a name is included only when style fields, market cap, TRBC sector, and TRBC industry are all present at that date.
- `daily_factor_returns.py` uses a minimum 7-day cross-section age (`date - 7d`) for regressions and does not use `Unmapped` fallback.
- `run_refresh()` updates loadings daily from latest snapshots but recomputes risk-engine internals (factor returns/covariance/specific risk) on a weekly cadence by default.
- `fundamental_snapshots.trbc_economic_sector_short` and `fundamental_snapshots.trbc_industry_group` are convenience copies for UI/search and should not be treated as the canonical PIT source for regressions.
