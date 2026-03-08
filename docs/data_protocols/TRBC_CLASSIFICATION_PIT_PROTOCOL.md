# TRBC Classification PIT Protocol

## Status
- This replaces the old `trbc_industry_history` protocol.
- Old script `backend.scripts.backfill_trbc_history_lseg` is archived and not part of active operations.

## Goal
Ensure each (`ric`, `as_of_date`) used by the model has point-in-time TRBC classification from canonical tables.

## Canonical Data Contract
- Table: `security_classification_pit`
- Grain: one row per (`ric`, `as_of_date`)
- Primary key: (`ric`, `as_of_date`)
- Key columns used by modeling:
  - `trbc_economic_sector`
  - `trbc_business_sector` (L2; active Barra dummy-factor level)
  - `trbc_industry_group` (L3; retained for output/detail views)
  - `trbc_industry`
  - `trbc_activity`

## Operational Flow
1. Refresh canonical source tables:
   - `python3 -m backend.scripts.download_data_lseg --db-path backend/runtime/data.db`
2. Run orchestrated model refresh:
   - `python3 -m backend.scripts.run_model_pipeline --profile source-daily-plus-core-if-due`
3. Use `cold-core` only after structural history changes:
   - `python3 -m backend.scripts.run_model_pipeline --profile cold-core`

## Modeling Behavior
- Eligibility and regressions read TRBC PIT classes from `security_classification_pit`.
- Industry dummy factors are built at TRBC L2 (`trbc_business_sector`) in current modeling.
- Output payloads can still include both L2 and L3 labels for interpretability.
- No separate `ticker_ric_map` or standalone `trbc_industry_history` table is required in current runtime flow.
