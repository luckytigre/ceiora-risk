# Projection-Only Universe: Current System Investigation

## Overview

This document is the initial broad investigation for projection-only support.

For the current implementation review and the core-package follow-up corrections, see:

- [projection-only-followup-review.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/archive/projection-only-followup-review.md)
- [projection-only-universe-target.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/archive/projection-only-universe-target.md)

This document summarizes how the cUSE4 system currently handles instrument eligibility, and identifies clean insertion points for supporting **projection-only** instruments (e.g., ETFs like SPY, XLE, XLF).

## Current Universe Flow

### 1. Security Master (`security_master` table)

The canonical instrument registry, keyed by RIC. Each row carries two boolean flags:

- **`classification_ok`** — set to 1 when LSEG TRBC classification data has been received
- **`is_equity_eligible`** — set to 1 when the instrument passes the equity filter (classification OK *and* TRBC economic sector is not in `NON_EQUITY_ECONOMIC_SECTORS`)

These flags are derived in `security_master_sync.derive_security_master_flags()` and enforced by `eligibility.NON_EQUITY_ECONOMIC_SECTORS = {"Exchange Traded Fund", "Digital Asset"}`.

### 2. Universe Resolution

`load_default_source_universe_rows()` in `security_master_sync.py` filters to instruments with `classification_ok=1 AND is_equity_eligible=1`. This is the canonical equity universe used by:

- **LSEG ingest** (`download_data_lseg.py`) — fetches prices, fundamentals, and classification
- **Price backfill** (`backfill_prices_range_lseg.py`) — extends price history backward
- **Raw cross-section history** (`raw_cross_section_history.py`) — builds factor descriptor panels
- **ESTU membership** (`estu.py`) — constructs the estimation universe

### 3. Core Model Path

The core model path is a strict pipeline:

1. `barra_raw_cross_section_history` — descriptor snapshots for eligible equities only
2. `estu_membership_daily` — estimation universe (requires `is_equity_eligible=1`)
3. `daily_factor_returns` — cross-sectional WLS regression on ESTU members
4. `model_factor_covariance_daily` — factor covariance from factor return history
5. `model_specific_risk_daily` — idiosyncratic risk from regression residuals

ETFs (TRBC sector "Exchange Traded Fund") are excluded at step 1 by the `NON_EQUITY_ECONOMIC_SECTORS` filter.

### 4. Serving Path

`build_universe_ticker_loadings()` in `universe_loadings.py` builds the full-universe payload from:
- Raw cross-section exposures (only has eligible equities)
- Fundamentals (market cap, TRBC)
- Prices
- Covariance matrix
- Specific risk

The `model_status` field already supports `"projected_only"` as a value, derived in `model_status.derive_model_status()`.

## Insertion Points

### A. Schema: `security_master`
A new column `coverage_role` can distinguish native equities from projection-only instruments without altering the existing `classification_ok`/`is_equity_eligible` flags that enforce core model exclusion.

### B. Ingest: `download_data_lseg.py`
The `_load_universe_from_security_master()` function delegates to `load_default_source_universe_rows()` when no explicit RICs are provided. A separate pass for projection-only instruments (price fields only) can be added without modifying the native equity pass.

### C. Price backfill: `backfill_prices_range_lseg.py`
Default universe comes from `load_default_source_universe_rows()`. Switching to a union that includes projection-only RICs is a one-line change.

### D. Core model: No changes needed
All core paths already exclude non-equity instruments via `classification_ok=1 AND is_equity_eligible=1`.

### E. Projection computation: New module
A new `projected_loadings.py` in `backend/risk_model/` can compute factor exposures via time-series OLS regression of ETF returns on existing `daily_factor_returns`.

### F. Serving: `universe_loadings.py`
`build_universe_ticker_loadings()` can accept an optional `projected_loadings` parameter and inject projected instruments into the `universe_by_ticker` dict after the native equity loop.

### G. Pipeline: `pipeline.py`
`run_refresh()` can call the projection builder after the risk engine stage and before the universe loadings build.

### H. Neon sync: `neon_stage2.py`
New tables (`projected_instrument_loadings`, `projected_instrument_meta`) need registration in `TABLE_CONFIGS`.

## Risk Assessment

- **No impact on core model**: Projection-only instruments have `is_equity_eligible=0`, which is the existing exclusion mechanism
- **Additive only**: All changes are additive — new column, new functions, new module, new tables
- **Backward compatible**: Existing rows default to `coverage_role='native_equity'`; existing queries are unaffected
