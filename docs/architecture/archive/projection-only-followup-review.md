## Projection-Only Follow-Up Review

Date: 2026-03-17
Status: Active follow-up review
Owner: Codex

## Current Implementation

### Factor-return source

Projection-only loadings are currently computed in [projected_loadings.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/risk_model/projected_loadings.py).

- `_load_factor_returns_wide(...)` reads from legacy cache-era `daily_factor_returns` in `cache.db`.
- It does not read from durable `model_factor_returns_daily`.
- This is a source-of-truth mismatch with the current cUSE operating model, where active factor-return truth is the durable core package.

### Cadence boundary

Projection-only loadings are currently computed from the serving refresh path in [pipeline.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/analytics/pipeline.py).

- When universe loadings are rebuilt, `run_refresh(...)` loads projection-only RICs from `security_master`.
- It then calls `compute_projected_loadings(...)` before building `universe_loadings`.
- This means projection-only loadings can be recomputed during ordinary serving rebuilds.
- They are not currently frozen to the core package cadence.

### Persisted projected tables

`compute_projected_loadings(...)` persists rows into:

- `projected_instrument_loadings`
- `projected_instrument_meta`

However:

- those tables are not currently the primary serving read surface
- the serving path recomputes projected loadings in memory and injects them directly into `universe_loadings`
- persistence is currently secondary storage, not the authoritative serving source

### `projection_asof`

`projection_asof` currently means:

- the latest common date between the instrument return history and the factor-return history used in the OLS sample

It does **not** currently mean:

- the active durable core package date
- `core_state_through_date`

This makes projection date semantics weaker than the rest of the stable core package semantics.

### Missing projected outputs

Current missing/failure behavior is soft:

- if `compute_projected_loadings(...)` raises, `pipeline.py` logs a warning and continues
- if a projection-only instrument returns `status != "ok"`, `universe_loadings.py` skips injecting it
- the instrument can therefore quietly disappear from the projected universe payload instead of being surfaced explicitly as unavailable/degraded

### Current quality gates

Current projection quality gates are minimal:

- minimum observation count (`min_obs`, default `60`)
- non-empty price history
- successful numeric OLS solve

There are no current gates for:

- fit quality / minimum `r_squared`
- core-package date alignment
- coefficient stability
- outlier handling
- weighting / shrinkage

### Projection methodology

The current projection-only method is a simple rolling time-series OLS:

- instrument arithmetic returns from `security_prices_eod`
- regressed on cUSE factor-return series
- trailing lookback window (default `252` days)
- no intercept
- no weighting
- no shrinkage
- specific risk estimated as annualized residual variance

This is acceptable as a first-pass projection method, but the larger correctness issues are architectural:

1. wrong factor-return source surface
2. wrong cadence boundary
3. persisted outputs not used as the primary serving read surface
4. weak projection date semantics

## Conclusion

The current projection-only implementation already preserves the most important native/core separation:

- projection-only instruments remain outside native cUSE estimation
- they do not affect factor returns, covariance, or native specific-risk estimation

But the current path still behaves like a serving-time convenience computation instead of a core-bound derived artifact.

The next narrow corrective pass should therefore:

1. read factor returns from durable `model_factor_returns_daily`
2. compute/persist projected outputs on the core-package cadence only
3. make persisted projected tables the primary serving read surface
4. tie `projection_asof` to the active core package date
5. surface missing projected outputs explicitly instead of quietly omitting them
