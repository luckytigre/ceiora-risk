# cPAR1 Math Kernel

Date: 2026-03-18
Status: Active slice-1 design and implementation notes
Owner: Codex

This document describes the pure `backend/cpar/*` math kernel only.

Related cPAR docs:
- [CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md)
- [CPAR_OPERATIONS_PLAYBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/operations/CPAR_OPERATIONS_PLAYBOOK.md)

It does not describe:
- persistence
- routes
- services
- orchestration
- frontend integration
- portfolio integration
- serving payloads

## Purpose

`cPAR1` is a small, fixed, returns-based model family that answers:

> What is driving this instrument, and how do I hedge it with real ETFs?

Slice 1 exists to prove the model math and the package boundary, not the app integration.

## Factor Registry

`cPAR1` uses a fixed factor registry.

Market:
- `SPY`

Sectors:
- `XLB`
- `XLC`
- `XLE`
- `XLF`
- `XLI`
- `XLK`
- `XLP`
- `XLRE`
- `XLU`
- `XLV`
- `XLY`

Styles:
- `MTUM`
- `VLUE`
- `QUAL`
- `USMV`
- `IWM`

The registry is owned in `backend/cpar/factor_registry.py`.
It is not sourced from cUSE4 factor catalogs.
These proxy ETFs are both the fixed cPAR basis and eligible modeled instruments in the current package build.
That means `SPY`, sector ETFs, and style ETFs can now receive persisted cPAR fits as single-name instruments.

## Weekly Anchors And Returns

Weekly anchors are XNYS week-ending anchors.

Rules:
- target the Friday of each calendar week
- if Friday is not a trading session, use the previous XNYS session
- use `53` weekly price anchors to produce `52` weekly returns
- for each weekly anchor, choose the latest eligible price on or before that anchor within the same Monday-Friday week
- prefer `adj_close`
- fall back to `close` only when `adj_close` is missing for that instrument and week

Weekly return:
- `r_t = P_t / P_(t-1) - 1`

Weights:
- exponential half-life = `26` weeks
- most recent week has age `0`

## Two-Step Fit With Package-Level Orthogonalization

### Step 1: Market Fit

Fit the instrument on raw weekly SPY returns:

`y_t = alpha_market + beta_market * m_t + eps_t`

This is weighted least squares with an intercept.

### Step 2: Package-Level Orthogonalization

For every non-market proxy ETF:

`x_f,t = a_f + b_f * m_t + u_f,t`

This is done once per package on the proxy panel, not once per ticker.

Stored transform pieces:
- proxy intercept `a_f`
- proxy market loading `b_f`
- orthogonalized residual series `u_f,t`

### Step 3: Post-Market Block

Use the market-step residual as the dependent variable:

`eps_t = alpha_block + Z_t * theta + eta_t`

Where:
- `Z_t` is the matrix of orthogonalized non-market proxy series
- regressors are weighted-standardized on the observed sample
- the intercept is included and never penalized

## Weighted Ridge And Thresholding

`cPAR1` uses ridge, not lasso.

Penalty constants:
- sectors = `4.0`
- styles = `8.0`

Thresholding happens only after the raw ETF trade-space vector has been recovered.

Rules:
- market is never thresholded
- non-market factors are thresholded at `abs(beta) < 0.05`
- exact boundary `0.05` is kept, not zeroed

## Raw ETF Trade-Space Back-Transform

The non-market raw trade-space coefficients are the de-standardized post-market coefficients.

SPY needs an adjustment because non-market raw proxy ETFs themselves contain market content.

If proxy `f` has package-level market loading `b_f`, then:

`beta_spy_trade = beta_market_step1 - Σ(beta_f_raw * b_f)`

The trade-space intercept is:

`alpha_trade = alpha_market + alpha_block - Σ(beta_f_raw * a_f)`

This is the coefficient vector used for:
- displayed cPAR loadings
- hedge construction
- post-hedge residual display

For proxy ETFs themselves:
- `SPY` naturally fits near pure `SPY`
- non-market proxy ETFs such as `XLK` or `IWM` are still governed by the same market-step, orthogonalized residual block, ridge, and back-transform pipeline
- they should not be hard-coded to identity vectors because their modeled loadings are defined in the same residualized cPAR trade space as every other fitted instrument

## Hedge Engine Rules

The hedge engine lives in `backend/cpar/hedge_engine.py`.

Supported modes:
- `market_neutral`
- `factor_neutral`

Rules:
- hedge directly in raw ETF trade space
- start from the thresholded raw ETF vector
- include SPY only if `abs(beta_spy_trade) >= 0.10`
- require complete covariance coverage for every factor pair used in pruning or variance display
- fail closed if the covariance surface is incomplete
- prune highly correlated substitutes when `abs(corr) > 0.90`
- keep the larger absolute exposure when pruning
- cap total hedge size at `5` ETFs including SPY
- drop any final hedge leg with `abs(weight) < 0.05`
- mark `hedge_degraded` if non-market gross reduction is below `50%`
- mark `hedge_unavailable` if fit status is `insufficient_history`

## Non-Goals For Slice 1

Slice 1 deliberately does not include:
- database tables
- Neon or SQLite persistence
- API routes
- services
- orchestration profiles or runtime lanes
- operator surfaces
- blob payload surfaces
- frontend pages
- holdings or portfolio integration
- what-if integration
- cUSE4 comparisons

The point of this slice is to prove:
- exact `cPAR1` math
- exact trade-space back-transform
- deterministic hedge behavior
- strict isolation of pure cPAR logic inside `backend/cpar/*`
