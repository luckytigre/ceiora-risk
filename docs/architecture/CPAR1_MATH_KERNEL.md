# cPAR1 Math Kernel

Date: 2026-03-18
Status: Active slice-1 design and implementation notes
Owner: Codex

This document describes the pure `backend/cpar/*` math kernel only.

Related cPAR docs:
- [CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md)
- [CPAR_OPERATIONS_PLAYBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/operations/CPAR_OPERATIONS_PLAYBOOK.md)

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

## One-Shot Fit With Package-Level Orthogonalization

### Step 1: Package-Level Orthogonalization

For every non-market proxy ETF:

`x_f,t = a_f + b_f * m_t + u_f,t`

This is done once per package on the proxy panel, not once per ticker.

Stored transform pieces:
- proxy intercept `a_f`
- proxy market loading `b_f`
- orthogonalized residual series `u_f,t`

### Step 2: One-Shot Weekly Fit

Fit the instrument directly on:

`y_t = alpha + beta_market * m_t + Z_t * theta + eta_t`

Where:
- `Z_t` is the matrix of orthogonalized non-market proxy series
- the market column stays in raw weekly return space
- regressors are weighted-standardized on the observed sample
- the intercept and market term are never penalized
- the sector/style block is ridge-penalized jointly

## Weighted Ridge And Thresholding

`cPAR1` uses ridge, not lasso.

Penalty constants:
- sectors = `1.0`
- styles = `2.0`

Thresholding for cPAR risk/read surfaces happens in residualized factor space.

Rules:
- market is never thresholded
- non-market factors are thresholded at `abs(beta) < 0.05`
- exact boundary `0.05` is kept, not zeroed

## Residualized Read Space And Hedge Trade Space

The persisted cPAR fit now treats the one-shot residualized basis as the primary explanatory space.

That means:
- `market_step_beta` now carries the one-shot market coefficient
- `raw_loadings` and `thresholded_loadings` are residualized-space explanatory coefficients
- risk pages, explore detail, variance attribution, and covariance heatmaps should all read those residualized-space fields

The raw ETF hedge-space translation is still available when needed.

If proxy `f` has package-level market loading `b_f`, then the hedge-space SPY leg is:

`beta_spy_trade = beta_market - Σ(theta_f * b_f)`

and the raw ETF intercept is:

`alpha_trade = alpha - Σ(theta_f * a_f)`

That hedge-space vector should stay confined to hedge workflows and hedge previews.

For proxy ETFs themselves:
- `SPY` naturally fits near pure `SPY`
- non-market proxy ETFs such as `XLK` or `IWM` are still governed by the same market-step, orthogonalized residual block, ridge, and back-transform pipeline
- they should not be hard-coded to identity vectors because their modeled loadings are defined in the same residualized cPAR trade space as every other fitted instrument

## Specific Risk Proxy

`cPAR1_residual_v1` now persists a per-instrument specific-risk proxy alongside the residualized factor vector.

Definition:
- after the one-shot ridge fit is run on the observed weekly sample, keep the final weighted residual series `eta_t`
- compute weighted specific variance as the weighted residual variance on that observed sample
- specific volatility is the square root of that variance proxy

Operationally:
- `specific_variance_proxy = Var_w(eta_t)`
- `specific_volatility_proxy = sqrt(max(specific_variance_proxy, 0))`

This does not change hedge construction in this slice.
The hedge engine still optimizes factor risk in raw ETF trade space when that path is invoked.
The new specific-risk proxy is used by package-pinned risk, portfolio, and what-if read surfaces so those pages can report total variance and idiosyncratic share truthfully.

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
