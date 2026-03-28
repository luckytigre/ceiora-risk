# cPAR Backend Read Surfaces

Date: 2026-03-20
Status: Active cPAR backend read and preview implementation notes
Owner: Codex

This document describes the cPAR service and API route layer for read-only and preview flows.

Related cPAR docs:
- [CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md)
- [CPAR_OPERATIONS_PLAYBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/operations/CPAR_OPERATIONS_PLAYBOOK.md)

## Purpose

This slice exposes the backend surfaces used by `/cpar/risk`, `/cpar/explore`, `/cpar/health`, and `/cpar/hedge`.

It does not add:
- frontend code
- route-triggered builds
- request-time fitting
- serving-payload persistence
- runtime-state keys
- operator-status integration

## Route Set

`GET /api/cpar/meta`
- returns active-package metadata plus the fixed cPAR1 factor registry
- includes package start/completion timestamps for read-surface freshness and operational context

`GET /api/cpar/search?q=&limit=`
- searches the active package’s persisted instrument-fit rows
- returns ticker, ric, display name, fit status, warnings, and country code
- may legitimately return rows with `ticker = NULL`

`GET /api/cpar/ticker/{ticker}`
- returns one active-package persisted cPAR fit plus source-context augmentation for the resolved ticker/ric
- now exposes residualized explanatory fit semantics:
  - `display_loadings`
  - compatibility field `beta_market_step1` carrying the one-shot market coefficient
- still preserves the explicit hedge-space market leg for hedge-specific consumers:
  - `beta_spy_trade`
  - `raw_loadings`
  - `thresholded_loadings`
- `raw_loadings` and `thresholded_loadings` are now residualized-space explanatory coefficients, not hedge-trade-space vectors
- hedge-space interpretation lives only through `beta_spy_trade` plus hedge-specific portfolio payloads

`GET /api/cpar/risk`
- returns the aggregate cPAR risk payload across all loaded holdings accounts
- reuses `backend/data/holdings_reads.py` for dedicated aggregate all-accounts holdings reads:
  - contributing accounts with live rows
  - netted aggregate positions across all accounts
- values those rows at the latest shared-source price on or before the active package date
- returns:
  - `aggregate_display_loadings`
  - `coverage_breakdown`
  - `display_cov_matrix`
  - `display_factor_variance_contributions`
  - `display_factor_chart`
  - `factor_variance_contributions`
  - `factor_chart`
  - `cov_matrix`
  - `positions[].display_contributions`
  - `positions[].thresholded_contributions`
- is read-only and package-pinned

`GET /api/cpar/factors/history?factor_id=&years=`
- returns supplemental cPAR factor-return history for drilldown use
- is gated by active cPAR package availability but reads daily proxy-price history for the cPAR factor instrument itself
- returns cumulative points only; it does not create a cUSE frontend dependency or a request-time compute path

`GET /api/cpar/portfolio/hedge?account_id=&mode=`
- returns a read-only account-scoped cPAR hedge workflow payload
- reuses `backend/data/holdings_reads.py` as the shared Neon-backed holdings/account adapter only as shared infrastructure
- values holdings rows at the latest shared-source price on or before the active package date
- aggregates only covered persisted cPAR thresholded loadings into one hedge vector
- reports `coverage_ratio` as covered gross market value divided by priced gross market value
- now also returns:
  - `aggregate_display_loadings` for explanatory charts and tables
  - `coverage_breakdown` for `covered`, `missing_price`, `missing_cpar_fit`, and `insufficient_history`
  - `display_factor_variance_contributions` derived from the aggregate display-loadings vector plus active-package covariance
  - `display_factor_chart` for explanatory factor charts and drilldown
  - top-level `factor_variance_contributions` derived from the aggregate thresholded loadings plus active-package covariance
  - per-position `display_contributions` for covered rows only
  - per-position `thresholded_contributions` for covered rows only
- supported `mode` values are `factor_neutral` and `market_neutral`

`POST /api/cpar/portfolio/whatif`
- returns a preview-only account-scoped cPAR what-if payload
- accepts `account_id`, `mode`, and `scenario_rows[{ric,ticker,quantity_delta}]`
- reuses the active package, the same account-level hedge baseline, and the shared `backend/data/holdings_reads.py` adapter
- stages signed share deltas only; it does not mutate holdings or apply trades
- additions outside the current holdings set must come from active-package search hits and therefore must be present in the active persisted cPAR fits
- response returns `scenario_rows`, `current`, `hypothetical`, and `_preview_only = true`
- the nested `current` and `hypothetical` snapshots carry the same additive hedge-space and display-space fields as the baseline hedge route

`POST /api/cpar/explore/whatif`
- returns the preview-only cPAR explore comparison envelope
- keeps the current/hypothetical what-if payload additive instead of silently redefining the older hedge-basis fields
- now exposes:
  - `current.display_exposure_modes`
  - `hypothetical.display_exposure_modes`
  - `diff.display_factor_deltas`
- preserves the existing hedge-basis `exposure_modes` and `factor_deltas` for compatibility while the explanatory pages migrate

## Read Authority

All cPAR read routes use the durable relational `cpar_*` tables through the cPAR data facade.
Each backend response pins one active `package_run_id` before reading dependent rows, so one payload does not silently mix active-package metadata from one package with fit or covariance reads from a later package.
The shared account-scoped snapshot assembly for the portfolio hedge and what-if flows now lives in `backend/services/cpar_portfolio_snapshot_service.py`, so the hedge and what-if services remain separate application-facing owners instead of calling one another’s internals.
The shared holdings/account dependency for those account-scoped routes lives below that service owner in `backend/data/holdings_reads.py`; cPAR portfolio flows do not reach sideways into cUSE4 holdings or what-if services.

These routes do not:
- read `serving_payload_current`
- trigger cPAR builds
- refit cPAR models on request

## Current Owner Freeze For The Risk/Explore Overhaul

The current cPAR overhaul should extend existing cPAR owners by default instead of introducing broad new route families up front.

Single-name owners:
- `GET /api/cpar/factors/history` is now owned by `backend/services/cpar_factor_history_service.py`
- `GET /api/cpar/ticker/{ticker}` remains owned by `backend/services/cpar_ticker_service.py`
- `POST /api/cpar/explore/whatif` remains owned by `backend/services/cpar_explore_whatif_service.py`

Aggregate risk owner:
- `GET /api/cpar/risk` is now owned by `backend/services/cpar_risk_service.py`
- this is the justified exception to the earlier account-scoped `/cpar/risk` freeze:
  - the user-facing page is now the aggregate all-accounts cPAR risk surface
  - the route-facing service stays thin and delegates aggregate package-pinned risk assembly to `backend/services/cpar_aggregate_risk_service.py`
  - that aggregate owner still reuses the shared package-pinned support rows and helper core in `backend/services/cpar_portfolio_snapshot_service.py` instead of inventing a second truth source
  - it does not collapse account-scoped hedge or what-if flows into the same payload

Account-scoped owners:
- `GET /api/cpar/portfolio/hedge` remains owned by `backend/services/cpar_portfolio_hedge_service.py`
- `POST /api/cpar/portfolio/whatif` remains owned by `backend/services/cpar_portfolio_whatif_service.py`
- shared lower assembly for both stays in `backend/services/cpar_portfolio_snapshot_service.py`
- aggregate current/hypothetical package-pinned snapshots reused by `POST /api/cpar/explore/whatif` now also flow through `backend/services/cpar_aggregate_risk_service.py`
- `backend/services/cpar_portfolio_snapshot_service.py` remains the shared lower support/core owner for all three flows
- the shared lower support/core now supports:
  - account-scoped hedge/what-if assembly
  - aggregate risk assembly for `/api/cpar/risk`
  - aggregate current/hypothetical snapshot assembly for `POST /api/cpar/explore/whatif`
- shared contract fields include:
  - `coverage_breakdown` remains the explicit bucketed exclusion summary
  - `factor_variance_contributions` remains a factor-only decomposition of the aggregate thresholded hedge vector
  - `display_factor_variance_contributions` is the explanatory factor-only decomposition of the aggregate display-loadings vector
  - `factor_chart[]` remains the hedge-basis signed chart contract
  - `display_factor_chart[]` packages the explanatory signed contribution arms plus per-factor drilldown rows used by `/cpar/risk`
  - `positions[].display_contributions` is the per-position explanatory contribution view derived from covered rows only
  - `positions[].thresholded_contributions` remains the per-position weighted contribution view derived from covered rows only

Current non-goals for this expansion stage:
- no reuse of cUSE4 dashboard, universe, or what-if service surfaces
- no generic `cpar_dashboard_*` or `cpar_risk_*` god service introduced only for symmetry
- no cUSE-style price-history surface in the ticker route during this slice

## Active-Package Semantics

Read routes use the active cPAR package selected by the cPAR persistence layer:
- latest successful package row
- complete child coverage required
- Neon-authoritative in `cloud-serve`
- local SQLite fallback only in local runtime roles

If no successful complete package exists, routes fail closed with `503` and a cPAR-specific `not_ready` payload.
If the active package is present but incomplete for a required relational surface, such as missing or partial covariance coverage for hedge preview, routes also fail closed with the same `503 not_ready` contract.

## Ambiguity And Failure Behavior

Ticker detail and hedge routes:
- return `409` when the active package contains multiple rows for the same ticker and `ric` is omitted
- return `404` when the requested ticker/ric is not present in the active package
- ticker detail resolves the persisted fit row first, then looks up any supplemental `source_context` by the resolved `ric` plus active `package_date`

Authority-read failures:
- return `503`
- do not fall back to route-local SQL or request-time recomputation
- the portfolio route maps only typed lower-layer infrastructure failures from holdings/source adapters into this `503 unavailable` contract
- unexpected service or data-shape defects are not intentionally swallowed as authority outages

Search-result limitations:
- the current detail route is ticker-keyed
- rows with `ticker = NULL` are therefore visible in search but not directly detail-addressable in v1
- the frontend must render that limitation explicitly instead of silently hiding those rows

Single-name source-context behavior:
- `source_context` is supplemental shared-source context, not package-produced fit output
- its common-name/classification/price lookups are capped at the active `package_date`
- shared-source augmentation is fail-soft for the ticker-detail route:
  - persisted cPAR fit detail still returns when those shared-source reads are degraded
  - the nested `source_context.status` / `reason` fields distinguish missing rows from shared-source unavailability
- this slice still does not add a 5Y price-history payload or a separate single-name history route

Portfolio-route limitations:
- the first portfolio workflow is account-scoped, not multi-account
- it reuses the shared `backend/data/holdings_reads.py` adapter but does not reuse cUSE4 portfolio or what-if payloads
- accounts with no live holdings rows return an explicit empty portfolio state instead of a synthesized hedge result
- accounts whose live holdings rows have no usable priced+cPAR-covered rows return an explicit unavailable portfolio state instead of a partial synthetic hedge
- the what-if preview remains preview-only and does not create an apply/mutation path
- staged additions must already exist in the active package; the route does not request-time fit new names
- account-scoped cPAR flows require two authorities at once: an active cPAR package and shared holdings/account reads; SQLite-only local cPAR package fallback does not make those routes available when holdings authority is down
- `coverage_breakdown.gross_market_value` is based only on positions that can be valued on or before the active package date, so `missing_price` rows correctly contribute `0.0`
- `factor_variance_contributions` remain factor-only proxy math from the persisted aggregate thresholded hedge vector plus active-package covariance
- `display_factor_variance_contributions` repeat the same factor-only proxy math on the explanatory display-loadings basis
- these baseline portfolio snapshots now also expose shared package-scoped `risk_shares`, variance proxies, and row `risk_mix`
- those fields remain derived read surfaces from the same package-pinned snapshot core, not a second risk engine or a cUSE-style risk truth source

Aggregate-risk limitations:
- `/api/cpar/risk` is all-accounts and read-only
- it is not an apply/mutation surface
- it does not embed account-scoped hedge preview or what-if comparison payloads
- it still depends on both authorities at once:
  - an active cPAR package
  - shared holdings/account and source-price reads
- `display_cov_matrix` is additive explanatory surface only:
  - it is derived read-time from the persisted package proxy-return panel plus persisted market-orthogonalization transforms
  - it stays package-pinned
  - it uses raw `SPY` returns and residualized non-market factor returns
  - it does not replace the persisted raw ETF `cov_matrix`, which remains the hedge-space covariance surface
- the current latency optimization keeps the route contract unchanged but changes the read path:
  - aggregate holdings rows are now netted in the shared holdings adapter instead of being aggregated in Python from all raw positions
  - display covariance and package/source support reads are fanned out concurrently on the request path
  - classification reads remain fail-soft while package, price, and covariance dependencies remain fail-closed

## Hedge Preview Behavior

The hedge route:
- uses persisted thresholded loadings for the requested fit row
- uses persisted covariance rows for the active package
- optionally derives stability diagnostics from the previous successful persisted fit and its covariance rows
- if the optional previous-package fit lookup or covariance read is unreadable, or if that previous covariance coverage is partial, the route still returns the current hedge preview and leaves stability metrics unset
- unexpected previous-package decode or hedge-construction errors are not swallowed; they still surface as real defects
- never refits the model on request

## Display-Loadings Behavior

Explanatory cPAR pages must not display hedge-trade-space loadings.

Current additive display contract:
- single-name detail exposes `display_loadings`
- aggregate/account snapshots expose:
  - `aggregate_display_loadings`
  - `display_factor_variance_contributions`
  - `display_factor_chart`
  - `positions[].display_contributions`
- explore what-if exposes:
  - `current.display_exposure_modes`
  - `hypothetical.display_exposure_modes`
  - `diff.display_factor_deltas`

Display-loadings rules:
- persisted fit rows now store residualized-space explanatory coefficients directly in `raw_loadings` and `thresholded_loadings`
- `display_loadings` is a presentation helper over that same residualized-space fit contract
- the only per-name hedge-trade-space escape hatch is `spy_trade_beta_raw`, surfaced to reads as `beta_spy_trade`
- hedge-trade-space fields such as `aggregate_thresholded_loadings`, account hedge previews, and `positions[].thresholded_contributions` remain valid only for hedge-specific workflows

## Explicit Deferred Items

This slice still does not include:
- cPAR operator surfaces
- cPAR runtime-state keys
- cPAR serving-payload surfaces
- any route-triggered build path
- any cPAR apply or mutation route
- any broader multi-account or strategy-style cPAR what-if route
