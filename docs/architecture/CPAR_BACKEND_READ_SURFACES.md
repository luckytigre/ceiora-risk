# cPAR Backend Read Surfaces

Date: 2026-03-19
Status: Active slice-4 read-only backend implementation notes
Owner: Codex

This document describes the read-only cPAR service and API route layer.

Related cPAR docs:
- [CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md)
- [CPAR_OPERATIONS_PLAYBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/operations/CPAR_OPERATIONS_PLAYBOOK.md)

## Purpose

This slice exposes the backend read surfaces used by `/cpar`, `/cpar/explore`, `/cpar/hedge`, and `/cpar/portfolio`.

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

`GET /api/cpar/search?q=&limit=`
- searches the active package’s persisted instrument-fit rows
- returns ticker, ric, display name, fit status, warnings, and country code
- may legitimately return rows with `ticker = NULL`

`GET /api/cpar/ticker/{ticker}?ric=`
- returns one active-package ticker detail payload
- if multiple active rows share the ticker and `ric` is omitted, returns `409`

`GET /api/cpar/ticker/{ticker}/hedge?mode=&ric=`
- returns a read-only hedge preview derived from persisted thresholded loadings and persisted covariance
- supported `mode` values are `factor_neutral` and `market_neutral`

`GET /api/cpar/portfolio/hedge?account_id=&mode=`
- returns a read-only account-scoped cPAR hedge workflow payload
- reuses a lower-layer holdings read adapter only as shared infrastructure
- values holdings rows at the latest shared-source price on or before the active package date
- aggregates only covered persisted cPAR thresholded loadings into one hedge vector
- reports `coverage_ratio` as covered gross market value divided by priced gross market value
- supported `mode` values are `factor_neutral` and `market_neutral`

## Read Authority

All cPAR read routes use the durable relational `cpar_*` tables through the cPAR data facade.
Each backend response pins one active `package_run_id` before reading dependent rows, so one payload does not silently mix active-package metadata from one package with fit or covariance reads from a later package.

These routes do not:
- read `serving_payload_current`
- trigger cPAR builds
- refit cPAR models on request

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

Authority-read failures:
- return `503`
- do not fall back to route-local SQL or request-time recomputation

Search-result limitations:
- the current detail route is ticker-keyed
- rows with `ticker = NULL` are therefore visible in search but not directly detail-addressable in v1
- the frontend must render that limitation explicitly instead of silently hiding those rows

Portfolio-route limitations:
- the first portfolio workflow is account-scoped, not multi-account
- it reuses holdings/account reads but does not reuse cUSE4 portfolio or what-if payloads
- accounts with no live holdings rows return an explicit empty portfolio state instead of a synthesized hedge result
- accounts whose live holdings rows have no usable priced+cPAR-covered rows return an explicit unavailable portfolio state instead of a partial synthetic hedge

## Hedge Preview Behavior

The hedge route:
- uses persisted thresholded loadings for the requested fit row
- uses persisted covariance rows for the active package
- optionally derives stability diagnostics from the previous successful persisted fit and its covariance rows
- if the optional previous-package fit lookup or covariance read is unreadable, or if that previous covariance coverage is partial, the route still returns the current hedge preview and leaves stability metrics unset
- unexpected previous-package decode or hedge-construction errors are not swallowed; they still surface as real defects
- never refits the model on request

## Explicit Deferred Items

This slice still does not include:
- cPAR operator surfaces
- cPAR runtime-state keys
- cPAR serving-payload surfaces
- any route-triggered build path
- any cPAR mutation or what-if route
