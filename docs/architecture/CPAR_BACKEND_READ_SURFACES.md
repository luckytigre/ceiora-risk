# cPAR Backend Read Surfaces

Date: 2026-03-18
Status: Active slice-4 read-only backend implementation notes
Owner: Codex

This document describes the read-only cPAR service and API route layer.

## Purpose

This slice exposes the minimal backend read surfaces needed for `/cpar` and `/cpar/explore`.

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

`GET /api/cpar/ticker/{ticker}?ric=`
- returns one active-package ticker detail payload
- if multiple active rows share the ticker and `ric` is omitted, returns `409`

`GET /api/cpar/ticker/{ticker}/hedge?mode=&ric=`
- returns a read-only hedge preview derived from persisted thresholded loadings and persisted covariance
- supported `mode` values are `factor_neutral` and `market_neutral`

## Read Authority

All cPAR read routes use the durable relational `cpar_*` tables through the cPAR data facade.

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

## Hedge Preview Behavior

The hedge route:
- uses persisted thresholded loadings for the requested fit row
- uses persisted covariance rows for the active package
- optionally derives stability diagnostics from the previous successful persisted fit and its covariance rows
- never refits the model on request

## Explicit Deferred Items

This slice still does not include:
- frontend `/cpar` pages
- cPAR operator surfaces
- cPAR runtime-state keys
- cPAR serving-payload surfaces
- any route-triggered build path
