# cPAR Architecture And Operating Model

Date: 2026-03-18
Status: Active cPAR architecture baseline
Owner: Codex

This document is the canonical repo-level overview for the current cPAR implementation.

Use the slice-specific docs for detailed implementation notes:
- [CPAR1_MATH_KERNEL.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/CPAR1_MATH_KERNEL.md)
- [CPAR_PERSISTENCE_LAYER.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/CPAR_PERSISTENCE_LAYER.md)
- [CPAR_ORCHESTRATION.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/CPAR_ORCHESTRATION.md)
- [CPAR_BACKEND_READ_SURFACES.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/CPAR_BACKEND_READ_SURFACES.md)
- [CPAR_FRONTEND_SURFACES.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/CPAR_FRONTEND_SURFACES.md)
- [CPAR_OPERATIONS_PLAYBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/operations/CPAR_OPERATIONS_PLAYBOOK.md)

## Purpose

cPAR is a new parallel model family beside cUSE4.

It is not:
- an extension of `backend/risk_model/*`
- a reuse of cUSE4 serving payloads
- a reuse of cUSE4 runtime-state/operator surfaces
- a request-time fitting path

The current repo shape intentionally proves cPAR in layers:
- pure math kernel
- durable relational persistence
- dedicated package-build orchestration
- read-only backend surfaces
- focused frontend surfaces
- one narrow account-scoped what-if preview

## Ownership Boundaries

Pure cPAR domain/model logic lives in `backend/cpar/*`.

Integration code stays in the normal repo layers:
- `backend/data/*` owns durable relational `cpar_*` persistence
- `backend/orchestration/*` owns cPAR package-build workflows
- `backend/services/*` owns cPAR app-facing payload assembly
- `backend/api/routes/*` owns thin cPAR transport surfaces
- `frontend/*` owns cPAR page rendering and user-facing status/warning semantics

Within `backend/services/*`, the shared account-scoped portfolio snapshot assembly now lives in `cpar_portfolio_snapshot_service.py`.
That keeps `cpar_portfolio_hedge_service.py` and `cpar_portfolio_whatif_service.py` as separate application-facing flows without turning either one into a hidden utility owner.

Current boundary rules:
- `backend/cpar/*` does not import `backend.api`, `backend.services`, `backend.orchestration`, or `backend.data`
- cPAR routes do not import `backend.data` or `backend.cpar`
- cPAR services do not import API layers or orchestration layers
- cPAR does not reuse `serving_payload_current`
- cPAR does not write runtime-state keys in the current implementation

## Methodology Summary

`cPAR1` is a fixed, weekly, ETF-proxy model.

Core rules:
- fixed factor registry: `SPY`, sector ETFs, and style ETFs
- 53 XNYS weekly anchors producing 52 weekly returns
- market-first weighted fit with intercept
- package-level market orthogonalization for non-market proxies
- weighted ridge on the post-market block
- raw ETF trade-space back-transform before thresholding
- deterministic hedge generation in raw ETF trade space

The authoritative math contract lives in [CPAR1_MATH_KERNEL.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/CPAR1_MATH_KERNEL.md).

## Durable Surfaces

Durable relational tables:
- `cpar_package_runs`
- `cpar_proxy_returns_weekly`
- `cpar_proxy_transform_weekly`
- `cpar_factor_covariance_weekly`
- `cpar_instrument_fits_weekly`

Read-only backend routes:
- `GET /api/cpar/meta`
- `GET /api/cpar/search?q=&limit=`
- `GET /api/cpar/ticker/{ticker}?ric=`
- `GET /api/cpar/ticker/{ticker}/hedge?mode=&ric=`
- `GET /api/cpar/portfolio/hedge?account_id=&mode=`
- `POST /api/cpar/portfolio/whatif`

Frontend pages:
- `/cpar`
- `/cpar/explore`
- `/cpar/hedge`
- `/cpar/portfolio`

There is no cPAR blob-serving surface in the current implementation.
API payloads are assembled from authoritative relational `cpar_*` tables.

Frontend consistency rule:
- `/cpar/explore` must not mix package banners, detail rows, and hedge previews from different active packages
- `/cpar/hedge` must not mix package banners, subject rows, and hedge previews from different active packages
- if package identity drifts between independent reads, the page fails closed and prompts the user to reload
- shared banner rendering exposes package freshness so stale active packages remain visible without implying any route-triggered rebuild path

## Active-Package Semantics

The active package is the latest successful `cpar_package_runs` row that has the required child coverage for the requested read surface.

Current read behavior:
- metadata/search/detail use the active successful package
- hedge preview additionally requires complete covariance coverage
- account-level portfolio hedge additionally requires live holdings rows plus latest shared-source prices on or before the active package date
- account-level what-if additionally requires one account hedge baseline, one active package, and staged signed share deltas that reference either existing holdings rows or active-package search hits
- missing required relational coverage fails closed with cPAR-specific `503 not_ready`
- the account-level what-if envelope and its nested `current` / `hypothetical` snapshots are part of the same package-scoped flow as the shared banner and baseline portfolio hedge payload
- the frontend uses package metadata as the first gate for dependent reads and does not intentionally keep querying detail/hedge/account payloads after a package-level `not_ready` or `unavailable` response
- current package freshness is interpreted from the active package date/source-as-of date, not from any cUSE4 refresh/runtime-state surface

Current runtime authority:
- `local-ingest`: may build and persist packages
- `cloud-serve`: read-only, no build path

Current storage authority:
- Neon is primary when configured
- local SQLite is the mirror when Neon writes succeed
- local SQLite is the sole authority only in local development when Neon is not configured

## Status And Warning Semantics

Fit statuses:
- `ok`
- `limited_history`
- `insufficient_history`

Warnings:
- `continuity_gap`
- `ex_us_caution`

Hedge statuses:
- `hedge_ok`
- `hedge_degraded`
- `hedge_unavailable`

Current UI contract:
- `insufficient_history` blocks loadings and hedge display
- `limited_history` still renders persisted loadings and hedge output
- warnings are non-blocking badges layered on top of fit status

## Current Deferred Limits

The current cPAR implementation intentionally defers:
- cUSE4 vs cPAR comparison views
- runtime-state/operator dashboard integration
- route-triggered cPAR builds
- request-time cPAR fitting
- any reuse of cUSE4 serving payload surfaces
- broader portfolio analytics beyond the first narrow account-level hedge and what-if workflow
- any cPAR apply or mutation flow
- any broader multi-account or strategy-style cPAR what-if expansion

One current v1 limitation is explicit:
- search results may include persisted rows with `ticker = NULL`
- those rows remain visible in search
- they are not directly detail-addressable in the current ticker-keyed route contract
- the frontend must surface them as non-navigable and explain the limitation instead of silently hiding them
