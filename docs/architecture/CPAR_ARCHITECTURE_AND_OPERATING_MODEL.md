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
- package-level market orthogonalization for non-market proxies
- one-shot weighted ridge on `market + residualized sector/style block`
- residualized factor-space persistence for cPAR risk/read surfaces
- raw ETF trade-space translation retained for hedge workflows
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
- `GET /api/cpar/risk`
- `GET /api/cpar/factors/history?factor_id=&years=`
- `GET /api/cpar/portfolio/hedge?account_id=&mode=`
- `POST /api/cpar/portfolio/whatif`

Frontend pages:
- `/cpar/risk`
- `/cpar/explore`
- `/cpar/health`
- `/cpar/hedge`
- legacy redirects from `/cpar` and `/cpar/portfolio` into `/cpar/risk`

There is no cPAR blob-serving surface in the current implementation.
API payloads are assembled from authoritative relational `cpar_*` tables.

Frontend consistency rule:
- `/cpar/risk` must not mix package banners and aggregate risk payloads from different active packages
- if package identity drifts between independent reads, the page fails closed and prompts the user to reload
- shared banner rendering exposes package freshness so stale active packages remain visible without implying any route-triggered rebuild path
- factor-history drilldown data is supplemental; if that read degrades, `/cpar/risk` still renders the aggregate package-pinned risk payload
- cPAR factor drilldown history is daily and arithmetic-cumulative
- `SPY` remains the raw market series
- non-market factors support two drilldown-only presentation modes over the same intercept-including daily regression:
  - `market_adjusted`: chart `alpha + residual = factor_return - beta * market_return`
  - `residual`: chart the pure zero-mean residual
- the mode toggle lives in the global `/settings` page under the cPAR section and does not change the package fit itself

## Risk And Explore Expansion Guardrails

The next cPAR frontend overhaul should remain cPAR-native even when it adopts cUSE-like presentation patterns.

Current owner decisions:
- rebuilt `/cpar/explore`, `/cpar/hedge`, and `/cpar/health` surfaces should remain cPAR-owned when they return
- `/cpar/risk` is now the explicit aggregate all-accounts cPAR risk owner:
  - route: `GET /api/cpar/risk`
  - route-facing service: `backend/services/cpar_risk_service.py`
  - aggregate snapshot owner: `backend/services/cpar_aggregate_risk_service.py`
  - shared lower support/core: `backend/services/cpar_portfolio_snapshot_service.py`
  - shared lower data adapters: `backend/data/holdings_reads.py` for aggregate holdings rows and `backend/data/cpar_outputs.py` / `backend/data/cpar_source_reads.py` for package/source support reads
- `backend/services/cpar_portfolio_snapshot_service.py` remains the shared lower support/core owner for account-scoped cPAR reads and the helper layer reused by `backend/services/cpar_aggregate_risk_service.py`
- aggregate current/hypothetical snapshots for `POST /api/cpar/explore/whatif` now also reuse `backend/services/cpar_aggregate_risk_service.py` directly rather than routing back through the snapshot-service compatibility alias
- the aggregate risk owner exposes:
  - `coverage_breakdown`
  - `factor_variance_contributions`
  - `factor_chart`
  - `positions[].thresholded_contributions`
  - `cov_matrix`
- account-scoped hedge and what-if owners remain separate:
  - `GET /api/cpar/portfolio/hedge`
  - `POST /api/cpar/portfolio/whatif`
- this does not create a generic model-family dashboard service; it is one explicit cPAR aggregate-risk owner for the user-facing `/cpar/risk` page

Current frontend boundary decision:
- cPAR pages may reuse neutral shared components and shared holdings widgets
- cPAR pages must not take ownership from `frontend/src/features/cuse4/*`, `frontend/src/features/explore/*`, or `frontend/src/features/whatif/*`
- a visual match to cUSE is acceptable; inheriting cUSE hooks, payload contracts, or apply semantics is not
- Slice 6 hardens the frontend import boundary as well:
  - cPAR-owned frontend files now prefer `frontend/src/hooks/useCparApi.ts` and `frontend/src/lib/cparApi.ts`
  - intentionally shared holdings/account plumbing now lives in `frontend/src/hooks/useHoldingsApi.ts` and `frontend/src/lib/holdingsApi.ts`
  - low-level fetch/error transport now lives in `frontend/src/lib/apiTransport.ts`
  - cPAR contracts now come from `frontend/src/lib/types/cpar.ts` plus shared `frontend/src/lib/types/holdings.ts` where account plumbing is intentionally reused
  - mixed-family compatibility barrels remain in the repo, but they are no longer the preferred import path for cPAR-owned frontend files or the owner of shared holdings/account reuse

Current package-truth decision:
- a richer cPAR page may continue to compose multiple requests only while it preserves one `package_run_id` / `package_date` across the full page
- if a richer page cannot do that cleanly, the next slice should introduce a composite cPAR payload rather than weaken fail-closed behavior
- the risk fields remain package-scoped for the same reason:
  - they are derived only from the active package, shared-source prices capped at the package date, and the aggregate live holdings snapshot
  - they do not introduce a second risk truth source beside the account-scoped hedge/what-if payloads

## Active-Package Semantics

The active package is the latest successful `cpar_package_runs` row that has the required child coverage for the requested read surface.

Current read behavior:
- metadata/search/detail use the active successful package
- aggregate risk, account hedge baselines, and cPAR what-if previews additionally require the residualized idio-capable method version `cPAR1_residual_v1`
- older factor-only packages are treated as `not_ready` for those surfaces until a fresh cPAR package is built
- aggregate risk additionally requires live holdings rows across all accounts plus latest shared-source prices on or before the active package date
- hedge preview additionally requires complete covariance coverage
- account-level portfolio hedge additionally requires live holdings rows plus latest shared-source prices on or before the active package date
- the shared snapshot assembly now also exposes:
  - explicit coverage buckets
  - total variance decomposition from aggregate residualized thresholded vectors plus residualized explanatory covariance and per-instrument specific-risk proxies
  - additive display-loadings analytics from aggregate residualized display vectors plus residualized explanatory covariance
  - hedge-basis factor-chart rows plus additive display-basis factor-chart rows
  - per-position weighted thresholded contributions plus additive display contributions
  - backend-owned `risk_shares`, `factor_variance_proxy`, `idio_variance_proxy`, `total_variance_proxy`, and row `risk_mix`
- `/api/cpar/risk` additionally exposes the full package-pinned covariance matrix for the frontend heatmap
- the current implementation keeps the frontend meta-first gate intact, but shortens the backend risk path by:
  - reading pre-aggregated all-account holdings rows from the shared holdings adapter
  - fanning out independent package/source/display-covariance reads concurrently after the aggregate book is known
- factor drilldown history now has a cPAR-owned supplemental route:
  - `GET /api/cpar/factors/history`
  - backed by daily proxy-price history with fresh daily market residualization over the displayed window for non-market factors
  - degradeable without suppressing the primary aggregate risk payload
- account-level what-if additionally requires one account hedge baseline, one active package, and staged signed share deltas that reference either existing holdings rows or active-package search hits
- aggregate explore what-if additionally reuses the same package-pinned aggregate snapshot core for both current and hypothetical comparison states
- missing required relational coverage fails closed with cPAR-specific `503 not_ready`
- the account-level what-if envelope and its nested `current` / `hypothetical` snapshots are part of the same package-scoped flow as the shared banner and baseline portfolio hedge payload
- the frontend may keep rendering the incumbent baseline hedge while staged what-if rows are invalid, recomputing, or fail closed, but it must not promote a hypothetical comparison panel unless the what-if envelope and both nested snapshots share the same package identity
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

Current display-vs-hedge rule:
- explanatory cPAR pages must use display-loadings surfaces
- hedge pages must use hedge-trade-space surfaces
- single-name detail therefore distinguishes:
  - compatibility field `beta_market_step1` for the one-shot market coefficient, plus `display_loadings`, `raw_loadings`, and `thresholded_loadings` for explanatory residualized display
  - `beta_spy_trade` for hedge-space interpretation

## Current Deferred Limits

The current cPAR implementation intentionally defers:
- cUSE4 vs cPAR comparison views
- runtime-state/operator dashboard integration
- route-triggered cPAR builds
- request-time cPAR fitting
- any reuse of cUSE4 serving payload surfaces
- broader portfolio analytics beyond the current aggregate risk surface plus the narrow account-level hedge and what-if workflows
- any cPAR-native apply or mutation flow beyond the explicit shared holdings apply reuse currently exposed from `/cpar/explore`
- any broader multi-account or strategy-style cPAR what-if expansion

One current v1 limitation is explicit:
- search results may include persisted rows with `ticker = NULL`
- those rows remain visible in search
- they are not directly detail-addressable in the current ticker-keyed route contract
- the frontend must surface them as non-navigable and explain the limitation instead of silently hiding them
