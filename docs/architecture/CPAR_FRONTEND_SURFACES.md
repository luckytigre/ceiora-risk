# cPAR Frontend Surfaces

Date: 2026-03-20
Status: Active cPAR frontend notes
Owner: Codex

This document describes the current cPAR frontend surfaces after the namespaced-family routing slice and the later reset of the exploratory cPAR pages. `/cpar/risk` remains the active aggregate all-accounts cPAR risk surface.

Related cPAR docs:
- [CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md)
- [CPAR_OPERATIONS_PLAYBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/operations/CPAR_OPERATIONS_PLAYBOOK.md)

## Purpose

This slice expands and restructures the cPAR frontend without widening backend compute scope.

It does not add:
- cUSE4 vs cPAR comparison views
- route-triggered build behavior
- any shared cUSE4/cPAR truth layer
- any cPAR-native apply or mutation flow; `/cpar/explore` may still invoke shared holdings updates explicitly through the shared holdings owner

## Page Structure

`/cpar/risk`
- current aggregate-book cPAR risk workspace across all loaded holdings accounts
- owns a cPAR-native aggregate risk composition:
  - coverage summary plus explicit exclusion buckets
  - one signed factor-loadings chart with per-factor drilldown, reconciled from one aggregate book snapshot
  - one 5Y daily factor-history block inside each factor drilldown
  - positions contribution mix table derived from backend-owned row `risk_mix`
  - one full market/industry/style factor correlation heatmap from the package-pinned residualized display covariance surface
- now has a stable backend contract:
  - `coverage_breakdown` for explicit exclusion buckets
  - `aggregate_display_loadings`
  - `display_cov_matrix`
  - `display_factor_variance_contributions`
  - `display_factor_chart`
  - `risk_shares`
  - `factor_variance_proxy`
  - `idio_variance_proxy`
  - `total_variance_proxy`
  - `positions[].risk_mix`
- explanatory factor displays must use those display-basis fields, not hedge-trade-space fields
- is the canonical route for that workflow now

`/cpar/explore`
- current cPAR single-name detail and scenario-preview surface
- owns:
  - active-package search and ticker selection
  - persisted fit detail plus source-context augmentation
  - one explanatory factor-exposure chart built from `display_loadings`
  - one preview-only scenario builder and before/after exposure comparison
  - explicit shared holdings apply reuse through `frontend/src/hooks/useHoldingsApi.ts`, rather than a cPAR-owned backend mutation surface
  - explanatory single-name display must use residualized cPAR factor-space fields:
  - compatibility field `beta_market_step1` for the one-shot market coefficient
  - `display_loadings`
  - `raw_loadings`
  - `thresholded_loadings`
- hedge-trade-space interpretation remains valid only for hedge-specific consumers through `beta_spy_trade`

`/cpar/health`
- intentionally reset placeholder page
- no longer renders package diagnostics, registry summaries, or route-orientation content in the current repo state
- kept as a route placeholder so the cPAR family hierarchy remains stable while the page is rebuilt from the ground up

`/cpar/hedge`
- intentionally reset placeholder page
- no longer renders the single-name hedge workflow in the current repo state
- kept as a route placeholder so the cPAR family hierarchy remains stable while the page is rebuilt from the ground up

Legacy redirects:
- `/cpar` redirects to `/cpar/risk`
- `/cpar/portfolio` redirects to `/cpar/risk`

Shared shell behavior:
- `/` is now a minimal centered family chooser: `cUSE | cPAR`
- the top header no longer uses `cUSE` and `cPAR` as tabs
- on `/cpar*`, the top header promotes the cPAR-local pages directly: `Risk`, `Explore`, `Health`, `Hedge`, plus shared `Positions`

## Backend Contracts Used By The Frontend

`GET /api/cpar/meta`
- package metadata plus ordered factor registry
- includes the active package completion timestamp used for banner-level operational context

`GET /api/cpar/search`
- active-package search hits only

`GET /api/cpar/ticker/{ticker}`
- single-name persisted cPAR fit detail with source-context augmentation
- explanatory display uses residualized cPAR factor-space loadings plus compatibility field `beta_market_step1` for the one-shot market coefficient
- hedge-space interpretation remains explicit through `beta_spy_trade`

`GET /api/cpar/risk`
- aggregate all-accounts cPAR risk payload
- package-pinned and read-only
- does not reuse the account-scoped hedge payload as its frontend owner
- explanatory display must consume:
  - `aggregate_display_loadings`
  - `display_cov_matrix`
  - `display_factor_variance_contributions`
  - `display_factor_chart`
  - `positions[].display_contributions`

`GET /api/cpar/factors/history`
- supplemental 5Y factor-return history for cPAR drilldown
- market factor charts show cumulative daily raw return
- non-market factor charts support two cPAR-only drilldown modes:
  - `market_adjusted` = cumulative arithmetic `alpha + residual`
  - `residual` = cumulative arithmetic zero-mean residual
- the user-facing mode is controlled from `/settings` under the cPAR section
- cPAR-owned route/hook path, even though the charting primitive is shared
- fail-soft at the page level: `/cpar/risk` keeps rendering the aggregate risk payload when this history read is degraded

`POST /api/cpar/explore/whatif`
- preview-only cPAR explore comparison payload
- explanatory preview uses:
  - `current.display_exposure_modes`
  - `hypothetical.display_exposure_modes`
  - `diff.display_factor_deltas`
- hedge-basis `exposure_modes` / `factor_deltas` remain additive compatibility fields during the migration window

`GET /api/cpar/portfolio/hedge`
- account-scoped portfolio hedge preview only
- no request-time refit or build path
- uses the active cPAR package, live holdings rows, and latest shared-source prices on or before the package date
- now also exposes the exclusion buckets, factor-only variance decomposition, and per-position weighted thresholded contributions needed for a richer cPAR-native risk page

`POST /api/cpar/portfolio/whatif`
- account-scoped preview-only what-if payload
- uses the same active cPAR package plus staged signed share deltas
- returns current and hypothetical account hedge payloads side by side
- nested `current` and `hypothetical` snapshots carry the same risk-summary contract as the baseline hedge payload
- does not apply trades, mutate holdings, or build/refit cPAR on request

Page consistency rule:
- the frontend must treat `meta` and the aggregate `/api/cpar/risk` payload as one package-scoped flow
- the frontend must treat the account-level hedge payload as a separate account-scoped flow
- the frontend must also treat any account-level what-if envelope plus its nested `current` and `hypothetical` payloads as one package-scoped account flow
- if those responses do not share the same `package_run_id` / `package_date`, the page must fail closed instead of mixing surfaces from different active packages
- the frontend now uses package metadata as the first gate for dependent reads, so package-level `not_ready` / `unavailable` states do not keep probing detail or account-risk endpoints on the same page load
- `/cpar/risk` enforces this for banner plus the aggregate risk payload
- the current risk-page latency work does not change that runtime rule; it optimizes the backend path behind `/api/cpar/risk` while keeping the same meta-first frontend contract and full-page loading behavior
- drilldown factor history is supplemental to that page and may degrade without suppressing the aggregate risk payload

## Current Frontend Owner Freeze

The current cPAR overhaul should keep page ownership inside cPAR-owned modules even when the presentation becomes more cUSE-like.

Current page owners:
- `/cpar/explore` stays owned by `frontend/src/app/cpar/explore/page.tsx`
- `/cpar/risk` stays owned by `frontend/src/features/cpar/components/CparRiskWorkspace.tsx`
- `/cpar/hedge` stays owned by `frontend/src/app/cpar/hedge/page.tsx`
- `/cpar/health` stays owned by `frontend/src/app/cpar/health/page.tsx`
- `/positions` is not a cPAR page owner, but it may intentionally render a read-only cPAR method/coverage overlay from `frontend/src/hooks/useCparApi.ts`

Preferred cPAR frontend import surfaces now include:
- `frontend/src/hooks/useCparApi.ts` as the current cPAR-owned facade for route hooks
- `frontend/src/lib/cparApi.ts` as the current cPAR-owned facade for route-path helpers over the shared fetch transport
- `frontend/src/lib/types/cpar.ts` for cPAR route contracts
- `frontend/src/hooks/useHoldingsApi.ts` and `frontend/src/lib/holdingsApi.ts` only where cPAR intentionally reuses shared holdings/account plumbing
- `frontend/src/lib/types/holdings.ts` when cPAR intentionally reuses shared holdings/account types
- `frontend/src/lib/cparTruth.ts` for cPAR-specific warning/status/package-truth helpers

Allowed reuse direction:
- neutral visual primitives from `frontend/src/components/*`
- shared holdings/account widgets such as `InlineShareDraftEditor`
- shared holdings/account API owners `frontend/src/hooks/useHoldingsApi.ts` and `frontend/src/lib/holdingsApi.ts`
- shared layout rhythm, spacing, and interaction grammar already proven on cUSE pages

Disallowed direct reuse direction for this overhaul stage:
- cUSE feature owners under `frontend/src/features/cuse4/*`
- cUSE explore owners under `frontend/src/features/explore/*`
- cUSE what-if owners under `frontend/src/features/whatif/*`
- cUSE hooks or payload semantics through `@/hooks/useCuse4Api`, `@/lib/cuse4Api`, or `@/lib/types/cuse4`
- transitional mixed-family barrels through `@/hooks/useApi`, `@/lib/api`, or `@/lib/types` from cPAR feature owners or page components

Current boundary note:
- cPAR wrappers now own only cPAR route helpers and hooks over the neutral low-level transport in `frontend/src/lib/apiTransport.ts`
- when cPAR intentionally reuses shared holdings/account behavior, that reuse should stay explicit through `frontend/src/hooks/useHoldingsApi.ts` and `frontend/src/lib/holdingsApi.ts`, not through the mixed-family compatibility barrels

If a richer cPAR page still needs multiple backend requests, it must preserve the same package-identity checks described above.
If that becomes too brittle for one page, the next slice should move that page to a composite cPAR payload rather than mixing partially coherent reads in the browser.

## Status And Warning Rendering

Fit status:
- `ok`: green success badge
- `limited_history`: warning badge, but loadings and hedge stay visible
- `insufficient_history`: error badge, identity stays visible, loadings and hedge are blocked

Warnings:
- `continuity_gap`: non-blocking warning badge
- `ex_us_caution`: non-blocking warning badge

Read failures:
- cPAR-specific `503 not_ready` is rendered as a package-not-ready state
- cPAR-specific `503 unavailable` is rendered as an authority-unavailable state
- package freshness is rendered from the active package date/source-as-of date on the shared banner so stale packages remain obvious even when reads succeed
- `/cpar/risk` must render explicit empty or unavailable aggregate-book states instead of synthesizing a risk surface from unpriced or uncovered holdings rows
- `empty` means no live holdings rows are loaded across any account
- `unavailable` means live holdings rows exist across the active book, but none are both priced and backed by a usable persisted cPAR fit in the active package

## Workflow Split

`/cpar/explore`
- owns single-name detail, source-context, and preview-only scenario analysis
- should present residualized explanatory loadings, not hedge-trade-space loadings
- may hand staged deltas into the shared holdings apply surface, but that handoff is outside the cPAR route family

`/cpar/hedge`
- is intentionally blank aside from a reset placeholder
- no longer owns single-name hedge workflow behavior in the current repo state

`/cpar/risk`
- is now the aggregate cPAR risk analytics surface across all loaded holdings accounts
- owns one signed factor-loadings chart with per-factor drilldown, 5Y factor-return history, positions contribution mix, and the full factor correlation heatmap
- now intentionally borrows the cUSE risk-page layout rhythm without importing cUSE feature owners or cUSE payload semantics
- uses residualized cPAR loadings for explanatory charts and tables
- uses the residualized display covariance surface for the heatmap, drilldown metrics, and variance attribution
- does not display hedge-trade-space vectors outside hedge-specific surfaces
- now avoids a duplicate factor-summary table under the chart, leaving the signed chart plus drilldown as the primary factor read
- still stops short of a full cUSE-style analytics workspace:
  - no variance-attribution table
  - no separate idio drilldown table beyond the top-line risk decomposition and row-level risk mix
  - no cPAR-vs-cUSE comparison layer
  - no apply/mutation semantics
- any richer risk charts or decomposition views must stay subordinate to that same aggregate-book analytics workflow
- does not own portfolio mutation, account editing, trade application, or broad scenario analytics

`/cpar/health`
- is intentionally blank aside from a reset placeholder
- no longer owns package-summary or diagnostics behavior in the current repo state

## Smoke Coverage

Current cPAR frontend smokes cover:
- `/cpar/explore` single-name detail rendering
- `/cpar/hedge` placeholder rendering
- `/cpar/risk` baseline flow
- `/cpar/risk` signed factor-loadings chart plus drilldown, 5Y factor history, positions contribution mix, and full-factor correlation heatmap
- `/cpar/risk` fail-closed branches on the aggregate risk payload
- family-route redirects for `/exposures`, `/explore`, `/health`, `/cpar`, and `/cpar/portfolio`, with the legacy root redirects owned in `frontend/next.config.js` only

## Deferred After This Slice

- frontend operator surfaces
- rebuilt `/cpar/explore` surface
- rebuilt `/cpar/hedge` surface
- rebuilt `/cpar/health` surface
- any shared cUSE4/cPAR comparison UI
- cPAR-native apply/mutation flows beyond the explicit shared holdings apply reuse on `/cpar/explore`
- broader portfolio-analytics cPAR views beyond the current aggregate risk surface
- broader cPAR what-if expansion beyond the current narrow account-scoped preview route

## Shared App Chrome

The global brand and background menu remain shared with the rest of the app.

The top header is now route-family aware:
- `/cuse*` shows `Risk`, `Explore`, `Health`, and shared `Positions`
- `/cpar*` shows `Risk`, `Explore`, `Health`, `Hedge`, and shared `Positions`
- `/` stays intentionally minimal and uses the centered family chooser instead of a second family subnav
- the global menu also exposes `/settings`, which now owns the cPAR drilldown history-mode toggle

The cUSE4 operator-status signal and `serve-refresh` control are intentionally suppressed on `/cpar*` routes so the first cPAR slice does not imply operator coupling that has not been implemented.
