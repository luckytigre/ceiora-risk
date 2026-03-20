# cPAR Frontend Surfaces

Date: 2026-03-19
Status: Active cPAR frontend notes
Owner: Codex

This document describes the current cPAR frontend surfaces after the namespaced-family routing slice, with the existing account-scoped risk workflow still kept intentionally narrow.

Related cPAR docs:
- [CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md)
- [CPAR_OPERATIONS_PLAYBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/operations/CPAR_OPERATIONS_PLAYBOOK.md)

## Purpose

This slice expands and restructures the cPAR frontend without widening backend compute scope.

It does not add:
- cUSE4 vs cPAR comparison views
- route-triggered build behavior
- any shared cUSE4/cPAR truth layer
- any cPAR apply or mutation flow

## Page Structure

`/cpar/risk`
- current account-level cPAR risk workspace
- owns holdings-account selection, coverage summary, aggregate thresholded loadings, one account hedge preview, and one narrow read-only what-if preview
- now has a stable backend contract for the next rebuild stage:
  - `coverage_breakdown` for explicit exclusion buckets
  - `factor_variance_contributions` for factor-only decomposition of the aggregate thresholded portfolio vector
  - `positions[].thresholded_contributions` for per-position weighted contributions
- keeps the staged what-if builder cPAR-owned even when it visually conforms to the cUSE builder grammar
- preserves the same active-package search semantics as the other cPAR pages, including disabled `Ticker required` rows when a search hit cannot open or stage directly
- is the canonical route for that workflow now

`/cpar/explore`
- general cPAR discovery/detail page
- owns search, ticker selection, persisted fit detail, and loadings interpretation
- now also renders a small package-date-capped `source_context` block for the selected instrument
- links into `/cpar/hedge` for hedge-specific interaction
- uses the active package only
- now uses three cPAR-owned presentation modules:
  - one active-package search/typeahead module
  - one selected-instrument detail module that embeds supplemental source context
  - one persisted-loadings plus hedge-handoff module
- intentionally borrows cUSE-like layout rhythm and typeahead grammar without importing cUSE feature owners, cUSE hooks, or cUSE payload semantics

`/cpar/health`
- lightweight cPAR package diagnostics page
- shows the active package summary
- shows the fixed cPAR1 factor registry summary
- shows the warning/status legend
- provides a lightweight entry point into `/cpar/explore`
- is the canonical home for the old `/cpar` landing content

`/cpar/hedge`
- dedicated hedge workflow page
- owns search, ticker selection, persisted hedge-subject summary, hedge mode toggle, hedge preview, and post-hedge display
- links back to `/cpar/explore` for raw and thresholded loadings review
- uses the active package only

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
- one active-package persisted fit row
- may include a nested `source_context` block with supplemental shared-source context pinned to the active package date
- returns `409` when ticker is ambiguous and `ric` is required

`GET /api/cpar/ticker/{ticker}/hedge`
- persisted hedge preview only
- no request-time refit

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
- the frontend must treat `meta`, `ticker detail`, and `hedge` as one package-scoped flow
- the frontend must treat `meta` and the account-level hedge payload as one package-scoped flow
- the frontend must also treat the account-level what-if envelope plus its nested `current` and `hypothetical` payloads as part of that same package-scoped flow
- if those responses do not share the same `package_run_id` / `package_date`, the page must fail closed instead of mixing surfaces from different active packages
- the frontend now uses package metadata as the first gate for dependent reads, so package-level `not_ready` / `unavailable` states do not keep probing detail or account-risk endpoints on the same page load
- `/cpar/explore` enforces this for banner plus detail
- `/cpar/explore` must treat `source_context` as supplemental to the same ticker-detail payload, not as an independent truth source
- `/cpar/hedge` enforces this for banner, selected subject, and hedge preview
- `/cpar/risk` enforces this for banner, the baseline account hedge payload, and the what-if envelope/current/hypothetical payloads

## Current Frontend Owner Freeze

The current cPAR overhaul should keep page ownership inside cPAR-owned modules even when the presentation becomes more cUSE-like.

Current page owners:
- `/cpar/explore` stays owned by `frontend/src/app/cpar/explore/page.tsx` plus cPAR-owned components
- `/cpar/risk` stays owned by `frontend/src/features/cpar/components/CparRiskWorkspace.tsx`
- `/cpar/hedge` stays owned by `frontend/src/app/cpar/hedge/page.tsx`
- `/cpar/health` stays owned by `frontend/src/features/cpar/components/CparHealthWorkspace.tsx`

Allowed reuse direction:
- neutral visual primitives from `frontend/src/components/*`
- shared holdings/account widgets such as `InlineShareDraftEditor`
- shared layout rhythm, spacing, and interaction grammar already proven on cUSE pages

Disallowed reuse direction for this overhaul stage:
- cUSE feature owners under `frontend/src/features/cuse4/*`
- cUSE explore owners under `frontend/src/features/explore/*`
- cUSE what-if owners under `frontend/src/features/whatif/*`
- cUSE hooks or payload semantics through `@/hooks/useCuse4Api`, `@/lib/cuse4Api`, or `@/lib/types/cuse4`

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
- ticker ambiguity is rendered as a UI instruction to choose a specific RIC from search results
- search hits without a ticker render as non-navigable rows because the current detail route is ticker-keyed
- a direct `/cpar/explore?ric=...` visit without `ticker=` must render an explanatory warning rather than silently failing or synthesizing a detail request
- a direct `/cpar/hedge?ric=...` visit without `ticker=` must render the same explanatory warning because the current hedge route is also ticker-keyed
- package-identity drift between active-package reads must render an explicit reload prompt rather than mixing banner/detail/hedge data from different packages
- explore-level shared-source context degradation is non-blocking:
  - the page keeps rendering the persisted cPAR fit row
  - the nested `source_context.status` / `reason` fields explain whether shared-source context is complete, partial, missing, or temporarily unavailable
- `/cpar/risk` must render explicit empty or unavailable account states instead of synthesizing a hedge from unpriced or uncovered holdings rows
- `empty` means the selected account has no live holdings rows
- `unavailable` means the selected account has live holdings rows, but none are both priced and backed by a usable persisted cPAR fit in the active package

## Workflow Split

`/cpar/explore`
- remains the persisted fit discovery/detail surface
- keeps raw and thresholded loadings visible
- may show package-date source context for identity/classification/latest source price, but it still does not become a cUSE-style quote/history page in this slice
- now presents the selected instrument as one cPAR-owned detail module rather than a stack of small generic cards, but the page owner still keeps all package-truth, ambiguity, `insufficient_history`, and package-mismatch branching explicit
- does not own hedge mode switching or post-hedge interpretation anymore

`/cpar/hedge`
- remains the focused hedge workflow surface
- reuses persisted detail only to identify the selected subject and package
- owns hedge mode switching, hedge legs, and post-hedge interpretation

`/cpar/risk`
- remains a narrow account-level hedge workflow
- owns account selection, coverage/exclusion explanation, aggregate loadings, staged scenario rows, and current vs hypothetical account hedge preview
- Slice 4 deliberately stops at contract expansion:
  - the page does not yet render a full cUSE-style decomposition workspace
  - the new backend fields exist so Slice 5 can rebuild `/cpar/risk` without re-deriving portfolio contributions in the browser
- any richer risk charts or decomposition views must stay subordinate to that same account-scoped hedge + preview-only what-if workflow
- does not own portfolio mutation, account editing, trade application, or broad scenario analytics

`/cpar/health`
- remains the lightweight diagnostics page
- owns the package summary, registry, warning legend, and route-level orientation for the rest of the cPAR family
- does not become a full operator-status or maintenance dashboard in this slice

## Smoke Coverage

Current cPAR frontend smokes cover:
- `/cpar/health` and `/cpar/explore` baseline flow
- `/cpar/explore` rendering the supplemental source-context card when the ticker route returns it
- `/cpar/explore` rendering the rebuilt persisted-loadings module after a successful detail selection
- `/cpar/hedge` baseline flow
- `/cpar/risk` baseline flow
- `/cpar/risk` narrow what-if preview flow
- `not_ready`
- `unavailable`
- package mismatch
- `/cpar/risk` fail-closed branches for `not_ready`, `unavailable`, and package mismatch
- meta-first gating for detail/account reads when package-level `not_ready` or `unavailable` blocks the page
- family-route redirects for `/exposures`, `/explore`, `/health`, `/cpar`, and `/cpar/portfolio`

This slice does not add new `/cpar/risk` frontend smoke cases because the new contract fields are not yet rendered directly; they are pinned by backend tests and frontend type ownership instead.

## Deferred After This Slice

- frontend operator surfaces
- any shared cUSE4/cPAR comparison UI
- cPAR apply/mutation flows
- broader multi-account or portfolio-analytics cPAR views
- broader cPAR what-if expansion beyond the current narrow account-scoped preview

## Shared App Chrome

The global brand and background menu remain shared with the rest of the app.

The top header is now route-family aware:
- `/cuse*` shows `Risk`, `Explore`, `Health`, and shared `Positions`
- `/cpar*` shows `Risk`, `Explore`, `Health`, `Hedge`, and shared `Positions`
- `/` stays intentionally minimal and uses the centered family chooser instead of a second family subnav

The cUSE4 operator-status signal and `serve-refresh` control are intentionally suppressed on `/cpar*` routes so the first cPAR slice does not imply operator coupling that has not been implemented.
