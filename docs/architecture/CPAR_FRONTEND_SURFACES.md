# cPAR Frontend Surfaces

Date: 2026-03-19
Status: Active slice-6 frontend notes
Owner: Codex

This document describes the current cPAR frontend surfaces after the first narrow portfolio hedge slice.

Related cPAR docs:
- [CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md)
- [CPAR_OPERATIONS_PLAYBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/operations/CPAR_OPERATIONS_PLAYBOOK.md)

## Purpose

This slice expands the read-only cPAR frontend without widening backend compute scope.

It does not add:
- cUSE4 vs cPAR comparison views
- route-triggered build behavior
- any shared cUSE4/cPAR truth layer
- any cPAR what-if or mutation flow

## Page Structure

`/cpar`
- lightweight landing page
- shows the active cPAR package summary
- shows the fixed cPAR1 factor registry summary
- shows the warning/status legend
- provides the search entry point into explore

`/cpar/explore`
- general cPAR discovery/detail page
- owns search, ticker selection, persisted fit detail, and loadings interpretation
- links into `/cpar/hedge` for hedge-specific interaction
- uses the active package only

`/cpar/hedge`
- dedicated hedge workflow page
- owns search, ticker selection, persisted hedge-subject summary, hedge mode toggle, hedge preview, and post-hedge display
- links back to `/cpar/explore` for raw and thresholded loadings review
- uses the active package only

`/cpar/portfolio`
- first narrow account-level cPAR hedge workflow
- owns holdings-account selection, account coverage summary, aggregate thresholded loadings, and one portfolio hedge preview
- reuses holdings/account reads only as shared plumbing
- does not reuse cUSE4 portfolio or what-if semantics
- uses the active package only
- treats `coverage_ratio` as covered gross over priced gross, not as a promise that every holdings row has a known market value

## Backend Contracts Used By The Frontend

`GET /api/cpar/meta`
- package metadata plus ordered factor registry

`GET /api/cpar/search`
- active-package search hits only

`GET /api/cpar/ticker/{ticker}`
- one active-package persisted fit row
- returns `409` when ticker is ambiguous and `ric` is required

`GET /api/cpar/ticker/{ticker}/hedge`
- persisted hedge preview only
- no request-time refit

`GET /api/cpar/portfolio/hedge`
- account-scoped portfolio hedge preview only
- no request-time refit or build path
- uses the active cPAR package, live holdings rows, and latest shared-source prices on or before the package date

Page consistency rule:
- the frontend must treat `meta`, `ticker detail`, and `hedge` as one package-scoped flow
- the frontend must treat `meta` and the portfolio hedge payload as one package-scoped flow
- if those responses do not share the same `package_run_id` / `package_date`, the page must fail closed instead of mixing surfaces from different active packages
- `/cpar/explore` enforces this for banner plus detail
- `/cpar/hedge` enforces this for banner, selected subject, and hedge preview
- `/cpar/portfolio` enforces this for banner plus account hedge payload

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
- ticker ambiguity is rendered as a UI instruction to choose a specific RIC from search results
- search hits without a ticker render as non-navigable rows because the current detail route is ticker-keyed
- a direct `/cpar/explore?ric=...` visit without `ticker=` must render an explanatory warning rather than silently failing or synthesizing a detail request
- a direct `/cpar/hedge?ric=...` visit without `ticker=` must render the same explanatory warning because the current hedge route is also ticker-keyed
- package-identity drift between active-package reads must render an explicit reload prompt rather than mixing banner/detail/hedge data from different packages
- `/cpar/portfolio` must render explicit empty or unavailable account states instead of synthesizing a hedge from unpriced or uncovered holdings rows

## Hedge Workflow Split

`/cpar/explore`
- remains the persisted fit discovery/detail surface
- keeps raw and thresholded loadings visible
- does not own hedge mode switching or post-hedge interpretation anymore

`/cpar/hedge`
- remains the focused hedge workflow surface
- reuses persisted detail only to identify the selected subject and package
- owns hedge mode switching, hedge legs, and post-hedge interpretation

`/cpar/portfolio`
- remains a narrow account-level hedge workflow
- owns account selection, coverage/exclusion explanation, aggregate loadings, and the resulting account hedge preview
- does not own portfolio mutation, account editing, or hypothetical scenario authoring

## Smoke Coverage

Current cPAR frontend smokes cover:
- `/cpar` landing and `/cpar/explore` baseline flow
- `/cpar/hedge` baseline flow
- `/cpar/portfolio` baseline flow
- `not_ready`
- `unavailable`
- package mismatch

## Deferred After This Slice

- frontend operator surfaces
- cPAR what-if integration
- any shared cUSE4/cPAR comparison UI
- broader multi-account or portfolio-analytics cPAR views

## Shared App Chrome

The global brand and background menu remain shared with the rest of the app.

The cUSE4 operator-status signal and `serve-refresh` control are intentionally suppressed on `/cpar*` routes so the first cPAR slice does not imply operator coupling that has not been implemented.
