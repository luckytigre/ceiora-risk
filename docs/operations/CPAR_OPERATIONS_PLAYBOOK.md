# cPAR Operations Playbook

Date: 2026-03-19
Status: Active cPAR operations baseline
Owner: Codex

This document describes the current cPAR runtime and operating assumptions.

Related references:
- [CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md)
- [CPAR_ORCHESTRATION.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/CPAR_ORCHESTRATION.md)
- [CPAR_PERSISTENCE_LAYER.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/CPAR_PERSISTENCE_LAYER.md)
- [CPAR_BACKEND_READ_SURFACES.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/CPAR_BACKEND_READ_SURFACES.md)

## Purpose

Define how cPAR currently operates in this repo without implying unimplemented operator or runtime-state surfaces.

It does not describe:
- cUSE4 refresh lanes
- cPAR frontend page details
- broader future cPAR what-if expansion
- any portfolio mutation/apply flow

## Runtime Roles

`local-ingest`
- may build cPAR packages
- reads shared source tables from local SQLite
- may persist cPAR package outputs

`cloud-serve`
- read-only for cPAR
- must not build cPAR packages
- must not trigger request-time fitting
- must fail closed when no successful active package exists in the authority store

## Build Entrypoints

Supported cPAR build profiles:
- `cpar-weekly`
- `cpar-package-date`

Entrypoints:
- `python -m backend.scripts.run_cpar_pipeline --profile cpar-weekly`
- `python -m backend.scripts.run_cpar_pipeline --profile cpar-package-date --as-of-date YYYY-MM-DD`

There is no cPAR `serve-refresh` equivalent in the current implementation.
The current cPAR CLI returns a non-zero exit code when a build is blocked or fails.
Partial cPAR stage-window overrides are not supported because current cPAR success is defined by durable package writes.

## Storage Authority

Local SQLite:
- direct source ingest/archive authority
- acceptable sole cPAR authority only in local development when Neon is not configured

Neon:
- primary write authority when configured
- intended app-serving read authority

Write policy:
- Neon-primary when configured
- SQLite mirror after successful Neon writes
- optional local-only fallback when Neon is not configured in local development
- fail closed on required Neon write/read failures

## Active Package And Read Behavior

The active cPAR package is selected from `cpar_package_runs`.

Read behavior:
- latest successful package wins
- child coverage is required for the requested surface
- hedge preview requires complete covariance coverage
- missing required coverage returns cPAR-specific `503 not_ready`
- the package banner exposes package date/source-as-of freshness plus completion time so stale-but-readable packages remain visible to operators
- frontend pages gate dependent detail/account reads on package metadata first; a package-level `not_ready` or `unavailable` state should not keep probing deeper cPAR routes on the same page load

The current read surfaces do not:
- reuse `serving_payload_current`
- read cUSE4 runtime-state keys
- trigger cPAR builds
- refit cPAR models on request

## Current Frontend/Backend Assumptions

Current frontend-backed read surfaces:
- `/cpar/risk`
- `/cpar/explore`
- `/cpar/health`
- `/cpar/hedge`
- legacy redirects from `/cpar` and `/cpar/portfolio` into `/cpar/risk`
- `GET /api/cpar/meta`
- `GET /api/cpar/search`
- `GET /api/cpar/ticker/{ticker}`
- `GET /api/cpar/ticker/{ticker}/hedge`
- `GET /api/cpar/portfolio/hedge`
- `POST /api/cpar/portfolio/whatif`

The current detail route is ticker-keyed.
Persisted search rows with `ticker = NULL` remain visible in search but are intentionally non-navigable in v1.
`GET /api/cpar/ticker/{ticker}` may now include a supplemental nested `source_context` block for `/cpar/explore`, but that block is still keyed by the resolved persisted fit `ric` plus active `package_date`; it does not change the persisted fit identity, loadings, or hedge truth.
The standalone hedge page reuses that same ticker-keyed selection rule and must fail closed when package identity drifts between the selected subject and the hedge preview.
The first portfolio workflow is account-scoped and read-only: it reuses the shared Neon-backed adapter in `backend/data/holdings_reads.py` plus latest shared-source prices, but it does not reuse cUSE4 portfolio or what-if payload semantics.
The first what-if workflow is embedded in `/cpar/risk` and remains preview-only: it stages signed share deltas against the same active package and account hedge baseline, but it does not apply trades or mutate holdings.
The shared account-scoped snapshot assembly for both flows lives in `backend/services/cpar_portfolio_snapshot_service.py`; that shared owner is cPAR-specific and does not imply any reuse of cUSE4 what-if services.
That shared snapshot now also carries explicit `coverage_breakdown`, factor-only `factor_variance_contributions`, and per-position `thresholded_contributions`; those fields are still derived read surfaces from the same package-scoped snapshot, not a second risk engine.
Upcoming cPAR risk/explore expansion should keep following the same ownership rule: extend current cPAR route/service owners by default, and only add a new cPAR-specific owner when the authority/read pattern is genuinely different.
Until that authority decision is made explicitly, the operations baseline does not assume a new cPAR single-name history route or any reuse of cUSE universe/read surfaces. This slice still does not add a cUSE-style price-history panel to `/cpar/explore`.

## Fail-Closed Cases

Current cPAR flows fail closed when:
- `cloud-serve` is asked to build cPAR packages
- no successful cPAR package exists
- a required cPAR relational surface is missing
- active covariance coverage is partial for hedge preview
- Neon authority reads are required and unavailable
- package identity drifts between package metadata and a later detail/hedge/account payload
- package identity drifts between the shared banner and the portfolio what-if envelope or its nested `current` / `hypothetical` payloads
- a staged what-if addition is not present in the active persisted cPAR package

Explore-only source-context degradation does not fail the ticker-detail route closed:
- if the persisted fit row is readable, `/cpar/explore` still renders
- the nested `source_context.status` / `reason` fields distinguish missing package-date source rows from shared-source unavailability
- treat that as degraded supplemental context, not as a cPAR package outage

## Runtime Troubleshooting

If `/cpar*` shows `not_ready`:
- confirm a successful `cpar-weekly` or explicit `cpar-package-date` build exists
- confirm the active package has the required relational child coverage for the requested surface
- do not expect the frontend to fall back to request-time fitting or route-triggered builds

If `/cpar*` shows `unavailable`:
- in `cloud-serve`, treat this as an authority/read-path outage until Neon-backed reads recover
- in local development, confirm whether Neon is expected; SQLite-only fallback is local-only behavior, not cloud behavior
- for `/cpar/risk*`, remember that cPAR package availability is not enough on its own; those account-scoped flows also require the shared holdings/account adapter to be healthy

If `/cpar/risk` rejects a staged addition:
- confirm the name was staged from an active-package cPAR search hit
- the preview route will not request-time fit off-package RICs or synthesize missing persisted fit rows

If `/cpar/explore` shows source-context degradation while the persisted detail still renders:
- confirm the active package itself is still readable; the fit row should remain authoritative
- if `source_context.status = unavailable`, treat that as shared-source context degradation rather than a package outage
- if `source_context.status = missing` or `partial`, confirm whether the source tables actually contain common-name/classification/price rows on or before the active package date for that `ric`

If `/cpar/risk` shows `empty` instead of `unavailable`:
- `empty` means the selected account has no live holdings rows at all
- `unavailable` means the selected account has live rows, but none are both priced and backed by a usable persisted cPAR fit in the active package

If `/cpar/risk` shows unexpected exclusions or coverage drift:
- inspect `coverage_breakdown` first to see whether excluded rows are coming from:
  - no package-date price
  - no active-package fit row
  - `insufficient_history`
- `thresholded_contributions` are intentionally populated only for covered rows; excluded rows contribute nothing to the aggregate account vector or factor-only variance decomposition

If the shared banner shows an aging or stale package:
- treat the current read surface as historical until a newer package is published
- use the package completion timestamp and package date to distinguish an old-but-consistent package from a current publish failure

## Explicit Non-Goals

This operations baseline still does not include:
- cPAR runtime-state keys
- cPAR operator dashboard integration
- route-triggered cPAR builds
- request-time cPAR fitting
- cPAR apply or mutation flows
- cUSE4/cPAR comparison flows
- broader portfolio analytics beyond the first account-level hedge and preview-only what-if workflow
