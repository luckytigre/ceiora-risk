# cPAR Operations Playbook

Date: 2026-03-18
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
- future cPAR what-if or portfolio mutation flows

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
- `/cpar`
- `/cpar/explore`
- `/cpar/hedge`
- `/cpar/portfolio`
- `GET /api/cpar/meta`
- `GET /api/cpar/search`
- `GET /api/cpar/ticker/{ticker}`
- `GET /api/cpar/ticker/{ticker}/hedge`
- `GET /api/cpar/portfolio/hedge`

The current detail route is ticker-keyed.
Persisted search rows with `ticker = NULL` remain visible in search but are intentionally non-navigable in v1.
The standalone hedge page reuses that same ticker-keyed selection rule and must fail closed when package identity drifts between the selected subject and the hedge preview.
The first portfolio workflow is account-scoped and read-only: it reuses live holdings accounts/positions plus latest shared-source prices, but it does not reuse cUSE4 portfolio or what-if payload semantics.

## Fail-Closed Cases

Current cPAR flows fail closed when:
- `cloud-serve` is asked to build cPAR packages
- no successful cPAR package exists
- a required cPAR relational surface is missing
- active covariance coverage is partial for hedge preview
- Neon authority reads are required and unavailable
- package identity drifts between package metadata and a later detail/hedge/account payload

## Runtime Troubleshooting

If `/cpar*` shows `not_ready`:
- confirm a successful `cpar-weekly` or explicit `cpar-package-date` build exists
- confirm the active package has the required relational child coverage for the requested surface
- do not expect the frontend to fall back to request-time fitting or route-triggered builds

If `/cpar*` shows `unavailable`:
- in `cloud-serve`, treat this as an authority/read-path outage until Neon-backed reads recover
- in local development, confirm whether Neon is expected; SQLite-only fallback is local-only behavior, not cloud behavior

If the shared banner shows an aging or stale package:
- treat the current read surface as historical until a newer package is published
- use the package completion timestamp and package date to distinguish an old-but-consistent package from a current publish failure

## Explicit Non-Goals

This operations baseline still does not include:
- cPAR runtime-state keys
- cPAR operator dashboard integration
- route-triggered cPAR builds
- request-time cPAR fitting
- cPAR what-if or mutation flows
- cUSE4/cPAR comparison flows
- broader portfolio analytics beyond the first account-level hedge workflow
