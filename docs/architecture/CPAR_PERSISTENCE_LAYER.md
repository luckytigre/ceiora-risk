# cPAR Persistence Layer

Date: 2026-03-18
Status: Active slice-2 persistence implementation notes
Owner: Codex

This document describes the `backend/data/cpar_*` persistence layer only.

Related cPAR docs:
- [CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md)
- [CPAR_OPERATIONS_PLAYBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/operations/CPAR_OPERATIONS_PLAYBOOK.md)

## Purpose

This slice defines the durable relational storage surfaces for `cPAR`.

It does not add:
- services
- routes
- orchestration entrypoints
- runtime-state keys
- serving payload surfaces
- frontend integration

## Durable Table Set

`cpar_package_runs`
- one row per cPAR package build attempt
- stores package date, run id, profile, status, method/version metadata, package counts, authority metadata, and error fields

`cpar_proxy_returns_weekly`
- one row per package run, week, and factor proxy
- stores the raw weekly proxy return panel and its package weights

`cpar_proxy_transform_weekly`
- one row per package run and non-market factor proxy
- stores the package-level market-orthogonalization transform for that proxy

`cpar_factor_covariance_weekly`
- one row per package run and factor pair
- stores the raw ETF covariance and correlation surface used by cPAR risk and hedge logic

`cpar_instrument_fits_weekly`
- one row per package run and instrument
- stores fit status, warnings, fit metadata, market-step values, raw loadings, thresholded loadings, and variance proxies

## Authority Model

Local SQLite:
- direct local durable store
- acceptable as sole authority in local development when Neon is not configured
- always used as the mirror store when Neon-primary writes succeed

Neon:
- primary write authority when configured
- intended app-serving authority for future cPAR reads
- cloud-serve must read from Neon and fail closed if no successful package exists

## Active-Package Semantics

The active cPAR package is:
- the latest successful row in `cpar_package_runs`
- ordered by `package_date DESC`, then `completed_at DESC`, then `updated_at DESC`, then `package_run_id DESC`
- only eligible if that `package_run_id` has non-empty proxy-return, proxy-transform, covariance, and instrument-fit child rows

There is no runtime-state key for active package selection in this slice.
The package-runs table is the authority, and active-package reads resolve child rows by the selected `package_run_id`.

## Write Behavior

If Neon is configured:
- write Neon first
- if Neon-primary is required and the Neon write fails, raise before any SQLite mirror write
- if Neon is optional and the Neon write fails, fall back to SQLite authority
- if the Neon write succeeds, also write the SQLite mirror
- the persistence layer, not the caller, sets durable `data_authority` metadata based on the resolved write outcome

Neon write failures are not silently ignored:
- required Neon writes raise
- optional Neon writes are recorded as failed in the persistence result before SQLite fallback is used

If Neon is not configured:
- write SQLite only

Writes replace child rows by `package_run_id` for:
- proxy returns
- proxy transforms
- covariance rows
- instrument fits

The package-runs table keeps one row per `package_run_id`.
Same-date reruns therefore do not overwrite a prior successful package’s child rows.
The default cPAR pipeline-generated `package_run_id` includes timestamp plus entropy so same-second builds remain distinct durable attempts.
Local SQLite schema creation also fails closed on stale cPAR table layouts by rebuilding mismatched cPAR tables before writes continue.
`status='ok'` package writes also fail closed if any durable child surface is empty.

## Read Behavior

Future cPAR app reads are expected to come from the durable relational `cpar_*` tables.

This slice does not introduce:
- `serving_payload_current`
- blob payload caching
- request-time fitting

`cloud-serve` behavior:
- read-only
- fail closed when there is no successful cPAR package
- do not fall back to local SQLite when the Neon authority store has no successful package
- raise a Neon authority-read error if the cloud Neon read path itself is broken

## Explicit Non-Goals

This slice does not include:
- cPAR services
- cPAR routes
- cPAR orchestration
- operator status integration
- runtime-state integration
- serving-payload integration
- frontend work
