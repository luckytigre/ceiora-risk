# cPAR Orchestration

Date: 2026-03-18
Status: Active slice-3 orchestration implementation notes
Owner: Codex

This document describes the dedicated cPAR package-build orchestration only.
Current cPAR read surfaces, routes, and frontend ownership live in the active cPAR architecture/operations docs; this note is intentionally limited to package-build ownership.

Related cPAR docs:
- [CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md)
- [CPAR_OPERATIONS_PLAYBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/operations/CPAR_OPERATIONS_PLAYBOOK.md)

## Purpose

This slice introduces a cPAR-only package-build entrypoint.

It does not add:
- cPAR services
- cPAR routes
- frontend integration
- runtime-state keys
- operator-status integration
- route-triggered builds

## Entrypoints

Python entrypoint:
- `backend/orchestration/run_cpar_pipeline.py`

CLI wrapper:
- `backend/scripts/run_cpar_pipeline.py`

Default generated `run_id` values include a UTC timestamp plus a short random suffix so concurrent or same-second cPAR builds do not collide on `package_run_id`.

## Supported Profiles

`cpar-weekly`
- resolves the latest completed weekly package date
- reads shared-source inputs from the local SQLite ingest/archive surface
- runs `source_read -> package_build -> persist_package`

`cpar-package-date`
- requires one explicit XNYS weekly package date
- reads shared-source inputs from the local SQLite ingest/archive surface
- runs `source_read -> package_build -> persist_package`

There is no `serve-refresh` equivalent in `v1`.
Partial cPAR stage windows are not supported in the current pipeline because success is defined as a durable package write.

## Runtime-Role Behavior

`local-ingest`
- allowed to build and persist cPAR packages
- reads shared-source inputs from the local archive
- persists durable `cpar_*` outputs through the cPAR persistence layer

`cloud-serve`
- not allowed to build cPAR packages
- fails closed before any cPAR stage starts
- does not create shared operator/status rows for blocked cPAR build attempts

This slice does not introduce any cloud build fallback or request-time fitting.

## Pipeline Stages

`source_read`
- resolves the fixed cPAR1 factor proxy set from the registry-first shared source surface
- uses `security_registry`, `security_policy_current`, and taxonomy/current-source tables rather than treating physical `security_master` as the authority contract
- loads the cPAR build universe from canonical source tables
- keeps factor proxies in the instrument build universe so proxy ETFs themselves also receive persisted cPAR fits
- loads package-window prices plus latest classification/common-name PIT rows
- fails early if the local archive is not current through the requested package date for required factor proxies

`package_build`
- builds weekly proxy returns from the approved cPAR1 anchor rules
- computes package-level market orthogonalization for non-market proxies
- computes the raw ETF covariance surface
- fits instrument rows and prepares durable relational cPAR rows

`persist_package`
- writes the durable relational `cpar_*` surfaces through `backend/data/cpar_outputs.py`
- uses Neon-primary plus SQLite mirror behavior when configured
- does not write runtime-state or serving-payload surfaces

## Success And Failure

Success means:
- all selected stages complete
- durable `cpar_*` rows are written for the target package date
- the pipeline return payload includes completed in-memory `run_rows`

Failure means:
- the pipeline stops at the first failed stage
- the failed stage appears in the returned in-memory `run_rows`
- no runtime-state keys or operator surfaces are updated
- the CLI exits non-zero for failed or blocked runs so shell automation can fail closed

Read-surface implication:
- the resulting active package date becomes the frontend freshness anchor
- if no successful current package exists, the read surfaces fail closed instead of probing lower package-dependent routes repeatedly

## Explicit Non-Goals

This slice does not include:
- cPAR route-triggered builds
- cPAR runtime-state keys
- cPAR operator dashboard integration
- cPAR app-facing payload services
- any reuse of cUSE4 profiles or `run_model_pipeline.py`
