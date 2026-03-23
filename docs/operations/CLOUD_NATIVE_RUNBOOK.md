# Cloud-Native Runbook

Date: 2026-03-21
Owner: Codex
Status: Prep complete, deployment not yet executed

## Purpose

Define the process split and environment contract needed to run the app in a cloud-native shape without relying on one all-in-one web process.

This is deployment prep only.
It does not imply that the repo has already been deployed to a cloud provider.

## Frozen Production Hostnames

- frontend: `https://app.ceiora.com`
- serve API: `https://api.ceiora.com`
- control API: `https://control.ceiora.com`

Temporary smoke validation should use Cloud Run `run.app` hostnames first.
Do not treat the final custom-domain cutover as complete until the `run.app` smoke path is clean.

## Process Split

### Serve App

Purpose:
- stateless public/editor-facing web API surface
- cUSE/cPAR read surfaces
- holdings reads and holdings/editor mutations

Entrypoint:
- `uvicorn backend.serve_main:app --host 0.0.0.0 --port 8000 --workers 1`

Owns these backend routes:
- `/api/portfolio`
- `/api/exposures`
- `/api/risk`
- `/api/holdings/*`
- `/api/universe/*`
- `/api/cpar/*`
- `/api/health`

Does not own:
- `/api/refresh`
- `/api/refresh/status`
- `/api/operator/status`
- `/api/health/diagnostics`
- `/api/data/diagnostics`

### Control App

Purpose:
- operator/control-plane API surface
- serve-refresh execution
- operator and diagnostics reads that track runtime/control truth

Entrypoint:
- `uvicorn backend.control_main:app --host 0.0.0.0 --port 8001 --workers 1`

Owns these backend routes:
- `/api/refresh`
- `/api/refresh/status`
- `/api/operator/status`
- `/api/health/diagnostics`
- `/api/data/diagnostics`
- `/api/health`

### Full Local App

Purpose:
- preserve the existing all-in-one local/dev shape

Entrypoint:
- `uvicorn backend.main:app --host 0.0.0.0 --port 8000 --workers 1`

This remains the compatibility surface for local development and existing tests.

## Environment Contract

### Backend serve app

Required:
- `APP_RUNTIME_ROLE=cloud-serve`
- `DATA_BACKEND=neon`
- `BACKEND_API_ORIGIN` is not used here; that is a frontend setting
- `NEON_DATABASE_URL`
- `NEON_AUTHORITATIVE_REBUILDS=true` for the intended cloud steady state
- operator/editor tokens as appropriate

Expected behavior:
- Neon-backed serving/runtime reads
- no local refresh execution ownership
- holdings writes may mark state dirty but return control-plane-required refresh metadata in cloud mode
- fail closed if Neon-backed serving or runtime truth is unavailable instead of silently falling back to local SQLite

### Backend control app

Required:
- `APP_RUNTIME_ROLE=cloud-serve`
- `DATA_BACKEND=neon`
- `NEON_DATABASE_URL`
- `NEON_AUTHORITATIVE_REBUILDS=true` for the intended cloud steady state
- `OPERATOR_API_TOKEN`

Expected behavior:
- owns `serve-refresh`
- does not need to expose public dashboard read routes
- uses Neon-backed runtime/control truth and should fail closed when that authority is unavailable

### Frontend

Required for split deployment:
- `BACKEND_API_ORIGIN`
  - serve app origin
- `BACKEND_CONTROL_ORIGIN`
  - control app origin
  - if omitted, frontend operator/control proxies fall back to `BACKEND_API_ORIGIN`
  - that fallback is local/single-origin compatibility behavior, not the intended cloud steady state
- `OPERATOR_API_TOKEN`
- `EDITOR_API_TOKEN`

Cloud steady-state values:
- `BACKEND_API_ORIGIN=https://api.ceiora.com`
- `BACKEND_CONTROL_ORIGIN=https://control.ceiora.com`

Local compatibility values:
- `BACKEND_API_ORIGIN=http://127.0.0.1:8000`
- omit `BACKEND_CONTROL_ORIGIN` to reuse the same local backend

## Frontend Proxy Ownership

The split-origin decision is intentionally isolated to Next App Router proxy handlers and their shared helper:

- `frontend/src/app/api/_backend.ts`
- `frontend/src/app/api/refresh/route.ts`
- `frontend/src/app/api/refresh/status/route.ts`
- `frontend/src/app/api/operator/status/route.ts`
- `frontend/src/app/api/health/diagnostics/route.ts`
- `frontend/src/app/api/data/diagnostics/route.ts`

Pages and components should not select backend origins directly.

## Refresh Ownership

Refresh execution ownership is deliberately split:

- `backend/services/refresh_manager.py`
  - process-local execution owner for the control-plane app
- `backend/services/refresh_status_service.py`
  - read-only persisted refresh-status owner
- `backend/services/refresh_dispatcher.py`
  - runtime-aware dispatch owner for “request serve-refresh” flows

This prevents a serve-only process from reconciling or mutating shared refresh state as though it owned the worker.

## Cloud Readiness Gates

Before real cloud reads or cloud `serve-refresh` ownership are treated as production-valid:
- `security_master` bootstrap/parity must be satisfied
- source-sync expectations must be satisfied
- stable-core expectations must be satisfied
- Neon-readiness must be satisfied for the lanes being exposed

`NEON_AUTHORITATIVE_REBUILDS=false` remains a rollback switch for local-SQLite rebuild authority.
That is not the intended steady-state value for Cloud Run services.

## Container Prep Assets

Prepared but not deployed:
- `backend/Dockerfile.serve`
- `backend/Dockerfile.control`
- `frontend/Dockerfile`
- `.dockerignore`
- `infra/terraform/bootstrap`
- `infra/terraform/envs/prod`

These are baseline build assets only. Provider-specific deploy manifests remain out of scope.
The Terraform foundation currently creates the substrate only:
- remote-state bucket bootstrap
- required project APIs
- Artifact Registry
- service accounts
- Secret Manager secret containers and access bindings

It does not yet create running Cloud Run services, Cloud Run Jobs, or final ingress resources.

## Remaining Out Of Scope

- live cloud deployment
- provider-specific ingress/networking/secrets config
- queue-based refresh execution
- dedicated worker surface for deeper local-ingest/core/cold-core lanes
- autoscaling or multi-region strategy

## Validation Expectations

Before using these split surfaces for real deployment:
- backend tests for app surfaces, refresh dispatch, and cloud-runtime behavior should pass
- frontend `typecheck` and `build` should pass
- operator/control proxy routes should be smoke-checked against separate origins
- docs and the tracked plan should be current
