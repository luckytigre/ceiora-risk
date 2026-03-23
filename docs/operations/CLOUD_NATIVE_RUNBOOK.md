# Cloud-Native Runbook

Date: 2026-03-21
Owner: Codex
Status: `run.app` rollout validated, final custom-domain cutover not yet complete

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
- `CLOUD_RUN_JOBS_ENABLED=true`
- `CLOUD_RUN_PROJECT_ID=project-4e18de12-63a3-4206-aaa`
- `CLOUD_RUN_REGION=us-east4`
- `SERVE_REFRESH_CLOUD_RUN_JOB_NAME=ceiora-prod-serve-refresh`

Expected behavior:
- dispatches `serve-refresh` to the Cloud Run Job surface
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
  - process-local execution owner for local compatibility only
- `backend/services/refresh_control_service.py`
  - application-facing refresh control surface for routes and control clients
- `backend/services/refresh_status_service.py`
  - read-only persisted refresh-status owner
- `backend/services/refresh_dispatcher.py`
  - runtime-aware dispatch owner for “request serve-refresh” flows
- `backend/ops/cloud_run_jobs.py`
  - Cloud Run Jobs dispatch adapter for the control-plane app
- `backend/scripts/run_refresh_job.py`
  - Cloud Run Job entrypoint for synchronous `serve-refresh` execution

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

It now also defines Cloud Run service resources for:
- frontend
- serve
- control

It now also defines the first Cloud Run Job surface for `serve-refresh`.
It still does not own final ingress resources or live deployed traffic by itself.

## Image Build Contract

Operator build entrypoints:
- `make cloud-images-build`
- `make cloud-images-push`
- `scripts/cloud/build_images.sh`
- `scripts/cloud/build_and_push_images.sh`
- the repo-owned Cloud Run image path now explicitly builds `linux/amd64` images via `docker buildx`; do not use the plain host-architecture Docker default for rollout images.
- the same scripts also support `BUILD_TARGETS=frontend` for the temporary `run.app` smoke rebuild, so the smoke exception can retarget only the frontend image without rebuilding serve/control.

Build-time contract:
- the frontend image reads `BACKEND_API_ORIGIN` at build time so the Next rewrite proxy is baked for the target serve API host
- default frontend build target is `https://api.ceiora.com`
- backend images do not copy repo-local `backend/.env` or the broad local backend tree into the image

Runtime contract:
- all three images honor Cloud Run's injected `PORT`
- `BACKEND_CONTROL_ORIGIN`, `OPERATOR_API_TOKEN`, and `EDITOR_API_TOKEN` stay runtime env/secret inputs
- runtime secrets are not baked into the images

Current Cloud Run Job prep:
- the Terraform `prod` root now defines a `serve-refresh` Cloud Run Job resource
- the control service is expected to dispatch to that job via:
  - `CLOUD_RUN_JOBS_ENABLED=true`
  - `CLOUD_RUN_PROJECT_ID`
  - `CLOUD_RUN_REGION`
  - `SERVE_REFRESH_CLOUD_RUN_JOB_NAME`

Current Cloud Run service prep:
- the Terraform `prod` root now defines frontend, serve, and control service resources
- all three services are intentionally public at the Cloud Run layer for the first `run.app` smoke phase
- the control service stays operator-token-protected in-app
- the frontend image build must follow this rule:
  - final-domain default: `BACKEND_API_ORIGIN=https://api.ceiora.com`
  - `run.app` smoke: rebuild the frontend image against the serve service's `run.app` URL, then set:
    - `frontend_image_ref`
    - `frontend_backend_api_origin`
- the frontend service mirrors `BACKEND_API_ORIGIN` at runtime for Next server-side proxy helpers, but that runtime env does not override the rewrite compiled into the image

Current ingress prep:
- the Terraform `prod` root now defines:
  - a single global HTTPS load balancer
  - host-based routing for `app.ceiora.com`, `api.ceiora.com`, and `control.ceiora.com`
  - one serverless NEG and backend service per Cloud Run surface
  - a managed certificate covering all three hostnames
  - HTTP-to-HTTPS redirect
  - Cloudflare DNS A records for `app`, `api`, and `control`
- Cloudflare DNS stays DNS-only for the first cutover:
  - `cloudflare_proxied=false`
- this ingress prep does not change the current public `run.app` smoke posture
- final-domain cutover must use a frontend image built against `https://api.ceiora.com`, not the earlier `run.app` smoke image

Current observability prep:
- Terraform now manages `_Default` Cloud Logging retention for the rollout project
- Terraform now defines Cloud Monitoring uptime checks for:
  - `https://app.ceiora.com/`
  - `https://api.ceiora.com/api/cpar/meta`
- `control.ceiora.com` is intentionally not a public uptime target
  - validate it with an operator-token smoke instead

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

## First Rollout Order

1. Bootstrap and Terraform state
- `cd infra/terraform/bootstrap`
- `terraform init`
- `terraform apply`
- `cd ../envs/prod`
- `cp backend.hcl.example backend.hcl`
- `terraform init -backend-config=backend.hcl`

2. Create secret containers and add versions
- `terraform apply` for the prod root once the secret containers are in the plan
- then add runtime secret versions out of band:
  - `NEON_DATABASE_URL`
  - `OPERATOR_API_TOKEN`
  - `EDITOR_API_TOKEN`

3. Build and push images
- final-domain default:
  - build/push frontend against `https://api.ceiora.com`
  - build/push serve and control normally
  - use the repo-owned scripts so the published images stay `linux/amd64` for Cloud Run
- `run.app` smoke exception:
  - apply the Cloud Run services first,
  - capture the serve `run.app` URL from Terraform outputs,
  - rebuild/push the frontend image against that serve `run.app` URL,
  - override `frontend_image_ref` and `frontend_backend_api_origin`

4. Smoke the `run.app` surfaces before domain cutover
- frontend root
- serve `/api/cpar/meta`
- control `/api/refresh/status` with `X-Operator-Token`
- verify the control service can dispatch the `serve-refresh` Cloud Run Job
  - the control service's job IAM must allow execution overrides because the dispatch path sets env overrides on the Cloud Run Job request

5. Cut over custom domains
- apply the ingress and DNS resources
- wait for the managed certificate to become active
- switch to the final-domain frontend image built against `https://api.ceiora.com`
- re-run app/api/control smoke against:
  - `https://app.ceiora.com`
  - `https://api.ceiora.com`
  - `https://control.ceiora.com`

## Current Rollout Notes

- `run.app` deployment is now live for:
  - frontend
  - serve
  - control
  - `serve-refresh` Cloud Run Job
- Basic `run.app` smoke already passed for:
  - frontend root
  - serve `/api/cpar/meta`
  - control `/api/refresh/status`
  - control dispatch of `serve-refresh`
- The first cloud `serve-refresh` attempts surfaced three cloud-only follow-ups:
  - persisted risk-artifact reuse needed to honor runtime-state risk metadata when it outranks stale model-run metadata,
  - eligibility reads needed to use Neon/core-read paths instead of SQLite-only source tables,
  - the control service needed to reconcile persisted `running` refresh state against terminal Cloud Run execution status so OOM-killed jobs do not remain stuck forever.
- Those repo fixes are now in place.
- Current remaining rollout blockers before custom-domain cutover:
  - decide when to promote the validated `run.app` rollout state to the final-domain cutover,
  - address `security_master` parity if projection-only loadings should become available in cloud mode instead of remaining fail-closed/unavailable.
- Latest validated `run.app` state:
  - the control service account can now read `serve-refresh` execution status and reconcile stale `running` rows,
  - a fresh `serve-refresh` Cloud Run Job run completed successfully with the `4Gi` memory limit,
  - the frontend `run.app` surface successfully proxies both serve and control routes,
  - projection-only loadings still warn and degrade unavailable when `security_master` parity is absent, but the refresh path remains green and publishes serving payloads.

## Control Smoke

Use the operator token for the control-plane smoke:

```bash
curl -i \
  -H "X-Operator-Token: ${OPERATOR_API_TOKEN}" \
  https://control.ceiora.com/api/refresh/status
```
