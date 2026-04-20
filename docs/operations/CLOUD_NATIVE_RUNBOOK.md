# Cloud-Native Runbook

Date: 2026-04-14
Owner: Codex
Status: current live topology is `run_app` with `edge_enabled=false`; `app.ceiora.com` is served through Firebase Hosting and backends are private behind the frontend

## Purpose

Define the process split and environment contract needed to run the app in a cloud-native shape without relying on one all-in-one web process.

This runbook covers both supported Cloud Run topology modes.
Resolve the current live origins from `terraform output endpoint_mode`, `terraform output edge_enabled`, and `terraform output public_origins`; do not assume a topology from stale rollout notes.

## Topology Modes

- `endpoint_mode=custom_domains`
  - rollback-only topology
  - requires `edge_enabled=true`
  - canonical public origins are the frozen `app.ceiora.com`, `api.ceiora.com`, and `control.ceiora.com` hostnames
  - no explicit origin/image overrides are required beyond the normal custom-domain rollout inputs
- `endpoint_mode=run_app`
  - current production mode
  - requires explicit `frontend_public_origin`, `frontend_backend_api_origin`, `frontend_backend_control_origin`, and pinned frontend/serve/control image refs
  - `edge_enabled=true` is the soak state that keeps rollback paths and custom-domain validation alive
  - `edge_enabled=false` is the no-edge steady state

The Terraform contract now exposes:
- `endpoint_mode`
- `edge_enabled`
- `public_origins`
- `frontend_build_contract`
- `service_image_refs`
- `load_balancer_ip`
- `load_balancer_dns_records`
- `load_balancer_host_routing`

## Current Public Origins

- frontend: `https://app.ceiora.com`
- serve runtime origin: resolve from `terraform output public_origins` and treat it as frontend-only private backend target, not a public user hostname
- control runtime origin: resolve from `terraform output public_origins` and treat it as frontend-only private backend target, not a public user hostname

Current operator rule:
- users should only navigate through `https://app.ceiora.com`
- do not publish or rely on `api.ceiora.com` / `control.ceiora.com`; those aliases were removed from live DNS

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
- public durable serving-payload reads/writes stay behind `backend/data/serving_outputs.py`, while lower Neon/SQLite authority helpers remain non-public implementation detail
- public runtime/control state reads and writes stay behind `backend/data/runtime_state.py`, while lower Neon/fallback authority helpers remain non-public implementation detail
- `source_sync` remains a `local-ingest` concern; cloud-serving surfaces should not call the source-only cycle in `backend/services/neon_source_sync_cycle.py`
- `source_sync` remains strictly `local-ingest` owned; `cloud-job` execution must fail closed rather than offering a generic env-toggle bypass for source publication from cloud compute
- Neon mirror artifact persistence, sync-health publication, and offline parity-repair helpers stay behind `backend/services/neon_mirror_reporting.py`; cloud-serving surfaces should not reassemble those payloads ad hoc

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
- `CORE_WEEKLY_CLOUD_RUN_JOB_NAME=ceiora-prod-core-weekly`
- `COLD_CORE_CLOUD_RUN_JOB_NAME=ceiora-prod-cold-core`
- `CPAR_BUILD_CLOUD_RUN_JOB_NAME=ceiora-prod-cpar-build`

Expected behavior:
- dispatches `serve-refresh`, `core-weekly`, and `cold-core` to their respective Cloud Run Jobs
- dispatches cPAR builds via `POST /api/cpar/build` (control-only route)
- does not need to expose public dashboard read routes
- uses Neon-backed runtime/control truth and should fail closed when that authority is unavailable
- startup must fail closed if the Cloud Run dispatch contract is incomplete for the active surface
  - control app requires project/region plus all four job-name vars
  - serve app should not boot with an ambiguous partial dispatch contract
- serving publication sequencing lives in `backend/analytics/refresh_publication.py`; do not split publish-only republish, durable publish, and post-publish health patch back across ad hoc `pipeline.py` branches
- workspace `data_db` / `cache_db` inputs handed to serving lanes are explicit file targets, not an automatic local-core-read override by themselves

### Frontend

Required for split deployment:
- `BACKEND_API_ORIGIN`
  - serve app origin
- `BACKEND_CONTROL_ORIGIN`
  - control app origin
  - if omitted, frontend operator/control proxies fall back to `BACKEND_API_ORIGIN`
  - that fallback is local/single-origin compatibility behavior, not the intended cloud steady state

Auth contract:
- the frontend now owns the shared signed-cookie app session and protects app pages plus `/api/*` before proxying upstream
- because Firebase Hosting forwards the `__session` cookie on Cloud Run rewrites, the shared app session must use the `__session` cookie name
- the frontend runtime requires:
  - `CEIORA_SHARED_LOGIN_USERNAME`
  - `CEIORA_SHARED_LOGIN_PASSWORD`
  - `CEIORA_SESSION_SECRET`
  - `CEIORA_PRIMARY_ACCOUNT_USERNAME`
- the frontend service must not mount `OPERATOR_API_TOKEN` or `EDITOR_API_TOKEN`
- privileged frontend `/api/*` routes forward caller-supplied auth headers instead:
  - `X-Operator-Token`
  - `X-Editor-Token`
- in cloud mode, `X-Refresh-Token` is not a public control-plane credential
- when `CLOUD_RUN_BACKEND_IAM_AUTH=true`, the frontend proxy layer uses `Authorization: Bearer <Cloud Run ID token>` for backend service-to-service invocation

run_app contract values:
- `frontend_public_origin=https://app.ceiora.com`
- `BACKEND_API_ORIGIN=https://<serve-service>.run.app`
- `BACKEND_CONTROL_ORIGIN=https://<control-service>.run.app`
- all three `*_image_ref` inputs must be explicit rather than inheriting `:latest`

Local compatibility values:
- `BACKEND_API_ORIGIN=http://127.0.0.1:8000`
- omit `BACKEND_CONTROL_ORIGIN` to reuse the same local backend

## Frontend Proxy Ownership

The split-origin decision is intentionally isolated to Next App Router proxy handlers and their shared helper:

- `frontend/src/app/api/_backend.ts`

Current rule:
- browser `/api/*` traffic must terminate in owned App Router route handlers
- the old catch-all `next.config.js` `/api/*` rewrite is no longer part of the supported contract
- if `private_backend_invocation_enabled=true`, the frontend runtime proxies to backend `run.app` service URLs with Cloud Run IAM auth instead of relying on public backend hosts
- `frontend/src/app/api/refresh/route.ts`
- `frontend/src/app/api/refresh/status/route.ts`
- `frontend/src/app/api/operator/status/route.ts`
- `frontend/src/app/api/health/diagnostics/route.ts`
- `frontend/src/app/api/data/diagnostics/route.ts`

Pages and components should not select backend origins directly.

## Shared Auth And Private-Backend Deployment Notes

The shared frontend auth boundary and the private-backend cutover are live.

Current steady-state notes:
1. `app.ceiora.com` is served through Firebase Hosting with a Cloud Run rewrite to the frontend service.
2. The shared app session is stored in the `__session` cookie so Firebase forwards it on rewritten requests.
3. `private_backend_invocation_enabled=true` is active.
4. `serve` and `control` no longer allow unauthenticated direct invocation.
5. The frontend service account invokes `serve` and `control` with Cloud Run IAM auth.
6. The old HTTPS load balancer has been removed.
7. The temporary ACME challenge route still exists in the frontend codebase and can be removed once Firebase control-plane certificate status settles beyond propagation.

Execution record:
- `docs/operations/cutover_evidence/FRONTEND_AUTH_EXECUTION_20260415T010336Z.md`

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
  - Cloud Run Job entrypoint for `serve-refresh`, `core-weekly`, and `cold-core` execution
- `backend/scripts/run_cpar_pipeline_job.py`
  - Cloud Run Job entrypoint for cPAR package-build execution
- `backend/services/cpar_build_service.py`
  - Control-plane dispatch service for cPAR builds
- `backend/api/routes/cpar_control.py`
  - Control-only `POST /api/cpar/build` route (operator-token protected)

This prevents a serve-only process from reconciling or mutating shared refresh state as though it owned the worker.

Operator diagnostics contract:
- `cache_not_ready` means the durable payload is genuinely unpublished/missing for the requested surface
- serving-authority connection/query failures must surface as an explicit authority-unavailable operator error, not as `cache_not_ready`
- this distinction is now part of the operator contract, not just logging hygiene; Step 3 hardened the route/service behavior so cloud-read failures are not misreported as unpublished payloads

## Current Cutover Notes

- As of the active Phase 4 window:
  - the historical cutover/rollback drill started from `custom_domains`
  - `edge_enabled=true` was the drill-time edge posture
  - control-surface rollback has been drill-validated against the corrected bundle `backend/runtime/cloud_rollouts/phase4_entry_20260414T201917Z`
- The recorded rollback drill used direct Cloud Run service/job updates because the full Terraform custom-domain path in that shell was blocked by missing Cloudflare auth.
- Treat the Phase 4 evidence log and rollback drill note as the authoritative execution record:
  - `docs/operations/cutover_evidence/PHASE4_ENTRY_20260414T193820Z.md`
  - `docs/operations/cutover_evidence/PHASE4_ROLLBACK_DRILL_20260414T215254Z.md`

## Cloud Readiness Gates

Before real cloud reads or cloud `serve-refresh` ownership are treated as production-valid:
- registry/policy/taxonomy/compat bootstrap and parity must be satisfied
- source-sync expectations must be satisfied
- stable-core expectations must be satisfied
- Neon-readiness must be satisfied for the lanes being exposed

`NEON_AUTHORITATIVE_REBUILDS=false` remains a rollback switch for local-SQLite rebuild authority.
That is not the intended steady-state value for Cloud Run services.

## Container Prep Assets

Repo-owned rollout assets:
- `backend/Dockerfile.serve`
- `backend/Dockerfile.control`
- `frontend/Dockerfile`
- `.dockerignore`
- `infra/terraform/bootstrap`
- `infra/terraform/envs/prod`

These assets are now the live deployment surface for the single `prod` environment.
The Terraform foundation now owns:
- remote-state bucket bootstrap
- required project APIs
- Artifact Registry
- service accounts
- Secret Manager secret containers and access bindings
- Cloud Run service resources for:
  - frontend
  - serve
  - control
- the first Cloud Run Job surface for `serve-refresh`
- final ingress resources:
  - global HTTPS load balancer
  - serverless NEGs
  - forwarding rules and proxies
  - managed certificate resources
  - Cloudflare DNS records
- observability basics:
  - logging retention
  - no public uptime checks by default; operator/manual smoke is the health path

Provider-specific deploy manifests outside this Terraform/Cloud Run path remain out of scope.

## Image Build Contract

Operator build entrypoints:
- `make cloud-images-build`
- `make cloud-images-push`
- `ALLOW_DIRECT_SERVE_DEPLOY=1 make cloud-serve-deploy`
- `make cloud-run-app-contract`
- `make cloud-topology-check`
- `scripts/cloud/build_images.sh`
- `scripts/cloud/build_and_push_images.sh`
- `ALLOW_DIRECT_SERVE_DEPLOY=1 scripts/cloud/deploy_serve.sh`
- `scripts/cloud/emit_run_app_contract.sh`
- `scripts/cloud/topology_check.sh`
- the repo-owned Cloud Run image path now explicitly builds `linux/amd64` images via `docker buildx`; do not use the plain host-architecture Docker default for rollout images.
- the same scripts also support `BUILD_TARGETS=frontend` when you need a frontend-only rebuild for an explicit `run_app` contract while keeping serve/control pinned to existing image refs.
- the build scripts now read `ENDPOINT_MODE`:
  - `custom_domains` is the default and requires `BACKEND_API_ORIGIN=https://api.ceiora.com` for frontend builds; omitting the env var falls back to that value
  - `run_app` is fail-closed for frontend builds and requires an explicit `BACKEND_API_ORIGIN=https://<serve-service>.run.app`
- the image-build scripts now stage per-target minimal Docker contexts:
  - `frontend` builds from a temp context containing only `frontend/`
  - `serve` / `control` build from a temp context containing only `backend/`
  - this avoids operator-machine runtime archives, virtualenvs, and other repo-local mass contaminating Cloud Run image builds.

Serve-only drift path:
- `ALLOW_DIRECT_SERVE_DEPLOY=1 make cloud-serve-deploy`
  - builds and pushes the `serve` image with the minimal backend-only context
  - deploys that image directly to the Cloud Run serve service as an explicit serve-only drift path
  - must not be used for topology contract changes, `endpoint_mode` flips, or `run_app` cutovers; use Terraform for those changes
  - validates `SERVICE_NAME` as a serve-only target by requiring it to end with `-serve`
  - preserves request-based billing with `--cpu-throttling`

Build-time contract:
- the frontend image reads `BACKEND_API_ORIGIN` at build time so the Next rewrite proxy is baked for the target serve API host
- `ENDPOINT_MODE=custom_domains` default frontend build target is `https://api.ceiora.com`
- `ENDPOINT_MODE=run_app` requires `BACKEND_API_ORIGIN` to be an explicit `https://<serve-service>.run.app` origin that matches the Terraform `frontend_build_contract.build_api_origin` / `frontend_backend_api_origin` input
- `scripts/cloud/build_and_push_images.sh` passes the same `ENDPOINT_MODE` / `BACKEND_API_ORIGIN` contract through to `scripts/cloud/build_images.sh`; do not rely on one script defaulting differently from the other
- the image-build scripts validate only the frontend build-side origin piece of the `run_app` contract; Terraform still owns the full topology contract, including `frontend_public_origin`, `frontend_backend_control_origin`, and pinned `*_image_ref` inputs
- `scripts/cloud/emit_run_app_contract.sh` renders the full `run_app` Terraform snippet from current prod outputs:
  - `RUN_APP_PHASE=soak` keeps `edge_enabled=true`
  - `RUN_APP_PHASE=no-edge` renders the final `edge_enabled=false` contract
- set `PROD_TERRAFORM_OUTPUT_JSON=/path/to/terraform-output.json` when you want the helper to read a saved `terraform output -json` bundle instead of shelling into the prod root
- backend images do not copy repo-local `backend/.env` or the broad local backend tree into the image

Runtime contract:
- all three images honor Cloud Run's injected `PORT`
- `BACKEND_CONTROL_ORIGIN` stays a frontend runtime env input
- `OPERATOR_API_TOKEN` and `EDITOR_API_TOKEN` stay runtime secret inputs for the backend services and jobs that actually consume them
- runtime secrets are not baked into the images

Current Cloud Run Job prep:
- the Terraform `prod` root defines four Cloud Run Job resources:
  - `serve-refresh` — rebuilds frontend-facing caches (short, 1 CPU / 4Gi / 3600s)
  - `core-weekly` — recomputes factor returns, covariance, and specific risk (2 CPU / 4Gi / 7200s)
  - `cold-core` — full structural rebuild from Neon scratch workspace (2 CPU / 8Gi / 14400s)
  - `cpar-build` — builds cPAR weekly packages from Neon source tables (1 CPU / 2Gi / 3600s)
- `serve-refresh` uses `APP_RUNTIME_ROLE=cloud-serve`; the three compute jobs use `APP_RUNTIME_ROLE=cloud-job`
- in `cloud-job` mode: `source_sync` is automatically skipped (no local SQLite); `neon_readiness` stage still runs to hydrate an ephemeral scratch workspace from Neon
- the control service dispatches jobs via:
  - `CLOUD_RUN_JOBS_ENABLED=true`, `CLOUD_RUN_PROJECT_ID`, `CLOUD_RUN_REGION`
  - `SERVE_REFRESH_CLOUD_RUN_JOB_NAME`, `CORE_WEEKLY_CLOUD_RUN_JOB_NAME`, `COLD_CORE_CLOUD_RUN_JOB_NAME`, `CPAR_BUILD_CLOUD_RUN_JOB_NAME`
- cPAR builds are dispatched via `POST /api/cpar/build` (control-only, operator-token protected)
- in `cloud-serve`, missing Cloud Run Job dispatch env is now a fail-closed control-plane error:
  - the control service must not fall back to the local in-process `refresh_manager` path
  - status continues to read the persisted refresh state instead of switching owners
- **local operator workflow after migration**: run `source-daily` locally (LSEG pull → Neon sync) then dispatch compute jobs via control API; no local model compute needed
- post-apply verification for the compute-job surface:
  - anonymous `POST /api/refresh?profile=core-weekly`, `POST /api/refresh?profile=cold-core`, and `POST /api/cpar/build?profile=cpar-weekly` must return `401`
  - tokened `POST /api/cpar/build?profile=not-a-profile` must return `400`
  - tokened dispatch for `core-weekly` / `cold-core` / `cpar-build` should return `202` when the refresh lock is free; `409` is expected if another refresh job is already running
  - confirm Cloud Run executions with `gcloud run jobs executions list --job <job-name> --region us-east4`
  - confirm `cpar_package_runs` persistence in Neon after `cpar-build` reaches terminal success

Current Cloud Run service prep:
- the Terraform `prod` root now defines frontend, serve, and control service resources
- current no-edge production keeps only the frontend public at the Cloud Run layer
- `serve` and `control` are private by IAM and are invoked through the frontend service account
- the control service stays operator-token-protected in-app
- the Terraform root now separates public topology from edge presence:
  - `endpoint_mode=custom_domains` + `edge_enabled=true` is the rollback contract
  - `endpoint_mode=run_app` + `edge_enabled=true` is the soak/rollback shape
  - `endpoint_mode=run_app` + `edge_enabled=false` is the current no-edge production shape
- all three services pin request-based billing by explicitly setting `cpu_idle=true` in Terraform
- any direct `gcloud run deploy` rollout must preserve request-based billing with `--cpu-throttling`
- the live service headroom is now:
  - frontend: `1 vCPU`, `1Gi`, `maxScale=4`
  - serve: `1 vCPU`, `1Gi`, `maxScale=4`
  - control: `1 vCPU`, `1Gi`, `maxScale=3`
- the frontend image build must follow this rule:
  - `endpoint_mode=custom_domains`: `BACKEND_API_ORIGIN=https://api.ceiora.com`
  - `endpoint_mode=run_app`: supply the full explicit Terraform contract together:
    - `frontend_public_origin`
    - `frontend_backend_api_origin`
    - `frontend_backend_control_origin`
    - `frontend_image_ref`
    - `serve_image_ref`
    - `control_image_ref`
- the frontend service mirrors `BACKEND_API_ORIGIN` at runtime for Next server-side proxy helpers, but that runtime env does not override the rewrite compiled into the image
- `frontend_build_contract.edge_enabled` shows whether the load balancer / DNS edge is still provisioned for rollback
- when `edge_enabled=false`, `load_balancer_ip`, `load_balancer_dns_records`, and `load_balancer_host_routing` return `null`

Current ingress prep:
- the Terraform `prod` root now owns the custom-domain edge through `module.edge`
- when `edge_enabled=true`, that module provisions:
  - a single global HTTPS load balancer
  - host-based routing for `app.ceiora.com`, `api.ceiora.com`, and `control.ceiora.com`
  - one serverless NEG and backend service per Cloud Run surface
  - a managed certificate covering all three hostnames
  - HTTP-to-HTTPS redirect
  - Cloudflare DNS A records for `app`, `api`, and `control`
- the root now carries explicit `moved` blocks from the earlier root-level ingress resources into `module.edge`
- Cloudflare DNS stays DNS-only for the first cutover:
  - `cloudflare_proxied=false`
- the public `run.app` smoke posture remains valid in every topology mode
- `hostnames` are the reserved custom-domain names; `load_balancer_*` outputs are live only when `edge_enabled=true`

Current observability prep:
- Terraform now manages `_Default` Cloud Logging retention for the rollout project
- public Cloud Monitoring uptime checks are intentionally removed to preserve scale-to-zero behavior
- `control.ceiora.com` remains an operator-token smoke target
- app and API validation now happen through:
  - explicit rollout smoke
  - operator/manual checks when needed

## Cutover Execution

Operator rollout entrypoints:
- `make cloud-run-app-bundle`
- `CUTOVER_ACTION=bundle make cloud-run-app-cutover`
- `CUTOVER_ACTION=build-frontend ROLLOUT_BUNDLE_DIR=... make cloud-run-app-cutover`
- `CUTOVER_ACTION=plan CUTOVER_PHASE=soak|no-edge|rollback ROLLOUT_BUNDLE_DIR=... make cloud-run-app-cutover`
- `CUTOVER_ACTION=apply CUTOVER_PHASE=soak|no-edge|rollback ROLLOUT_BUNDLE_DIR=... ALLOW_TERRAFORM_APPLY=1 make cloud-run-app-cutover`
- `CUTOVER_ACTION=verify ROLLOUT_BUNDLE_DIR=... OPERATOR_API_TOKEN=... make cloud-run-app-cutover`
- `scripts/cloud/capture_run_app_rollout_bundle.sh`
- `scripts/cloud/run_app_cutover.sh`

Bundle contract:
- `cloud-run-app-bundle` captures a distinct staged-cutover bundle under `backend/runtime/cloud_rollouts/` by default
- `cloud-run-app-steady-state-bundle` captures the current `run_app` topology as a post-cutover pin bundle for config-only changes and helper refresh after targeted image applies
  - that steady-state bundle also emits current-image `run_app_no_edge.base.tfvars` and `rollback_custom_domains.tfvars` so a verified soak can still proceed to no-edge or rollback without replaying stale image refs
  - for future config-only Terraform work in no-edge steady state, start by recapturing this bundle and use `run_app_current_topology.tfvars` as the pinned image/origin contract
- that bundle is not the same thing as the generic `PROD_TERRAFORM_OUTPUT_JSON=...` input used by the read-only helper scripts
- the bundle includes:
  - `terraform-output.json`
  - `manifest.json`
  - `rollback_custom_domains.tfvars`
  - `run_app_soak.base.tfvars`
  - `run_app_no_edge.base.tfvars`
- by default the bundle capture expects the live source topology to still be `custom_domains + edge_enabled=true`
- use `ROLLOUT_CAPTURE_MODE=steady-state` once the app is already on `endpoint_mode=run_app`
- `ROLLOUT_SOURCE_OUTPUT_JSON=...` remains a saved-output seam; do not confuse it with the saved bundle itself

Execution contract:
- `CUTOVER_ACTION=build-frontend` builds and pushes a `run_app` frontend image against the bundle's `service_urls.serve` origin and records that image ref into `run_app_frontend_image_ref.txt`
- `CUTOVER_ACTION=plan` and `CUTOVER_ACTION=apply` are fail-closed for `run_app` phases:
  - they require a run.app-built frontend image ref
  - set `RUN_APP_FRONTEND_IMAGE_REF=...` explicitly or run `CUTOVER_ACTION=build-frontend` first
- `CUTOVER_ACTION=plan` and `CUTOVER_ACTION=apply` also verify that the bundle still matches the current live topology and applied image refs for `soak` / `no-edge`:
  - if you hotfix a service/job image with a targeted apply, recapture a fresh bundle before reusing the cutover helpers
  - bypass only with `ALLOW_STALE_ROLLOUT_BUNDLE=1` for an intentional replay
- `CUTOVER_ACTION=apply` requires `ALLOW_TERRAFORM_APPLY=1`
- `CUTOVER_PHASE=no-edge` apply also requires `ALLOW_EDGE_DISABLE=1`
- `cloud-run-app-cutover` does not replace repo-side smoke:
  - keep running `make smoke-check` separately
  - `CUTOVER_ACTION=verify` wraps the live topology-aware operator check and may optionally include request-billing verification when `VERIFY_REQUEST_BILLING=1`

## Remaining Out Of Scope

- queue-based refresh execution
- Cloud Scheduler automation for compute jobs (manual dispatch via CLI for now)
- autoscaling or multi-region strategy

## Validation Expectations

Before using these split surfaces for real deployment:
- backend tests for app surfaces, refresh dispatch, and cloud-runtime behavior should pass
- frontend `typecheck` and `build` should pass
- operator/control proxy routes should be smoke-checked against separate origins
- docs and the tracked plan should be current

For the request-based billing rollout specifically:
- roll out `control` first, then `serve`, then `frontend`
- after each Terraform apply, confirm Cloud Run reports `run.googleapis.com/cpu-throttling=true` with `make cloud-request-billing-check`
- require a clean `terraform plan` after the rollout so live state and Terraform state agree
- use `make smoke-check` for repo-side contract checks
- use `make operator-check` with `APP_BASE_URL`, `CONTROL_BASE_URL`, and `OPERATOR_API_TOKEN` set for live control-plane validation
- `CUTOVER_ACTION=verify make cloud-run-app-cutover` wraps the live topology-aware operator smoke; it does not replace `make smoke-check`
- use `make cloud-topology-check` when you want the repo to choose the live URLs from current Terraform outputs:
  - `custom_domains`: checks only the custom-domain path
  - `run_app + edge_enabled=true`: checks both the `run.app` path and the still-live custom-domain rollback path
  - `run_app + edge_enabled=false`: checks only the `run.app` path
- `cloud-topology-check` is safe-by-default for soak:
  - it forces `RUN_REFRESH_DISPATCH=0` unless you explicitly set `TOPOLOGY_CHECK_RUN_REFRESH_DISPATCH=1`
  - when dispatch is enabled, it dispatches against one chosen surface only instead of every checked path
  - default `TOPOLOGY_CHECK_DISPATCH_SURFACE=active`
  - set `TOPOLOGY_CHECK_DISPATCH_SURFACE=run_app` or `edge` when you want to force the real dispatch to a specific soak surface
  - default `TOPOLOGY_CHECK_REFRESH_EXPECTED_OUTCOME=success` requires a successful terminal `serve-refresh`
  - set `TOPOLOGY_CHECK_REFRESH_EXPECTED_OUTCOME=core_due_refusal` when the operating-model-correct result is a fail-closed refusal because the stable core package is due
- that check now validates both the frontend-proxied and direct control paths:
  - anonymous `/api/operator/status` and `/api/refresh/status` must return `401`
  - legacy `X-Refresh-Token` and invalid-token `/api/operator/status`, `/api/refresh/status`, and `POST /api/refresh` must return `401`
  - tokened `/api/operator/status`, `/api/refresh/status`, `/api/health/diagnostics`, and gated `/api/data/diagnostics?include_paths=true` must return `200`
  - `Authorization: Bearer <OPERATOR_API_TOKEN>` is also validated on the status routes
- set `OPERATOR_CHECK_REQUIRE_LIVE=1` during rollout guardrails so missing live URLs fail closed instead of silently skipping the cloud smoke
- set `RUN_REFRESH_DISPATCH=1` on `make operator-check` to run a real `POST /api/refresh?profile=serve-refresh` and watch `/api/refresh/status` to terminal state
- the real `POST /api/refresh` dispatch is exercised for the selected target only:
  - default `RUN_REFRESH_DISPATCH_TARGET=proxy`
  - set `RUN_REFRESH_DISPATCH_TARGET=direct` when you want that real dispatch to hit `control` directly instead of the frontend proxy
  - default `RUN_REFRESH_EXPECTED_OUTCOME=success` requires the dispatch to finish with `refresh.status=ok`
  - set `RUN_REFRESH_EXPECTED_OUTCOME=core_due_refusal` when a real dispatch should prove the fail-closed core-due guard instead of a successful refresh

Topology guardrails:
- always confirm `terraform output endpoint_mode`, `terraform output edge_enabled`, and `terraform output public_origins` match the intended rollout path
- `service_urls` remain the topology-neutral Cloud Run reference surface
- `service_image_refs` are the configured Terraform pins; `service_image_refs_applied` are the live refs recorded in Terraform state for the Cloud Run services
- `control_surface_image_refs_applied` should show matching control-service and serve-refresh-job images before you emit or capture a shared control-image contract
- `hostnames` remain the reserved custom-domain names even when the edge is disabled
- `load_balancer_*` outputs are edge-only and may be `null` when `edge_enabled=false`
- `cloud-topology-check` shells into `terraform output -json` by default; use `PROD_TERRAFORM_OUTPUT_JSON=...` when you want it to operate from a saved rollout bundle instead

## Rollout Order

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
- do not wire operator/editor secrets into the frontend service; the browser/operator supplies those credentials at request time

3. Build and push images
- final-domain default:
  - build/push frontend against `https://api.ceiora.com`
  - build/push serve and control normally
  - use the repo-owned scripts so the published images stay `linux/amd64` for Cloud Run
- `endpoint_mode=run_app` soak or no-edge contract:
  - capture the staged rollout bundle first:
    - `make cloud-run-app-bundle`
    - or `CUTOVER_ACTION=bundle make cloud-run-app-cutover`
  - the bundle now holds the pinned rollback contract plus the staged `run_app` base contracts
  - rebuild/push the frontend image against the bundle's `service_urls.serve` value:
    - `CUTOVER_ACTION=build-frontend ROLLOUT_BUNDLE_DIR=... make cloud-run-app-cutover`
  - do not plan/apply a `run_app` phase until a run.app-built frontend image ref is recorded or supplied explicitly

4. Smoke the `run.app` surfaces
- frontend root
- serve `/api/cpar/meta`
- control `/api/refresh/status` with `X-Operator-Token`
- verify the control service can dispatch the `serve-refresh` Cloud Run Job
  - the control service's job IAM must allow execution overrides because the dispatch path sets env overrides on the Cloud Run Job request
  - if operator status reports `.core_due.due=true`, use the stronger live check with `RUN_REFRESH_EXPECTED_OUTCOME=core_due_refusal` or `TOPOLOGY_CHECK_REFRESH_EXPECTED_OUTCOME=core_due_refusal` so the rollout proves the fail-closed guard instead of expecting a successful serving refresh

5. Soak with the edge still enabled
- plan the soak contract:
  - `CUTOVER_ACTION=plan CUTOVER_PHASE=soak ROLLOUT_BUNDLE_DIR=... make cloud-run-app-cutover`
- apply the soak contract:
  - `CUTOVER_ACTION=apply CUTOVER_PHASE=soak ROLLOUT_BUNDLE_DIR=... ALLOW_TERRAFORM_APPLY=1 make cloud-run-app-cutover`
- re-run repo smoke separately:
  - `make smoke-check`
- then re-run the live topology smoke:
  - `CUTOVER_ACTION=verify ROLLOUT_BUNDLE_DIR=... OPERATOR_API_TOKEN=... make cloud-run-app-cutover`

6. Disable the edge only after soak
- plan the no-edge contract:
  - `CUTOVER_ACTION=plan CUTOVER_PHASE=no-edge ROLLOUT_BUNDLE_DIR=... make cloud-run-app-cutover`
- apply the no-edge contract:
  - `CUTOVER_ACTION=apply CUTOVER_PHASE=no-edge ROLLOUT_BUNDLE_DIR=... ALLOW_TERRAFORM_APPLY=1 ALLOW_EDGE_DISABLE=1 make cloud-run-app-cutover`
- confirm `load_balancer_ip`, `load_balancer_dns_records`, and `load_balancer_host_routing` now return `null`
- re-run repo smoke separately:
  - `make smoke-check`
- then re-run the live smoke against the `run.app` URLs only:
  - `CUTOVER_ACTION=verify ROLLOUT_BUNDLE_DIR=... OPERATOR_API_TOKEN=... make cloud-run-app-cutover`

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
- Final-domain validation is now complete:
  - the Google-managed certificate is `ACTIVE`,
  - `https://app.ceiora.com/` returns `200`,
  - `https://api.ceiora.com/api/cpar/meta` returns `200`,
  - `https://control.ceiora.com/api/refresh/status` returns `200` with the operator token,
  - `https://app.ceiora.com/api/refresh/status` returns `200` with the operator token,
  - a post-cutover `serve-refresh` dispatch completed successfully through the control plane and reconciled in persisted runtime status.
- Separate cloud caveat still open after cutover:
  - address `security_master_compat_current` parity if projection-only loadings should become available in cloud mode instead of remaining fail-closed/unavailable.
- Latest validated `run.app` state:
  - the control service account can now read `serve-refresh` execution status and reconcile stale `running` rows,
  - a fresh `serve-refresh` Cloud Run Job run completed successfully with the `4Gi` memory limit,
  - the frontend `run.app` surface successfully proxies both serve and control routes,
  - frontend, serve, and control now run with `1Gi` memory and higher `maxScale` headroom,
  - cloud control misconfiguration now fails closed instead of falling back to the local in-process refresh manager,
  - the final-domain frontend image is already published as `frontend:20260323-finaldomain-r1`,
  - the Google load balancer, serverless NEGs, forwarding rules, and managed certificate resource are live on `34.50.154.73`,
  - Cloudflare DNS now points `app.ceiora.com`, `api.ceiora.com`, and `control.ceiora.com` at that shared ingress IP,
  - the frontend Cloud Run service is already running the final-domain bake against `https://api.ceiora.com` and `https://control.ceiora.com`,
  - the managed certificate is active and the final custom-domain HTTPS path is now the primary validated ingress,
  - projection-only loadings still warn and degrade unavailable when `security_master_compat_current` parity is absent, but the refresh path remains green and publishes serving payloads.

## Control Smoke

Use the operator token for the control-plane smoke:

```bash
curl -i \
  -H "X-Operator-Token: ${OPERATOR_API_TOKEN}" \
  https://control.ceiora.com/api/refresh/status
```
