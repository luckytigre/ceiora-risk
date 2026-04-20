# GCP Cloud Run + Terraform Migration Plan

Date: 2026-03-22
Owner: Codex
Status: Active tracker; `run_app` no-edge production is live and the compute-job rollout is applied

Related docs:
- [Cloud-Native Implementation Plan](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/architecture/CLOUD_NATIVE_IMPLEMENTATION_PLAN.md)
- [Cloud-Native Runbook](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/operations/CLOUD_NATIVE_RUNBOOK.md)
- [Architecture And Operating Model](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md)
- [Operations Playbook](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/operations/OPERATIONS_PLAYBOOK.md)

## Update Discipline

- This file is the canonical tracker for the actual GCP Cloud Run + Terraform migration.
- Every change in this workstream must update this file in the same diff.
- Updates must do at least one of the following:
  - check off completed steps,
  - refine an in-progress step so it matches the approved implementation,
  - update `Decisions`, `Open Questions`, or `Validation Notes`, or
  - add a dated note to `Progress Notes`.
- Do not leave completed migration work undocumented here.
- Do not use this file as a brainstorming scratchpad. Keep it current, concrete, and runnable.
- Any frozen decision that changes runtime topology, env/secret contract, ingress/auth behavior, or operator workflow must update this tracker and the relevant referenced architecture/operations doc in the same diff.

## Fixed Decisions

- One cloud environment only for the initial rollout.
- GCP rollout project:
  - `project-4e18de12-63a3-4206-aaa`
- Billing is enabled on the rollout project.
- The local all-in-one app remains a first-class path for:
  - fast development,
  - local diagnostics,
  - direct LSEG ingest,
  - fallback local operations.
- LSEG ingest stays local-only for this migration phase.
- Terraform is the source of truth for cloud resources.
- Google Cloud Run is the target compute surface for the cloud app.
- Scale-to-zero is a hard requirement unless a later validated exception is documented here.
- The cloud app should be standalone and Neon-backed.
- Verified operator-workstation prerequisites now in place:
  - Google ADC working
  - Docker working locally
  - Cloudflare DNS token available via 1Password
- In `cloud-serve`, cUSE payloads, runtime/operator state, holdings, and cPAR package reads must remain Neon-authoritative and fail closed instead of falling back to local SQLite.
- `NEON_AUTHORITATIVE_REBUILDS` is a separate contract from Neon serving reads:
  - default-on when Neon is the active backend and a Neon DSN is configured,
  - set `NEON_AUTHORITATIVE_REBUILDS=false` only as an explicit rollback to local-SQLite rebuild authority.
- Custom domain family remains the rollback/reference edge contract:
  - `app.ceiora.com` for the frontend
  - `api.ceiora.com` for the serve API
  - `control.ceiora.com` for the control API
- Initial cloud access model:
  - internet-reachable frontend
  - internet-reachable serve API
  - internet-reachable control API, but still protected by operator credentials and not anonymous
- Production custom-domain routing will use a global external HTTPS load balancer with serverless NEGs, not Cloud Run preview domain mapping.
- Current production topology is `endpoint_mode=run_app` with `edge_enabled=false`; resolve live origins from Terraform `public_origins`.
- The control API is intended for:
  - the frontend's server-side proxy routes, and
  - explicit operator or automation clients
  It is not intended for anonymous browser-direct usage.
- The durable cloud job migration sequence is:
  - `serve-refresh` first,
  - then `core-weekly`, `cold-core`, and `cpar-build`.
- The additional compute-job slice is now applied in Terraform; treat post-apply execution/durable-output verification as the remaining operational validation step when checking this tracker.
- Local operator workflow after compute migration is intended to be: run `source-daily` locally (LSEG pull → Neon sync), then dispatch compute jobs via control API. No local model compute required once the jobs are provisioned and validated.
- Cloud `serve-refresh` cutover is valid only after the repo's stable-core, source-sync, Neon-readiness, and `security_master` parity prerequisites are frozen and documented for cloud execution.
- Initial image publishing will start from the verified operator workstation:
  - local Docker build
  - push to Artifact Registry
  - CI/CD image publishing is deferred until the first cloud rollout is stable

## Objective

Build a standalone cloud runtime that preserves the repo's current architectural split:

- frontend on Cloud Run,
- serve API on Cloud Run,
- control API on Cloud Run,
- Neon as the serving authority,
- local LSEG ingest and local dev app kept intact,
- Terraform managing the cloud stack end to end.

Target outcome:
- the cloud app can scale to zero when idle,
- the local app is not replaced or broken by cloud rollout work,
- runtime boundaries from the current repo stay explicit,
- operator/control traffic is separated from public/editor traffic,
- cloud deployment can be repeated from Terraform without hand-built infrastructure.

## Non-Goals

- No live deployment in this planning phase.
- No attempt to move direct LSEG ingest into cloud.
- No multi-environment (`dev` + `prod`) Terraform split yet.
- No multi-region rollout.
- No rewrite of cUSE/cPAR frontend IA.
- No premature queue platform or Kubernetes adoption.

## Current Repo Baseline

Already present:
- FastAPI split entrypoints:
  - [backend/main.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/main.py)
  - [backend/serve_main.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/serve_main.py)
  - [backend/control_main.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/control_main.py)
- frozen route matrix in [router_registry.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/api/router_registry.py)
- frontend split-origin helper in [_backend.ts](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/frontend/src/app/api/_backend.ts)
- refresh-status read ownership already split into a dedicated service
- refresh dispatch ownership already split into a dedicated service
- current local app/runtime bootstrap now has concrete owners:
  - [scripts/setup_local_env.sh](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/scripts/setup_local_env.sh)
  - [scripts/local_app/up.sh](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/scripts/local_app/up.sh)
  - [scripts/doctor.sh](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/scripts/doctor.sh)
- baseline container assets:
  - [backend/Dockerfile.serve](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/Dockerfile.serve)
  - [backend/Dockerfile.control](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/Dockerfile.control)
  - [frontend/Dockerfile](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/frontend/Dockerfile)

Important remaining caveat from the current codebase:
- the control plane still owns process-local background refresh execution through `refresh_manager`
- that is not a durable scale-to-zero Cloud Run execution model by itself
- this migration plan must resolve that explicitly instead of treating it as a deployment detail

## Target Topology

### Local Runtime

Purpose:
- preserve fast dev iteration and local troubleshooting
- preserve direct LSEG ingest
- preserve current all-in-one operational fallback

Entrypoint:
- [backend.main:app](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/main.py)

Ownership:
- not managed by Terraform
- not replaced by the cloud app
- canonical local bootstrap and health commands are now:
  - `./scripts/setup_local_env.sh`
  - `make app-up`
  - `make app-down`
  - `make app-check`
  - `make app-status`
  - `make doctor`
- the canonical local Python environment is `.venv_local`, not `backend/.venv`

### Cloud Frontend

Purpose:
- public Next.js app on `app.ceiora.com`
- proxies public/editor API traffic to `api.ceiora.com`
- proxies operator/control traffic to `control.ceiora.com`

Entrypoint:
- [frontend/Dockerfile](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/frontend/Dockerfile)

### Cloud Serve API

Purpose:
- public/editor-facing read and holdings mutation surface
- stateless and Neon-backed

Entrypoint:
- [backend.serve_main:app](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/serve_main.py)

Hostname:
- `api.ceiora.com`

### Cloud Control API

Purpose:
- operator/control-plane API surface
- status, diagnostics, refresh dispatch

Entrypoint:
- [backend.control_main:app](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/control_main.py)

Hostname:
- `control.ceiora.com`

### Cloud Job Surface

Purpose:
- durable background execution for control-plane work that should not live inside a request/response service

Target state for this slice:
- Cloud Run Jobs are the execution surface for all model compute:
  - `serve-refresh` — cache rebuild (1 CPU / 4Gi / 3600s)
  - `core-weekly` — factor returns + risk recompute (2 CPU / 4Gi / 7200s)
  - `cold-core` — full structural rebuild (2 CPU / 8Gi / 14400s)
  - `cpar-build` — cPAR weekly package build (1 CPU / 2Gi / 3600s)
- All four jobs use `APP_RUNTIME_ROLE=cloud-job` (except serve-refresh which uses `cloud-serve`).
- All four jobs should be provisioned in the Terraform `prod` root once this slice is applied.

## Region And Naming Strategy

- Use one environment named `prod` from day one, even though it is the only environment.
- Prefer an east-coast GCP region to keep browser latency low for New York usage.
- Primary region is frozen to `us-east4` after checking the live Neon host against AWS `us-east-1`.
- All Terraform naming should assume a single-environment prefix pattern such as:
  - `ceiora-prod-frontend`
  - `ceiora-prod-serve`
  - `ceiora-prod-control`

## Terraform Ownership Scope

Terraform should own:
- bootstrap state-bucket creation through an explicit bootstrap step or bootstrap Terraform root
- main remote state bucket usage after bootstrap
- required Google APIs
- Artifact Registry repository
- service accounts
- IAM bindings
- Secret Manager secrets and secret access bindings
- Cloud Run services:
  - frontend
  - serve
  - control
- Cloud Run Jobs for control-plane execution
- Cloud Scheduler resources only when the job-triggering phase starts
- domain/DNS/load-balancer resources for custom domains
  - Google provider owns the load balancer, serverless NEGs, and certificates
  - Cloudflare provider owns public DNS records for `app.ceiora.com`, `api.ceiora.com`, and `control.ceiora.com`
- minimal logging/monitoring resources that do not undermine scale-to-zero by default

Terraform should not own:
- the local LSEG machine
- local SQLite artifacts
- local-only launcher scripts
- app runtime secrets that belong in Cloud Run/Secret Manager consumption rather than in Terraform state

## Environment And Secret Contract

### Backend serve

Required:
- `APP_RUNTIME_ROLE=cloud-serve`
- `DATA_BACKEND=neon`
- `NEON_DATABASE_URL`
- `NEON_AUTHORITATIVE_REBUILDS` explicitly set to the intended cloud value
- `EDITOR_API_TOKEN`

Expected:
- no local refresh execution ownership
- no local SQLite serving authority
- fail-closed Neon-serving behavior for cUSE, holdings, runtime/operator state, and cPAR package reads

### Backend control

Required:
- `APP_RUNTIME_ROLE=cloud-serve`
- `DATA_BACKEND=neon`
- `NEON_DATABASE_URL`
- `NEON_AUTHORITATIVE_REBUILDS` explicitly set to the intended cloud value
- `OPERATOR_API_TOKEN`
- `CLOUD_RUN_JOBS_ENABLED=true`
- `CLOUD_RUN_PROJECT_ID=project-4e18de12-63a3-4206-aaa`
- `CLOUD_RUN_REGION=us-east4`
- `SERVE_REFRESH_CLOUD_RUN_JOB_NAME=ceiora-prod-serve-refresh`

Local single-backend compatibility only:
- `REFRESH_API_TOKEN`
  - not a public cloud control-plane credential

### Terraform operator credentials

Required on the operator workstation or CI runner:
- Google ADC credentials for Terraform
- Cloudflare API token for DNS changes

Rule:
- The Cloudflare token is a Terraform/operator credential, not an app runtime secret.
- Do not put the Cloudflare token into Cloud Run runtime env vars or app-facing Secret Manager entries.

### Frontend

Required:
- `BACKEND_API_ORIGIN=https://api.ceiora.com`
- `BACKEND_CONTROL_ORIGIN=https://control.ceiora.com`

Rule:
- the frontend service must not mount `OPERATOR_API_TOKEN` or `EDITOR_API_TOKEN`
- privileged frontend `/api/*` routes forward caller-supplied auth headers instead

### Local app

Remains outside Terraform.
Its current `.env` and local launcher shape stay valid.

## Security Model For Initial Rollout

Chosen initial bias:
- keep the cloud surfaces internet-reachable
- keep the auth model simple and explicit
- do not make anonymous operator/control access acceptable

Initial rollout model:
- frontend public with unauthenticated Cloud Run access
- serve public with unauthenticated Cloud Run access
- control internet-reachable with unauthenticated Cloud Run access, but protected in-app by operator-token auth
- frontend control proxies continue to use the token-backed server-side proxy model already in the repo

Hardening follow-up options:
- Identity-Aware Proxy
- service-to-service IAM with tighter ingress
- load-balancer-only ingress for control

Those are follow-on hardening options, not prerequisites for the first cloud migration slice.

## Critical Design Gate

Before real cloud deployment, resolve the control-plane execution model.

The current repo still uses process-local background refresh execution.
That does not cleanly match a scale-to-zero Cloud Run service.

Approved migration direction:
- keep `control_main` as the control API surface
- move durable refresh execution to Cloud Run Jobs
- let the control API dispatch jobs and rely on persisted runtime-state continuity instead of owning the worker thread directly
- do not treat `serve-refresh` as independently movable until the stable-core and Neon-readiness prerequisites for that lane are frozen for cloud execution

Do not treat this as optional if the goal is a durable cloud-native runtime.

## Planned Slices

### Slice 1: Freeze Cloud Topology And Terraform Conventions

- [x] Freeze the canonical cloud topology:
  - local app
  - Cloud Run frontend
  - Cloud Run serve
  - Cloud Run control
  - Cloud Run Jobs
- [x] Freeze the hostname plan:
  - `app.ceiora.com`
  - `api.ceiora.com`
  - `control.ceiora.com`
- [x] Freeze the single-environment naming convention as `prod`.
- [x] Freeze the initial auth model for public vs operator surfaces.
- [x] Freeze the cloud fail-closed authority contract:
  - Neon-backed serving reads
  - Neon-backed runtime/operator state
  - explicit `NEON_AUTHORITATIVE_REBUILDS` behavior
- [x] Freeze the prerequisite source-of-truth gate for cloud reads:
  - `security_master` bootstrap/parity
  - source-sync expectations
  - stable-core expectations
- [x] Update `.env.example` so the split-origin and token contract is explicit:
  - `BACKEND_CONTROL_ORIGIN`
  - `OPERATOR_API_TOKEN`
  - `EDITOR_API_TOKEN`
  - cloud/runtime examples for `NEON_AUTHORITATIVE_REBUILDS`

### Slice 2: Terraform Foundation

- [x] Add a dedicated Terraform root under a durable repo-owned path.
- [x] Freeze the bootstrap rule for remote Terraform state:
  - a tiny local-state bootstrap root under `infra/terraform/bootstrap` creates the state bucket first,
  - the real environment root lives under `infra/terraform/envs/prod` and uses the GCS backend after bootstrap.
- [x] Record the now-fixed rollout project directly in Terraform inputs/examples:
  - `project-4e18de12-63a3-4206-aaa`
- [x] Add provider/version pinning.
- [x] Add remote state guidance and backend configuration shape.
- [x] Add project-service enablement for the required GCP APIs.
- [x] Add Artifact Registry ownership.
- [x] Add Secret Manager ownership.
- [x] Add service-account and IAM ownership.
- [x] Add Cloudflare provider ownership for public DNS records.
- [x] Add the custom-domain routing ownership foundation:
  - the Terraform roots and providers now explicitly reserve Google-owned ingress resources and Cloudflare-owned public DNS records,
  - the actual load balancer, serverless NEGs, certificates, and DNS records remain a later slice so the foundation can validate without deployed services.

### Slice 3: Container Build And Runtime Contract Hardening

- [x] Run and record successful local builds for:
  - `backend/Dockerfile.serve`
  - `backend/Dockerfile.control`
  - `frontend/Dockerfile`
- [x] Tighten container/runtime env contracts so Cloud Run deployment is deterministic.
- [x] Implement and document the initial image build/publish path:
  - local Docker build
  - Artifact Registry push from the verified operator workstation
- [x] Defer CI/CD image publishing until after the first cloud rollout is stable.
- [x] Keep the local app path unchanged.

### Slice 4: Control-Plane Execution Migration

- [x] Replace process-local refresh execution assumptions with durable dispatch/execution ownership.
- [x] Add Cloud Run Job ownership to the Terraform plan, not as a later optional add-on.
- [x] Freeze the first cloud job lane as `serve-refresh` only.
- [x] Freeze the `serve-refresh` cloud prerequisite contract:
  - stable core package present
  - required source-sync state present
  - Neon-readiness satisfied
  - `security_master` parity satisfied
- [x] Add the operator workflow cutover plan:
  - dispatch trigger path
  - persisted status continuity path
  - failure and retry handling
- [ ] Add direct Cloud Run execution observation if we later need job-level operator visibility beyond persisted runtime state.
- [x] Update the runbook and operator-facing docs when this cutover lands.

### Slice 5: Frontend + Serve Cloud Run Smoke Rollout

- [x] Add Cloud Run service ownership in Terraform for:
  - frontend
  - serve
  - control
- [x] Freeze per-surface image refs and runtime env/secret wiring in Terraform outputs and variables.
- [x] Freeze the frontend smoke-build rule:
  - final-domain images default to `https://api.ceiora.com`,
  - `run.app` smoke requires rebuilding the frontend image against the serve service's `run.app` URL and overriding `frontend_image_ref` plus `frontend_backend_api_origin`,
  - the frontend Cloud Run service mirrors `BACKEND_API_ORIGIN` at runtime for Next server-side proxy helpers, but changing the service env alone does not retarget the compiled rewrite.
- [x] Freeze temporary Cloud Run layer access for smoke:
  - all three services are explicitly internet-reachable at `run.app`,
  - control remains token-protected in-app even though Cloud Run ingress is public for the first smoke phase.
- [x] Deploy frontend and serve first against default `run.app` hostnames for smoke validation.
- [x] Freeze the temporary control-origin coexistence rule for this phase:
  - either operator/control routes are intentionally unavailable in the public smoke environment,
  - or the cloud frontend points `BACKEND_CONTROL_ORIGIN` at the cloud control smoke endpoint only after Slice 6 exists.
- [ ] Validate:
- [x] Validate:
  - Neon-backed reads
  - split-origin proxy behavior
  - no local SQLite dependency in cloud mode
  - expected behavior when operator/control routes remain token-protected through the cloud frontend proxy
  - projection-only instruments and other fail-closed degraded states still behave correctly

### Slice 6: Control Cloud Run Rollout

- [x] Deploy the control API after Slice 5 is implemented.
- [x] Validate operator/control routes through `control.ceiora.com`.
- [ ] Confirm scale-to-zero behavior does not break job dispatch/status visibility.
- [x] Keep operator credentials required in cloud mode.
- [x] Keep `cloud-serve` refresh dispatch fail-closed:
  - missing Cloud Run Job env must not fall back to the local in-process `refresh_manager`,
  - status should stay on the persisted runtime-state owner even when dispatch is unavailable.

### Slice 7: Custom Domains, Ingress, And Certificates

- [x] Freeze the ingress non-goal:
  - this slice does not tighten the current public `run.app` Cloud Run posture,
  - it only prepares final-domain cutover resources.
- [x] Freeze the split-origin ingress rule:
  - host-based routing must keep `app`, `api`, and `control` on separate backends.
- [x] Freeze the frontend cutover rule:
  - final-domain cutover must use a frontend image built against `https://api.ceiora.com`, not the earlier `run.app` smoke image.
- [x] Add DNS and certificate ownership.
- [x] Implement the already chosen production ingress path for custom domains:
  - global external HTTPS load balancer
  - serverless NEGs
  - managed certificates or explicitly owned certificate path
- [x] Add Cloudflare DNS ownership for:
  - `app.ceiora.com`
  - `api.ceiora.com`
  - `control.ceiora.com`
- [x] Validate:
  - `app.ceiora.com`
  - `api.ceiora.com`
  - `control.ceiora.com`
- [x] Document any split between temporary `run.app` testing and final custom-domain ingress.

### Slice 8: Observability And Operations

- [x] Add log retention expectations.
- [x] Add minimal health validation while preserving scale-to-zero behavior.
- [x] Freeze the control-plane observability rule:
  - `control.ceiora.com` remains an operator-token smoke target, not a public uptime probe.
  - `app` / `api` should not have continuous public uptime probes if they materially interfere with scale-to-zero.
- [x] Add operator runbook steps for cloud troubleshooting.
- [x] Update docs so the local app and cloud app are both clearly supported paths.

## Validation Expectations

Every implementation slice in this migration should update this file and run the validation relevant to the touched scope.

Minimum recurring validation:
- targeted backend tests for the changed surface
- `cd frontend && npm run typecheck`
- `cd frontend && npm run build`
- split-origin proxy validation when frontend proxy code changes
- `git diff --check`
- `git diff --cached --check`

Additional validation by phase:
- Terraform:
  - `terraform fmt -check`
  - `terraform validate`
  - provider init succeeds
- Containers:
  - Docker builds succeed for frontend, serve, and control
- Cloud rollout:
  - isolated smoke checks against `run.app` URLs before domain cutover
  - cloud fail-closed checks for missing Neon/runtime truth
  - projection-only output checks against the stable core package
- Control-plane migration:
  - dispatch/status tests
  - background execution ownership tests

## Open Questions

- None currently blocking the prep-only implementation slices.

## Progress Notes

- 2026-03-22: Initial Google Cloud Run + Terraform migration tracker drafted. This builds on the completed cloud-native prep work but treats control-plane execution as the main unresolved design gate before real deployment.
- 2026-03-22: First independent review round reinforced three requirements that are now frozen into this plan:
  - preserve the repo's route matrix and split-origin frontend contract,
  - keep the local app explicitly in scope,
  - treat Cloud Run control execution as a dispatch/jobs problem, not just another always-on service.
- 2026-03-22: Second independent review round tightened the plan in four places:
  - remote-state bootstrap is now an explicit tracked decision,
  - Cloud Run Jobs are part of the initial migration path, not a vague later add-on,
  - the temporary frontend/control coexistence step is now explicit,
  - the first cloud job lane is frozen to `serve-refresh` only.
- 2026-03-23: Repo re-review after newer work tightened the plan again:
  - the local fallback path is now explicitly `.venv_local` + `scripts/local_app/*` + `make doctor`,
  - cloud env assumptions now explicitly include fail-closed Neon authority behavior and `NEON_AUTHORITATIVE_REBUILDS`,
  - `serve-refresh` cloud cutover now carries explicit stable-core, source-sync, Neon-readiness, and `security_master` parity prerequisites,
  - cloud acceptance testing now explicitly includes projection-only outputs and other fail-closed degraded states.
- 2026-03-23: Implementation-readiness re-review after the local mods settled:
  - the rollout project is now fixed to `project-4e18de12-63a3-4206-aaa`,
  - billing, ADC, Docker, and Cloudflare token access are all confirmed,
  - `app.ceiora.com` is detached from Vercel and no longer part of the temporary smoke path,
  - Slice 2 and Slice 3 now assume execution readiness instead of workstation/tool discovery.
- 2026-03-23: Slice 1 completed:
  - `.env.example` now preserves local defaults while explicitly documenting the split-origin cloud env contract,
  - the cloud runbook now freezes `app.ceiora.com` / `api.ceiora.com` / `control.ceiora.com`,
  - `BACKEND_CONTROL_ORIGIN` fallback is now documented as local/single-origin compatibility only,
  - the cloud runbook now explicitly carries the source-of-truth gate and `NEON_AUTHORITATIVE_REBUILDS` steady-state vs rollback distinction.
- 2026-03-23: Slice 2 foundation completed:
  - added `infra/terraform/bootstrap` for local-state creation of the shared GCS backend bucket,
  - added `infra/terraform/envs/prod` as the single live environment root with pinned Google, Google Beta, and Cloudflare providers,
  - froze `us-east4` as the first-cut region after checking the live Neon host against AWS `us-east-1`,
  - added repo-owned modules for project API enablement, Artifact Registry, service accounts, and Secret Manager secret containers,
  - documented the out-of-band secret-version workflow and validated both Terraform roots with `terraform init -backend=false` and `terraform validate`.
- 2026-03-23: Slice 3 container/runtime hardening completed:
  - backend images now copy only the explicit backend package surface instead of the whole local `backend/` tree,
  - `.dockerignore` now excludes repo-local env files, local virtualenvs, offline backups, runtime archives, and Terraform workdirs from image contexts,
  - the frontend image now takes `BACKEND_API_ORIGIN` as a build-time input so the rewrite proxy no longer bakes `127.0.0.1` into cloud images,
  - all three images now honor runtime `PORT`,
  - added operator-owned build/push scripts and `make cloud-images-build` / `make cloud-images-push`,
  - validated the full local image build path for frontend, serve, and control.
- 2026-03-23: Slice 4 control-plane migration completed:
  - added `refresh_control_service` as the route-facing refresh owner and kept `refresh_manager` as local process/thread compatibility glue,
  - extracted synchronous refresh execution into `backend/orchestration/refresh_execution.py` so local threads and Cloud Run Jobs share the same status-update path,
  - added `backend/ops/cloud_run_jobs.py` plus `backend/scripts/run_refresh_job.py` for Cloud Run Jobs dispatch and execution,
  - extended runtime config with explicit Cloud Run Jobs env flags,
  - added Terraform ownership for the `serve-refresh` Cloud Run Job and the control-service dispatch env contract,
  - pinned durable single-flight dispatch through atomic refresh-status claiming in the refresh-status owner so duplicate `serve-refresh` jobs are not dispatched under concurrent requests,
  - explicitly kept operator status on the persisted runtime-state path and deferred direct Cloud Run Execution observation to a later slice if richer job-level visibility is needed.
- 2026-03-23: Slice 5 service/runtime prep completed:
  - added Terraform-owned Cloud Run service definitions for frontend, serve, and control,
  - added Artifact Registry reader access for all runtime service accounts,
  - added public `run.app` invoker bindings for the smoke phase and explicit control-to-job invoke IAM,
  - froze per-surface image refs plus frontend proxy-origin inputs in Terraform variables and outputs,
  - documented the rule that `run.app` smoke requires a frontend image rebuilt against the serve service's `run.app` URL before apply.
- 2026-03-23: Slice 7 ingress/DNS prep completed:
  - added a single global HTTPS load balancer with host-based routing for `app`, `api`, and `control`,
  - added one serverless NEG and backend service per Cloud Run surface,
  - added a managed multi-domain certificate plus HTTP-to-HTTPS redirect path,
  - added Cloudflare DNS A records for `app.ceiora.com`, `api.ceiora.com`, and `control.ceiora.com` pointing at the shared global IP with `proxied=false`,
  - explicitly preserved the public `run.app` smoke posture and froze the rule that final-domain cutover must use a frontend image built against `https://api.ceiora.com`.
- 2026-03-23: Slice 8 observability/runbook prep completed:
  - added Terraform ownership for `_Default` Cloud Logging retention,
  - initially added Cloud Monitoring uptime checks for `app.ceiora.com` and `api.ceiora.com`,
  - explicitly kept `control.ceiora.com` on an operator-token smoke path instead of a public uptime probe,
  - expanded the runbook with the exact secret-version, image-build, Terraform, smoke, and custom-domain sequencing needed for the first live rollout while preserving the local app as a first-class path.
- 2026-03-23: Live rollout phase 1 completed:
  - applied the Terraform bootstrap root and created the shared GCS state bucket `ceiora-risk-project-4e18de12-63a3-4206-aaa-tfstate`,
  - initialized the Terraform `prod` root against that remote backend,
  - applied the foundation resources needed before runtime secret versions and image pushes:
    - required project services,
    - Artifact Registry,
    - runtime service accounts,
    - Secret Manager secret containers,
    - secret accessor IAM bindings,
  - confirmed the rollout remains on `us-east4` and is ready for runtime secret version provisioning.
- 2026-03-23: Live rollout phase 2 completed:
  - added version `1` to all three production runtime secrets in Secret Manager:
    - `ceiora-prod-neon-database-url`
    - `ceiora-prod-operator-api-token`
    - `ceiora-prod-editor-api-token`,
  - generated fresh operator/editor tokens for the cloud stack and stored them in 1Password as:
    - `GCP - Ceiora Prod Operator Token`
    - `GCP - Ceiora Prod Editor Token`,
  - confirmed the prod root now has the secret material needed for Cloud Run service and job deployment.
- 2026-03-23: Live rollout phase 3 build-path correction:
  - the first service deployment attempt failed because the operator workstation published OCI indexes without a guaranteed `linux/amd64` runtime image,
  - the repo-owned cloud build scripts now default to `docker buildx` with `CLOUD_RUN_PLATFORM=linux/amd64`,
  - `build_and_push_images.sh` now publishes Cloud Run-compatible images directly instead of relying on a separate `docker push` of host-default artifacts.
- 2026-03-24: Serve deploy-path hardening completed:
  - `build_images.sh` now stages minimal per-target Docker contexts instead of building serve/control from the broad repo root,
  - the minimal-context contract is now:
    - `frontend` context = `frontend/` only
    - `serve` / `control` context = `backend/` only
  - added `scripts/cloud/deploy_serve.sh` and `make cloud-serve-deploy` as the deterministic operator path for pushing and rolling the serve image.
- 2026-03-23: Live rollout phase 4 `run.app` deployment completed:
  - deployed frontend, serve, control, and the `serve-refresh` Cloud Run Job to `us-east4`,
  - basic smoke passed against the Cloud Run hostnames:
    - frontend root,
    - serve `/api/cpar/meta`,
    - control `/api/refresh/status`,
    - control `/api/refresh?profile=serve-refresh` dispatch,
  - Slice 5 and Slice 6 are now deployed on `run.app`, but final validation remains open until `serve-refresh` finishes cleanly in cloud mode.
- 2026-03-23: Live rollout phase 5 cloud-runtime fixes landed during the first `serve-refresh` attempts:
  - cloud `serve-refresh` was incorrectly refusing to reuse richer persisted risk artifacts when runtime state, rather than model-run metadata, was the effective risk-engine source; the pipeline now reuses those artifacts only when the persisted risk metadata matches the effective runtime metadata,
  - cloud `serve-refresh` was still reading SQLite-only eligibility inputs such as `security_fundamentals_pit`; the risk-model eligibility loaders now use the Neon/core-read backend in cloud mode,
  - the control-plane refresh owner now reconciles persisted `running` cloud-job state against terminal Cloud Run executions so OOM-killed or otherwise terminated jobs do not leave refresh status permanently stuck.
- 2026-03-23: Live rollout blockers remaining after the first cloud `serve-refresh` executions:
  - the Cloud Run Job hit memory pressure at `512Mi` and again at `2Gi`; Terraform raised the `serve-refresh` job memory limit to `4Gi`,
  - projection-only cloud refresh still warns that `security_master` parity is missing in the cloud runtime path, so those outputs remain explicitly fail-closed/unavailable until that source-of-truth gap is addressed,
  - final-domain cutover remains blocked until the `run.app` rollout state is deliberately promoted.
- 2026-03-23: Live rollout phase 6 `run.app` validation completed:
  - granted the control service account Cloud Run execution-view access on the `serve-refresh` job so persisted refresh state can reconcile against actual Cloud Run executions,
  - verified the stale `running` refresh row reconciles to terminal `failed` after an OOM-killed execution instead of blocking future dispatch forever,
  - reran `serve-refresh` under the `4Gi` Cloud Run Job spec and observed a successful execution (`ceiora-prod-serve-refresh-xzl9k`) with Neon-backed serving payload publication,
  - validated the frontend `run.app` surface proxies both serve (`/api/cpar/meta`) and control (`/api/refresh/status`) correctly with the split-origin cloud wiring,
  - confirmed projection-only loadings currently degrade unavailable with a warning instead of crashing the refresh path when `security_master` parity is absent.
- 2026-03-23: Service headroom was raised for the live `run.app` rollout while keeping the smoke image contract stable:
  - frontend now runs at `1 vCPU`, `1Gi`, `maxScale=4`,
  - serve now runs at `1 vCPU`, `1Gi`, `maxScale=4`,
  - control now runs at `1 vCPU`, `1Gi`, `maxScale=3`,
  - the current `run.app` frontend image and `run.app` serve/control origins were kept pinned during that change so the headroom bump did not silently promote the final-domain frontend bake.
- 2026-03-23: Control-plane dispatch hardening landed before final-domain cutover:
  - in `cloud-serve`, missing Cloud Run Job dispatch env now fails closed instead of delegating to the local in-process `refresh_manager`,
  - `get_refresh_status` keeps reading the persisted runtime-state owner in that degraded case instead of switching to local process state,
  - the runbook now explicitly records the live Cloud Run headroom, the fail-closed control contract, and the extra final-domain smoke gates for frontend-proxied control status plus post-cutover `serve-refresh` dispatch.
- 2026-03-23: Final-domain cutover staging advanced on the Google side:
  - built and pushed the final-domain frontend image `frontend:20260323-finaldomain-r1` against `https://api.ceiora.com`,
  - created the Google global HTTPS load balancer, serverless NEGs, backend services, URL maps, proxies, and forwarding rules,
  - reserved the shared ingress IP `34.50.154.73`,
  - created the managed certificate resource for `app.ceiora.com`, `api.ceiora.com`, and `control.ceiora.com`,
  - left the `run.app` smoke path as the last fully validated ingress path until DNS cutover and final HTTPS smoke complete.
- 2026-03-23: Final-domain DNS cutover and frontend promotion completed:
  - removed the legacy Cloudflare CNAMEs that still pointed `app.ceiora.com` at Vercel and `api.ceiora.com` at Railway,
  - applied the Terraform-managed Cloudflare A records so `app.ceiora.com`, `api.ceiora.com`, and `control.ceiora.com` now resolve to `34.50.154.73`,
  - promoted the frontend Cloud Run service to the final-domain image `frontend:20260323-finaldomain-r1` with `https://api.ceiora.com` and `https://control.ceiora.com` origins,
  - verified the HTTP listener redirects to HTTPS on the final domains,
  - left Slice 7 validation open until the Google-managed certificate becomes `ACTIVE` and final HTTPS smoke passes.
- 2026-03-23: Final-domain validation completed:
  - the Google-managed certificate `ceiora-prod-cloud-app-cert` is now `ACTIVE` for `app.ceiora.com`, `api.ceiora.com`, and `control.ceiora.com`,
  - `https://app.ceiora.com/`, `https://api.ceiora.com/api/cpar/meta`, `https://control.ceiora.com/api/refresh/status`, and the frontend-proxied `https://app.ceiora.com/api/refresh/status` all returned `200`,
  - a post-cutover `serve-refresh` dispatch completed successfully as Cloud Run execution `ceiora-prod-serve-refresh-q5n2f` and reconciled back into persisted runtime status,
  - Slice 6 control-route validation and Slice 7 custom-domain validation are now closed,
  - scale-to-zero behavior remains the last intentionally open cloud-runtime validation item.
- 2026-03-23: Public uptime probes were removed from the live stack to preserve scale-to-zero behavior:
  - the `app` and `api` Cloud Monitoring uptime checks were continuously waking Cloud Run and repeatedly hitting Neon-backed metadata reads,
  - Terraform now keeps logging retention but removes those public uptime resources,
  - operator/manual smoke remains the intended health-validation path,
  - scale-to-zero validation should now be rechecked against the quieter runtime surface.
