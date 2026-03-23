# GCP Cloud Run + Terraform Migration Plan

Date: 2026-03-22
Owner: Codex
Status: Active implementation tracker

Related docs:
- [Cloud-Native Implementation Plan](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/CLOUD_NATIVE_IMPLEMENTATION_PLAN.md)
- [Cloud-Native Runbook](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/operations/CLOUD_NATIVE_RUNBOOK.md)
- [Architecture And Operating Model](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md)
- [Operations Playbook](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/operations/OPERATIONS_PLAYBOOK.md)

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
- Custom domain family:
  - `app.ceiora.com` for the frontend
  - `api.ceiora.com` for the serve API
  - `control.ceiora.com` for the control API
- Initial cloud access model:
  - internet-reachable frontend
  - internet-reachable serve API
  - internet-reachable control API, but still protected by operator credentials and not anonymous
- Production custom-domain routing will use a global external HTTPS load balancer with serverless NEGs, not Cloud Run preview domain mapping.
- Temporary smoke validation will use Cloud Run `run.app` hostnames before cutover to `app.ceiora.com`.
- The control API is intended for:
  - the frontend's server-side proxy routes, and
  - explicit operator or automation clients
  It is not intended for anonymous browser-direct usage.
- The first durable cloud job migration moves `serve-refresh` only.
- `core-weekly` and `cold-core` remain outside the first cloud job cutover unless this file is explicitly amended later.
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
  - [backend/main.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/main.py)
  - [backend/serve_main.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/serve_main.py)
  - [backend/control_main.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/control_main.py)
- frozen route matrix in [router_registry.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/api/router_registry.py)
- frontend split-origin helper in [_backend.ts](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/frontend/src/app/api/_backend.ts)
- refresh-status read ownership already split into a dedicated service
- refresh dispatch ownership already split into a dedicated service
- current local app/runtime bootstrap now has concrete owners:
  - [scripts/setup_local_env.sh](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/scripts/setup_local_env.sh)
  - [scripts/local_app/up.sh](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/scripts/local_app/up.sh)
  - [scripts/doctor.sh](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/scripts/doctor.sh)
- baseline container assets:
  - [backend/Dockerfile.serve](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/Dockerfile.serve)
  - [backend/Dockerfile.control](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/Dockerfile.control)
  - [frontend/Dockerfile](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/frontend/Dockerfile)

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
- [backend.main:app](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/main.py)

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
- [frontend/Dockerfile](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/frontend/Dockerfile)

### Cloud Serve API

Purpose:
- public/editor-facing read and holdings mutation surface
- stateless and Neon-backed

Entrypoint:
- [backend.serve_main:app](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/serve_main.py)

Hostname:
- `api.ceiora.com`

### Cloud Control API

Purpose:
- operator/control-plane API surface
- status, diagnostics, refresh dispatch

Entrypoint:
- [backend.control_main:app](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/backend/control_main.py)

Hostname:
- `control.ceiora.com`

### Cloud Job Surface

Purpose:
- durable background execution for control-plane work that should not live inside a request/response service

Initial expectation:
- Cloud Run Jobs are the likely long-running execution surface for:
  - `serve-refresh`
  - later, possibly `core-weekly`
  - later, possibly `cold-core`

This remains a migration phase, not yet a completed implementation.

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
- minimal logging/monitoring/uptime resources

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
- `OPERATOR_API_TOKEN`
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

Optional compatibility only:
- `REFRESH_API_TOKEN`

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
- `OPERATOR_API_TOKEN`
- `EDITOR_API_TOKEN`

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
- let the control API dispatch and observe jobs instead of owning the worker thread directly
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

- [ ] Run and record successful local builds for:
  - `backend/Dockerfile.serve`
  - `backend/Dockerfile.control`
  - `frontend/Dockerfile`
- [ ] Tighten container/runtime env contracts so Cloud Run deployment is deterministic.
- [ ] Implement and document the initial image build/publish path:
  - local Docker build
  - Artifact Registry push from the verified operator workstation
- [ ] Defer CI/CD image publishing until after the first cloud rollout is stable.
- [ ] Keep the local app path unchanged.

### Slice 4: Control-Plane Execution Migration

- [ ] Replace process-local refresh execution assumptions with durable dispatch/execution ownership.
- [ ] Add Cloud Run Job ownership to the Terraform plan, not as a later optional add-on.
- [ ] Freeze the first cloud job lane as `serve-refresh` only.
- [ ] Freeze the `serve-refresh` cloud prerequisite contract:
  - stable core package present
  - required source-sync state present
  - Neon-readiness satisfied
  - `security_master` parity satisfied
- [ ] Add the operator workflow cutover plan:
  - dispatch trigger path
  - execution status path
  - failure and retry handling
- [ ] Update the runbook and operator-facing docs when this cutover lands.

### Slice 5: Frontend + Serve Cloud Run Smoke Rollout

- [ ] Deploy frontend and serve first against default `run.app` hostnames for smoke validation.
- [ ] Freeze the temporary control-origin coexistence rule for this phase:
  - either operator/control routes are intentionally unavailable in the public smoke environment,
  - or the cloud frontend points `BACKEND_CONTROL_ORIGIN` at the cloud control smoke endpoint only after Slice 6 exists.
- [ ] Validate:
  - Neon-backed reads
  - split-origin proxy behavior
  - no local SQLite dependency in cloud mode
  - expected behavior when operator/control routes are not yet public
  - projection-only instruments and other fail-closed degraded states still behave correctly

### Slice 6: Control Cloud Run Rollout

- [ ] Deploy the control API after Slice 5 is implemented.
- [ ] Validate operator/control routes through `control.ceiora.com`.
- [ ] Confirm scale-to-zero behavior does not break job dispatch/status visibility.
- [ ] Keep operator credentials required in cloud mode.

### Slice 7: Custom Domains, Ingress, And Certificates

- [ ] Add DNS and certificate ownership.
- [ ] Implement the already chosen production ingress path for custom domains:
  - global external HTTPS load balancer
  - serverless NEGs
  - managed certificates or explicitly owned certificate path
- [ ] Validate:
  - `app.ceiora.com`
  - `api.ceiora.com`
  - `control.ceiora.com`
- [ ] Document any split between temporary `run.app` testing and final custom-domain ingress.

### Slice 8: Observability And Operations

- [ ] Add log retention expectations.
- [ ] Add minimal uptime checks or equivalent health validation.
- [ ] Add operator runbook steps for cloud troubleshooting.
- [ ] Update docs so the local app and cloud app are both clearly supported paths.

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
