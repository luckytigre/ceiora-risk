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
- Custom domain family:
  - `ceiora.com` for the frontend
  - `api.ceiora.com` for the serve API
  - `control.ceiora.com` for the control API
- Initial cloud access model:
  - internet-reachable frontend
  - internet-reachable serve API
  - internet-reachable control API, but still protected by operator credentials and not anonymous
- Production custom-domain routing will use a global external HTTPS load balancer with serverless NEGs, not Cloud Run preview domain mapping.
- The control API is intended for:
  - the frontend's server-side proxy routes, and
  - explicit operator or automation clients
  It is not intended for anonymous browser-direct usage.
- The first durable cloud job migration moves `serve-refresh` only.
- `core-weekly` and `cold-core` remain outside the first cloud job cutover unless this file is explicitly amended later.

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

### Cloud Frontend

Purpose:
- public Next.js app on `ceiora.com`
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
- Do not finalize the region until Neon proximity is checked.
- Initial candidates:
  - `us-east5`
  - `us-east4`
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
- minimal logging/monitoring/uptime resources

Terraform should not own:
- the local LSEG machine
- local SQLite artifacts
- local-only launcher scripts

## Environment And Secret Contract

### Backend serve

Required:
- `APP_RUNTIME_ROLE=cloud-serve`
- `DATA_BACKEND=neon`
- `NEON_DATABASE_URL`
- `OPERATOR_API_TOKEN`
- `EDITOR_API_TOKEN`

Expected:
- no local refresh execution ownership
- no local SQLite serving authority

### Backend control

Required:
- `APP_RUNTIME_ROLE=cloud-serve`
- `DATA_BACKEND=neon`
- `NEON_DATABASE_URL`
- `OPERATOR_API_TOKEN`

Optional compatibility only:
- `REFRESH_API_TOKEN`

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

Do not treat this as optional if the goal is a durable cloud-native runtime.

## Planned Slices

### Slice 1: Freeze Cloud Topology And Terraform Conventions

- [ ] Freeze the canonical cloud topology:
  - local app
  - Cloud Run frontend
  - Cloud Run serve
  - Cloud Run control
  - later Cloud Run Jobs
- [ ] Freeze the hostname plan:
  - `ceiora.com`
  - `api.ceiora.com`
  - `control.ceiora.com`
- [ ] Freeze the single-environment naming convention as `prod`.
- [ ] Freeze the initial auth model for public vs operator surfaces.
- [ ] Update `.env.example` so the split-origin and token contract is explicit.

### Slice 2: Terraform Foundation

- [ ] Add a dedicated Terraform root under a durable repo-owned path.
- [ ] Freeze the bootstrap rule for remote Terraform state:
  - either a tiny bootstrap root with local state creates the state bucket first,
  - or a one-time manual bootstrap step is documented and accepted.
- [ ] Add provider/version pinning.
- [ ] Add remote state guidance and backend configuration shape.
- [ ] Add project-service enablement for the required GCP APIs.
- [ ] Add Artifact Registry ownership.
- [ ] Add Secret Manager ownership.
- [ ] Add service-account and IAM ownership.
- [ ] Add the custom-domain routing foundation:
  - global external HTTPS load balancer
  - serverless NEGs
  - certificate ownership
  - DNS ownership boundary

### Slice 3: Container Build And Runtime Contract Hardening

- [ ] Validate that the existing Dockerfiles build cleanly in a Docker-enabled environment.
- [ ] Tighten container/runtime env contracts so Cloud Run deployment is deterministic.
- [ ] Decide and document the image build/publish path:
  - manual build/push
  - Cloud Build
  - or GitHub Actions publishing into Artifact Registry
- [ ] Keep the local app path unchanged.

### Slice 4: Control-Plane Execution Migration

- [ ] Replace process-local refresh execution assumptions with durable dispatch/execution ownership.
- [ ] Add Cloud Run Job ownership to the Terraform plan, not as a later optional add-on.
- [ ] Freeze the first cloud job lane as `serve-refresh` only.
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
  - `ceiora.com`
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
- Control-plane migration:
  - dispatch/status tests
  - background execution ownership tests

## Open Questions

- Which GCP project ID will own the first rollout?
- Which east-coast region best matches Neon latency and Cloud Run feature support?
- Should the frontend live on apex `ceiora.com` immediately, or should it start on `app.ceiora.com` and move later?

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
