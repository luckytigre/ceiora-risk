# Terraform Cloud Run Stack

This directory owns the GCP + Cloudflare infrastructure for the standalone cloud app.

Layout:
- `bootstrap/`
  - local-state root that creates the shared GCS Terraform state bucket
- `envs/prod/`
  - the only environment root for the current rollout
- `modules/`
  - narrow reusable Terraform modules owned by this repo

Rules:
- keep the local app outside Terraform
- do not commit `.terraform/`, `*.tfstate*`, or `*.tfplan`
- do not put runtime secret values in Terraform state
- use Secret Manager for runtime secrets and add secret versions out of band

## Bootstrap

Create the remote-state bucket first with local state:

```bash
cd infra/terraform/bootstrap
terraform init
terraform apply
```

The bootstrap root defaults to:
- project: `project-4e18de12-63a3-4206-aaa`
- region: `us-east4`
- bucket: `ceiora-risk-project-4e18de12-63a3-4206-aaa-tfstate`

## Prod Root

Initialize the real environment root with the generated bucket:

```bash
cd infra/terraform/envs/prod
cp backend.hcl.example backend.hcl
terraform init -backend-config=backend.hcl
cp terraform.tfvars.example terraform.tfvars
terraform plan
```

The `prod` root currently owns:
- required GCP API enablement
- Artifact Registry repository
- service accounts
- Secret Manager secret containers
- secret access bindings for the secret-consuming Cloud Run services and jobs
- Cloud Run service definitions for:
  - frontend
  - serve
  - control
- Cloud Run Job definition for `serve-refresh`
- Cloud Run IAM bindings for:
  - the public frontend `run.app` entrypoint
  - private frontend-to-backend invocation for `serve` and `control`
  - control-service invocation of the `serve-refresh` job
- ingress prep for final-domain cutover:
  - global HTTPS load balancer
  - serverless NEGs
  - managed certificate
  - Cloudflare DNS records for `app`, `api`, and `control`

Topology contract:
- `endpoint_mode=custom_domains`
  - rollback-only topology
  - requires `edge_enabled=true`
  - canonical public origins resolve to `app.ceiora.com`, `api.ceiora.com`, and `control.ceiora.com`
- `endpoint_mode=run_app`
  - current production topology
  - requires explicit `frontend_public_origin`, `frontend_backend_api_origin`, `frontend_backend_control_origin`, and pinned `*_image_ref` inputs
  - `edge_enabled=true` is the soak/rollback state
  - `edge_enabled=false` is the no-edge steady state
- `private_backend_invocation_enabled=true`
  - keeps the frontend public but repoints frontend runtime proxying to backend Cloud Run `run.app` service URLs
  - enables frontend-to-backend Cloud Run IAM auth
  - removes unauthenticated invoker access from `serve` and `control`
  - grants the frontend service account invoker on `serve` and `control`
  - current production also expects the Neon-auth env contract to be declared in Terraform for `frontend` and `serve`; image pins alone are not sufficient

Important ingress rule:
- the root now owns the edge through `module.edge`, with `moved` blocks preserving state addresses from the earlier root-level ingress resources
- `edge_enabled=true` keeps the shared HTTPS load balancer, NEGs, cert, and Cloudflare records provisioned
- `edge_enabled=false` tears down those edge resources but leaves the Cloud Run services and `run.app` origins intact
- `endpoint_mode=custom_domains` cannot be combined with `edge_enabled=false`

Build/deploy operator rule:
- `scripts/cloud/build_images.sh` and `scripts/cloud/build_and_push_images.sh` read `ENDPOINT_MODE`
- the default is `ENDPOINT_MODE=custom_domains`, which requires `BACKEND_API_ORIGIN=https://api.ceiora.com` for frontend builds and falls back to that value when the env var is omitted
- `ENDPOINT_MODE=run_app` is fail-closed for frontend builds and requires an explicit `BACKEND_API_ORIGIN=https://<serve-service>.run.app`
- `scripts/cloud/deploy_serve.sh` is intentionally guarded behind `ALLOW_DIRECT_SERVE_DEPLOY=1`
- that script is a serve-only Cloud Run drift path, validates `SERVICE_NAME` by requiring a `-serve` suffix, and must not be used for topology changes, `endpoint_mode` changes, or `run_app` cutovers
- `make cloud-run-app-contract` renders the exact `run_app` Terraform snippet from current prod outputs:
  - `RUN_APP_PHASE=soak` keeps `edge_enabled=true`
  - `RUN_APP_PHASE=no-edge` renders the final `edge_enabled=false` contract
- `make cloud-topology-check` reads current prod outputs and runs the correct live operator checks automatically for:
  - `custom_domains`
  - `run_app` soak
  - `run_app` no-edge
- `make cloud-topology-check` clears `RUN_REFRESH_DISPATCH` unless `TOPOLOGY_CHECK_RUN_REFRESH_DISPATCH=1` is set explicitly
- when dispatch is enabled on that wrapper, `TOPOLOGY_CHECK_DISPATCH_SURFACE=active|run_app|edge` selects the one surface that receives the real refresh dispatch
- both helper scripts can read a saved `terraform output -json` bundle through `PROD_TERRAFORM_OUTPUT_JSON=...` when the local prod root is not backend-initialized
- `make cloud-topology-check` reuses `scripts/operator_check.sh`; during `run_app` soak it runs the local pytest gate once, then reuses the live-only path for the custom-domain rollback check
- `make cloud-run-app-bundle` captures a distinct staged-cutover bundle under `backend/runtime/cloud_rollouts/`
- `make cloud-run-app-steady-state-bundle` captures the current `run_app` topology as a post-cutover pin bundle and emits current-image `no-edge` / rollback tfvars
  - use `run_app_current_topology.tfvars` from that bundle as the pinned contract for future config-only Terraform changes in no-edge steady state
- `CUTOVER_ACTION=bundle|build-frontend|plan|apply|verify make cloud-run-app-cutover` drives the staged `run_app` rollout from that bundle
- the bundle is a different operator artifact from `PROD_TERRAFORM_OUTPUT_JSON=...`:
  - `PROD_TERRAFORM_OUTPUT_JSON` is a read-only helper input
  - the rollout bundle is the staged cutover source that holds rollback and run_app base tfvars files
- targeted image applies invalidate older rollout bundles for later soak/no-edge plan/apply:
  - recapture a fresh bundle from the current live topology before reusing the helpers
  - `CUTOVER_ACTION=plan|apply` now fail closed if the bundle topology/image pins do not match the live Terraform outputs, unless `ALLOW_STALE_ROLLOUT_BUNDLE=1` is set explicitly
- `run_app` plan/apply phases fail closed until a run.app-built frontend image ref is available:
  - set `RUN_APP_FRONTEND_IMAGE_REF=...`
  - or run `CUTOVER_ACTION=build-frontend ROLLOUT_BUNDLE_DIR=... make cloud-run-app-cutover`
- `CUTOVER_ACTION=verify` wraps the topology-aware live smoke and optionally `make cloud-request-billing-check`, but `make smoke-check` remains a separate repo-side gate

Observability prep owned here:
- `_Default` Cloud Logging retention
- no public uptime checks by default, to preserve scale-to-zero behavior
- backend smoke targets should use the `run.app` service URLs from Terraform outputs; `control.ceiora.com` and `api.ceiora.com` are no longer live DNS aliases

Important Cloud Run billing rule:
- when a service defines `template.containers.resources`, set `cpu_idle = true` explicitly to preserve request-based billing
- direct `gcloud run deploy` workflows must also pass `--cpu-throttling`, or the live service can drift back to instance-based billing
- rollout verification should include `terraform plan`, `make cloud-request-billing-check`, `make smoke-check`, and either `RUN_REFRESH_DISPATCH=1 make operator-check` or `TOPOLOGY_CHECK_RUN_REFRESH_DISPATCH=1 make cloud-topology-check`
- for config-only Terraform changes in production, pin `frontend_image_ref`, `serve_image_ref`, and `control_image_ref` to the currently deployed image refs so the plan does not drift to `:latest`

Important frontend rule:
- the frontend no longer relies on a catch-all `next.config.js` `/api/*` rewrite
- `BACKEND_API_ORIGIN` and `BACKEND_CONTROL_ORIGIN` are now runtime proxy inputs for owned App Router handlers
- the Terraform `prod` root therefore still owns the frontend runtime proxy origins, but they are no longer compile-time rewrite inputs
- because Firebase Hosting only forwards the `__session` cookie on Cloud Run rewrites, the shared app session must use the `__session` cookie name
- the canonical topology-facing outputs are:
  - `endpoint_mode`
  - `edge_enabled`
  - `public_origins`
  - `frontend_build_contract`
  - `service_image_refs`
  - `service_image_refs_applied`
  - `control_surface_image_refs_applied`
  - `load_balancer_ip`
  - `load_balancer_dns_records`
  - `load_balancer_host_routing`
- `service_image_refs` are the configured Terraform pins; `service_image_refs_applied` are the live refs recorded in Terraform state for the Cloud Run services
- post-cutover operator capture/contract helpers now prefer the applied refs so a targeted control/job image hotfix does not silently get overwritten by a stale bundle
- the frontend service must not hold `OPERATOR_API_TOKEN` or `EDITOR_API_TOKEN`; privileged frontend `/api/*` routes must forward caller-supplied auth headers instead of injecting server-side secrets
- secret access bindings in the prod root should therefore exist only for secret-consuming backend services and jobs, not for the frontend service account
- exception: the frontend still consumes the shared session and shared-login compatibility secrets during the current Neon-auth transition, so those frontend secret bindings remain intentional until the shared-auth rollback path is formally removed
- for the current no-edge production shape, set:
  - `frontend_public_origin=https://app.ceiora.com`
  - `frontend_backend_api_origin=https://<serve-service>.run.app`
  - `frontend_backend_control_origin=https://<control-service>.run.app`
- the repo-owned Cloud Run image scripts mirror that contract:
  - `scripts/cloud/build_images.sh` / `scripts/cloud/build_and_push_images.sh` default `ENDPOINT_MODE=custom_domains`
  - `ENDPOINT_MODE=custom_domains` requires `BACKEND_API_ORIGIN=https://api.ceiora.com`; use `ENDPOINT_MODE=run_app` for explicit `run.app` frontend builds
  - `ENDPOINT_MODE=run_app` is fail-closed for frontend builds and requires explicit `BACKEND_API_ORIGIN=https://<serve-service>.run.app`
- `scripts/cloud/deploy_serve.sh` is an intentional serve-only drift path and requires `ALLOW_DIRECT_SERVE_DEPLOY=1`; do not use it for topology contract changes or `endpoint_mode` cutovers
- for `endpoint_mode=run_app`, provide the full explicit contract together:
  - `frontend_public_origin`
  - `frontend_backend_api_origin`
  - `frontend_backend_control_origin`
  - `frontend_image_ref`
  - `serve_image_ref`
  - `control_image_ref`
- the image-build scripts validate only the frontend build-side origin piece of that `run_app` contract; Terraform still validates the full topology contract and pinned image refs
- when building a frontend image for `endpoint_mode=run_app`, pass the same origin explicitly to the image script:
  - `ENDPOINT_MODE=run_app BACKEND_API_ORIGIN=https://<serve-service>.run.app BUILD_TARGETS=frontend make cloud-images-build`
- the `run_app` contract rejects partial overrides and rejects `:latest` image refs
- `frontend_build_contract.edge_enabled` shows whether the custom-domain edge is still present for rollback during a `run_app` soak
- when `edge_enabled=false`, `load_balancer_ip`, `load_balancer_dns_records`, and `load_balancer_host_routing` return `null`

Secret values are intentionally out of band. After the secret containers exist, add versions manually:

```bash
printf '%s' "$NEON_DATABASE_URL" | gcloud secrets versions add ceiora-prod-neon-database-url --data-file=-
printf '%s' "$OPERATOR_API_TOKEN" | gcloud secrets versions add ceiora-prod-operator-api-token --data-file=-
printf '%s' "$EDITOR_API_TOKEN" | gcloud secrets versions add ceiora-prod-editor-api-token --data-file=-
printf '%s' "$CEIORA_SHARED_LOGIN_USERNAME" | gcloud secrets versions add ceiora-prod-shared-login-username --data-file=-
printf '%s' "$CEIORA_SHARED_LOGIN_PASSWORD" | gcloud secrets versions add ceiora-prod-shared-login-password --data-file=-
printf '%s' "$CEIORA_SESSION_SECRET" | gcloud secrets versions add ceiora-prod-session-secret --data-file=-
printf '%s' "$CEIORA_PRIMARY_ACCOUNT_USERNAME" | gcloud secrets versions add ceiora-prod-primary-account-username --data-file=-
```

Cloudflare DNS access is expected through the provider environment contract:

```bash
export CLOUDFLARE_API_TOKEN="$(op item get 'Cloudflare - Ceiora Risk' --fields label=credential --reveal)"
```
