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
  - public `run.app` smoke access
  - control-service invocation of the `serve-refresh` job
- ingress prep for final-domain cutover:
  - global HTTPS load balancer
  - serverless NEGs
  - managed certificate
  - Cloudflare DNS records for `app`, `api`, and `control`

Topology contract:
- `endpoint_mode=custom_domains`
  - current default
  - requires `edge_enabled=true`
  - canonical public origins resolve to `app.ceiora.com`, `api.ceiora.com`, and `control.ceiora.com`
- `endpoint_mode=run_app`
  - explicit alternate public contract
  - requires explicit `frontend_public_origin`, `frontend_backend_api_origin`, `frontend_backend_control_origin`, and pinned `*_image_ref` inputs
  - `edge_enabled=true` is the soak/rollback state
  - `edge_enabled=false` is the no-edge steady state

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

Observability prep owned here:
- `_Default` Cloud Logging retention
- no public uptime checks by default, to preserve scale-to-zero behavior
- `control.ceiora.com` remains an operator-token smoke target and is documented in the runbook instead of being a public uptime probe

Important Cloud Run billing rule:
- when a service defines `template.containers.resources`, set `cpu_idle = true` explicitly to preserve request-based billing
- direct `gcloud run deploy` workflows must also pass `--cpu-throttling`, or the live service can drift back to instance-based billing
- rollout verification should include `terraform plan`, `make cloud-request-billing-check`, `make smoke-check`, and either `RUN_REFRESH_DISPATCH=1 make operator-check` or `TOPOLOGY_CHECK_RUN_REFRESH_DISPATCH=1 make cloud-topology-check`
- for config-only Terraform changes in production, pin `frontend_image_ref`, `serve_image_ref`, and `control_image_ref` to the currently deployed image refs so the plan does not drift to `:latest`

Important frontend rule:
- the frontend image bakes `BACKEND_API_ORIGIN` at build time
- the Terraform `prod` root therefore treats `frontend_backend_api_origin` and `frontend_image_ref` as explicit rollout inputs
- the frontend Cloud Run service mirrors the same `BACKEND_API_ORIGIN` at runtime for Next server-side proxy helpers, but changing the service env alone does not retarget the compiled rewrite
- the canonical topology-facing outputs are:
  - `endpoint_mode`
  - `edge_enabled`
  - `public_origins`
  - `frontend_build_contract`
  - `service_image_refs`
  - `load_balancer_ip`
  - `load_balancer_dns_records`
  - `load_balancer_host_routing`
- the frontend service must not hold `OPERATOR_API_TOKEN` or `EDITOR_API_TOKEN`; privileged frontend `/api/*` routes must forward caller-supplied auth headers instead of injecting server-side secrets
- secret access bindings in the prod root should therefore exist only for secret-consuming backend services and jobs, not for the frontend service account
- for final-domain rollout, the default stays `https://api.ceiora.com`
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
```

Cloudflare DNS access is expected through the provider environment contract:

```bash
export CLOUDFLARE_API_TOKEN="$(op item get 'Cloudflare - Ceiora Risk' --fields label=credential)"
```
