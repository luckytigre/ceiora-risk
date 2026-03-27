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
- secret access bindings for the Cloud Run surfaces
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

Important ingress rule:
- this slice does not change the public `run.app` smoke posture of the Cloud Run services
- it only prepares the later custom-domain cutover path
- final-domain cutover must use a frontend image built against `https://api.ceiora.com`, not the earlier `run.app` smoke image

Observability prep owned here:
- `_Default` Cloud Logging retention
- no public uptime checks by default, to preserve scale-to-zero behavior
- `control.ceiora.com` remains an operator-token smoke target and is documented in the runbook instead of being a public uptime probe

Important Cloud Run billing rule:
- when a service defines `template.containers.resources`, set `cpu_idle = true` explicitly to preserve request-based billing
- direct `gcloud run deploy` workflows must also pass `--cpu-throttling`, or the live service can drift back to instance-based billing
- rollout verification should include `terraform plan`, `make cloud-request-billing-check`, `make smoke-check`, and `RUN_REFRESH_DISPATCH=1 make operator-check`
- for config-only Terraform changes in production, pin `frontend_image_ref`, `serve_image_ref`, and `control_image_ref` to the currently deployed image refs so the plan does not drift to `:latest`

Important frontend rule:
- the frontend image bakes `BACKEND_API_ORIGIN` at build time
- the Terraform `prod` root therefore treats `frontend_backend_api_origin` and `frontend_image_ref` as explicit rollout inputs
- the frontend Cloud Run service mirrors the same `BACKEND_API_ORIGIN` at runtime for Next server-side proxy helpers, but changing the service env alone does not retarget the compiled rewrite
- for final-domain rollout, the default stays `https://api.ceiora.com`
- for `run.app` smoke, rebuild and push a frontend image against the serve service's `run.app` URL, then override:
  - `frontend_image_ref`
  - `frontend_backend_api_origin`

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
