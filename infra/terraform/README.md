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
