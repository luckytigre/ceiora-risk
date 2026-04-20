project_id                         = "project-4e18de12-63a3-4206-aaa"
environment                        = "prod"
region                             = "us-east4"
artifact_registry_repository_id    = "ceiora-images"
cloudflare_zone_name               = "ceiora.com"
cloudflare_proxied                 = false
endpoint_mode                      = "run_app"
edge_enabled                       = false
frontend_public_origin             = "https://app.ceiora.com"
frontend_backend_api_origin        = "https://ceiora-prod-serve-i5znti5joq-uk.a.run.app"
frontend_backend_control_origin    = "https://ceiora-prod-control-i5znti5joq-uk.a.run.app"
private_backend_invocation_enabled = true

# Pinned rollout image refs.
# Update these only when intentionally publishing a new service build.
frontend_image_ref = "us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images/frontend:fb70615-runtime"
serve_image_ref    = "us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images/serve:fb70615-runtime"
control_image_ref  = "us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images/control:c009762-rollup4"

app_auth_provider               = "neon"
app_account_enforcement_enabled = true
app_auth_bootstrap_enabled      = true
app_admin_settings_enabled      = true
app_shared_auth_accept_legacy   = false

neon_auth_base_url         = "https://ep-ancient-mode-ai9rw5ae.neonauth.c-4.us-east-1.aws.neon.tech/neondb/auth"
neon_auth_issuer           = "https://ep-ancient-mode-ai9rw5ae.neonauth.c-4.us-east-1.aws.neon.tech/neondb/auth"
neon_auth_audience         = "https://ep-ancient-mode-ai9rw5ae.neonauth.c-4.us-east-1.aws.neon.tech"
neon_auth_jwks_json        = "{\"keys\":[{\"kid\":\"2cb9d9bd-5b83-4e06-abf6-981838f6c8be\",\"crv\":\"Ed25519\",\"x\":\"xbAMx6hLqh03W421gG0wqEirM4EPamhRJTIFPx5y2NM\",\"kty\":\"OKP\"}]}"
neon_auth_allowed_emails   = ["shaun.skc@gmail.com", "shauny27@gmail.com"]
neon_auth_bootstrap_admins = ["shaun.skc@gmail.com", "shauny27@gmail.com"]
