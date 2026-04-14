project_id                      = "project-4e18de12-63a3-4206-aaa"
environment                     = "prod"
region                          = "us-east4"
artifact_registry_repository_id = "ceiora-images"
cloudflare_zone_name            = "ceiora.com"
cloudflare_proxied              = false
endpoint_mode                   = "custom_domains"
edge_enabled                    = true

# Pinned to currently deployed tags.
# Update these when intentionally rolling a new image.
# frontend stays at :latest (no backend changes in Phase 3 Step 3 frontend image).
# serve and control pinned to 2012c3a (Phase 3 Step 3 guardrail + diagnostics fixes).
frontend_image_ref = "us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images/frontend:latest"
serve_image_ref    = "us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images/serve:2012c3a"
control_image_ref  = "us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images/control:ab737d3-stagemetrics2"
