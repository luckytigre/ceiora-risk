output "artifact_registry_repository" {
  description = "Artifact Registry repository for cloud app images."
  value       = module.artifact_registry.repository_url
}

output "service_account_emails" {
  description = "Service account emails for the cloud stack."
  value       = module.service_accounts.email_by_key
}

output "secret_ids" {
  description = "Secret Manager secret ids created for the cloud stack."
  value       = module.secret_manager.secret_ids
}

output "hostnames" {
  description = "Reserved custom-domain hostnames for the cloud stack. These are live only when edge_enabled=true."
  value       = local.hostnames
}

output "service_names" {
  description = "Cloud Run service names for the cloud app surfaces."
  value = {
    frontend = google_cloud_run_v2_service.frontend.name
    serve    = google_cloud_run_v2_service.serve.name
    control  = google_cloud_run_v2_service.control.name
  }
}

output "service_urls" {
  description = "Cloud Run run.app URLs for the live services. These remain useful in every topology mode."
  value = {
    frontend = google_cloud_run_v2_service.frontend.uri
    serve    = google_cloud_run_v2_service.serve.uri
    control  = google_cloud_run_v2_service.control.uri
  }
}

output "endpoint_mode" {
  description = "Current public topology contract for the cloud app."
  value       = local.endpoint_mode
}

output "edge_enabled" {
  description = "Whether the custom-domain edge resources are currently provisioned."
  value       = local.edge_enabled
}

output "private_backend_invocation_enabled" {
  description = "Whether the frontend currently invokes serve/control privately through Cloud Run IAM-authenticated service URLs."
  value       = var.private_backend_invocation_enabled
}

output "public_origins" {
  description = "Canonical public origins for the current topology contract. In run_app mode these must be explicit inputs."
  value       = local.public_origins
}

output "service_image_refs" {
  description = "Image refs pinned into the Terraform Cloud Run service definitions. endpoint_mode=run_app requires these to be explicit inputs."
  value = {
    frontend = local.frontend_image_ref
    serve    = local.serve_image_ref
    control  = local.control_image_ref
  }
}

output "service_image_refs_applied" {
  description = "Image refs currently recorded in Terraform state for the live Cloud Run service resources. Prefer these for post-targeted-apply operator capture and verification."
  value = {
    frontend = google_cloud_run_v2_service.frontend.template[0].containers[0].image
    serve    = google_cloud_run_v2_service.serve.template[0].containers[0].image
    control  = google_cloud_run_v2_service.control.template[0].containers[0].image
  }
}

output "control_surface_image_refs_applied" {
  description = "Applied image refs for the control service and every control-surface Cloud Run Job. These should normally match because they are driven by the same control image input."
  value = {
    service           = google_cloud_run_v2_service.control.template[0].containers[0].image
    serve_refresh_job = google_cloud_run_v2_job.serve_refresh.template[0].template[0].containers[0].image
    core_weekly_job   = google_cloud_run_v2_job.core_weekly.template[0].template[0].containers[0].image
    cold_core_job     = google_cloud_run_v2_job.cold_core.template[0].template[0].containers[0].image
    cpar_build_job    = google_cloud_run_v2_job.cpar_build.template[0].template[0].containers[0].image
  }
}

output "frontend_build_contract" {
  description = "Frontend build/runtime proxy inputs. BACKEND_API_ORIGIN must match the image build input; changing the service env alone will not retarget the compiled rewrite."
  value = {
    endpoint_mode          = local.endpoint_mode
    edge_enabled           = local.edge_enabled
    public_frontend_origin = local.frontend_public_origin
    build_api_origin       = local.frontend_backend_api_origin
    runtime_control_origin = local.frontend_backend_control_origin
  }
}

output "auth_runtime_contract" {
  description = "Non-secret auth/runtime contract values that should stay visible in Terraform state and outputs."
  value = {
    APP_AUTH_PROVIDER               = var.app_auth_provider
    APP_ACCOUNT_ENFORCEMENT_ENABLED = var.app_account_enforcement_enabled
    APP_AUTH_BOOTSTRAP_ENABLED      = var.app_auth_bootstrap_enabled
    APP_ADMIN_SETTINGS_ENABLED      = var.app_admin_settings_enabled
    APP_SHARED_AUTH_ACCEPT_LEGACY   = var.app_shared_auth_accept_legacy
    NEON_AUTH_BASE_URL              = var.neon_auth_base_url
    NEON_AUTH_ISSUER                = var.neon_auth_issuer
    NEON_AUTH_AUDIENCE              = var.neon_auth_audience
    NEON_AUTH_ALLOWED_EMAILS        = var.neon_auth_allowed_emails
    NEON_AUTH_BOOTSTRAP_ADMINS      = var.neon_auth_bootstrap_admins
  }
}

output "serve_refresh_job_name" {
  description = "Cloud Run Job name for serve-refresh execution."
  value       = google_cloud_run_v2_job.serve_refresh.name
}

output "control_job_names" {
  description = "Cloud Run Job names owned by the control surface."
  value = {
    serve_refresh = google_cloud_run_v2_job.serve_refresh.name
    core_weekly   = google_cloud_run_v2_job.core_weekly.name
    cold_core     = google_cloud_run_v2_job.cold_core.name
    cpar_build    = google_cloud_run_v2_job.cpar_build.name
  }
}

output "load_balancer_ip" {
  description = "Global load balancer IPv4 address for app/api/control when the custom-domain edge is in use; null when edge_enabled=false."
  value       = module.edge.load_balancer_ip
}

output "load_balancer_dns_records" {
  description = "Cloudflare-managed DNS records for the custom-domain edge; null when edge_enabled=false."
  value       = module.edge.load_balancer_dns_records
}

output "load_balancer_host_routing" {
  description = "Host-based routing contract for the shared HTTPS load balancer when the edge is enabled; null when edge_enabled=false."
  value       = module.edge.load_balancer_host_routing
}

output "control_service_job_env" {
  description = "Environment values the control service will need for Cloud Run Job dispatch."
  value = {
    CLOUD_RUN_JOBS_ENABLED           = "true"
    CLOUD_RUN_PROJECT_ID             = var.project_id
    CLOUD_RUN_REGION                 = var.region
    SERVE_REFRESH_CLOUD_RUN_JOB_NAME = google_cloud_run_v2_job.serve_refresh.name
    CORE_WEEKLY_CLOUD_RUN_JOB_NAME   = google_cloud_run_v2_job.core_weekly.name
    COLD_CORE_CLOUD_RUN_JOB_NAME     = google_cloud_run_v2_job.cold_core.name
    CPAR_BUILD_CLOUD_RUN_JOB_NAME    = google_cloud_run_v2_job.cpar_build.name
  }
}
