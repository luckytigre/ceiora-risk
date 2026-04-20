locals {
  name_prefix        = "ceiora-${var.environment}"
  registry_base      = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_registry_repository_id}"
  frontend_image_ref = var.frontend_image_ref != "" ? var.frontend_image_ref : "${local.registry_base}/frontend:${var.image_tag}"
  serve_image_ref    = var.serve_image_ref != "" ? var.serve_image_ref : "${local.registry_base}/serve:${var.image_tag}"
  control_image_ref  = var.control_image_ref != "" ? var.control_image_ref : "${local.registry_base}/control:${var.image_tag}"
  endpoint_mode      = trimspace(var.endpoint_mode)
  edge_enabled       = var.edge_enabled

  hostnames = {
    frontend = "app.${var.cloudflare_zone_name}"
    serve    = "api.${var.cloudflare_zone_name}"
    control  = "control.${var.cloudflare_zone_name}"
  }

  custom_domain_origins = {
    frontend = "https://${local.hostnames.frontend}"
    serve    = "https://${local.hostnames.serve}"
    control  = "https://${local.hostnames.control}"
  }

  normalized_frontend_public_origin          = trimsuffix(trimspace(var.frontend_public_origin), "/")
  normalized_frontend_backend_api_origin     = trimsuffix(trimspace(var.frontend_backend_api_origin), "/")
  normalized_frontend_backend_control_origin = trimsuffix(trimspace(var.frontend_backend_control_origin), "/")

  public_origins = {
    frontend = local.endpoint_mode == "run_app" ? local.normalized_frontend_public_origin : local.custom_domain_origins.frontend
    serve    = local.endpoint_mode == "run_app" ? local.normalized_frontend_backend_api_origin : local.custom_domain_origins.serve
    control  = local.endpoint_mode == "run_app" ? local.normalized_frontend_backend_control_origin : local.custom_domain_origins.control
  }

  frontend_backend_api_origin     = local.public_origins.serve
  frontend_backend_control_origin = local.public_origins.control
  frontend_public_origin          = local.public_origins.frontend
  neon_auth_allowed_emails_csv    = join(",", var.neon_auth_allowed_emails)
  neon_auth_bootstrap_admins_csv  = join(",", var.neon_auth_bootstrap_admins)

  public_cors_allow_origins = join(
    ",",
    distinct(
      compact(
        var.private_backend_invocation_enabled
        ? [
          local.public_origins.frontend,
          local.edge_enabled ? local.custom_domain_origins.frontend : null,
        ]
        : local.endpoint_mode == "run_app"
        ? [
          local.public_origins.frontend,
          local.edge_enabled ? local.custom_domain_origins.frontend : null,
        ]
        : [
          local.public_origins.frontend,
          local.public_origins.serve,
          local.public_origins.control,
        ]
      )
    ),
  )

  secret_ids = {
    neon_database_url        = "${local.name_prefix}-neon-database-url"
    operator_api_token       = "${local.name_prefix}-operator-api-token"
    editor_api_token         = "${local.name_prefix}-editor-api-token"
    shared_login_username    = "${local.name_prefix}-shared-login-username"
    shared_login_password    = "${local.name_prefix}-shared-login-password"
    session_secret           = "${local.name_prefix}-session-secret"
    primary_account_username = "${local.name_prefix}-primary-account-username"
  }

  service_accounts = {
    frontend = {
      account_id   = "${replace(local.name_prefix, "-", "")}frontend"
      display_name = "ceiora prod frontend"
      description  = "Cloud Run service account for the frontend app."
    }
    serve = {
      account_id   = "${replace(local.name_prefix, "-", "")}serve"
      display_name = "ceiora prod serve"
      description  = "Cloud Run service account for the serve API."
    }
    control = {
      account_id   = "${replace(local.name_prefix, "-", "")}control"
      display_name = "ceiora prod control"
      description  = "Cloud Run service account for the control API."
    }
    jobs = {
      account_id   = "${replace(local.name_prefix, "-", "")}jobs"
      display_name = "ceiora prod jobs"
      description  = "Cloud Run Jobs service account for control-plane execution."
    }
  }

  secret_accessors = {
    frontend_shared_login_username = {
      secret_key          = "shared_login_username"
      service_account_key = "frontend"
    }
    frontend_shared_login_password = {
      secret_key          = "shared_login_password"
      service_account_key = "frontend"
    }
    frontend_session_secret = {
      secret_key          = "session_secret"
      service_account_key = "frontend"
    }
    serve_session_secret = {
      secret_key          = "session_secret"
      service_account_key = "serve"
    }
    frontend_primary_account_username = {
      secret_key          = "primary_account_username"
      service_account_key = "frontend"
    }
    serve_neon = {
      secret_key          = "neon_database_url"
      service_account_key = "serve"
    }
    serve_operator = {
      secret_key          = "operator_api_token"
      service_account_key = "serve"
    }
    serve_editor = {
      secret_key          = "editor_api_token"
      service_account_key = "serve"
    }
    control_neon = {
      secret_key          = "neon_database_url"
      service_account_key = "control"
    }
    control_operator = {
      secret_key          = "operator_api_token"
      service_account_key = "control"
    }
    jobs_neon = {
      secret_key          = "neon_database_url"
      service_account_key = "jobs"
    }
    jobs_operator = {
      secret_key          = "operator_api_token"
      service_account_key = "jobs"
    }
  }
}
