locals {
  name_prefix = "ceiora-${var.environment}"

  hostnames = {
    frontend = "app.${var.cloudflare_zone_name}"
    serve    = "api.${var.cloudflare_zone_name}"
    control  = "control.${var.cloudflare_zone_name}"
  }

  secret_ids = {
    neon_database_url  = "${local.name_prefix}-neon-database-url"
    operator_api_token = "${local.name_prefix}-operator-api-token"
    editor_api_token   = "${local.name_prefix}-editor-api-token"
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
    frontend_operator = {
      secret_key          = "operator_api_token"
      service_account_key = "frontend"
    }
    frontend_editor = {
      secret_key          = "editor_api_token"
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
