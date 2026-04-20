resource "google_artifact_registry_repository_iam_member" "runtime_image_readers" {
  for_each = module.service_accounts.email_by_key

  project    = var.project_id
  location   = var.region
  repository = var.artifact_registry_repository_id
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${each.value}"

  depends_on = [module.artifact_registry, module.service_accounts]
}

resource "google_cloud_run_v2_service" "frontend" {
  name     = "${local.name_prefix}-frontend"
  project  = var.project_id
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  # Cloud Run v2 persists service-level zero-instance automatic scaling in
  # Terraform state separately from revision max-scale. Declare the zero values
  # explicitly so Terraform converges instead of planning a null reset.
  scaling {
    min_instance_count    = 0
    manual_instance_count = 0
  }

  template {
    service_account = module.service_accounts.email_by_key["frontend"]
    timeout         = "300s"

    scaling {
      max_instance_count = var.frontend_max_instances
    }

    containers {
      image = local.frontend_image_ref

      resources {
        cpu_idle = true
        limits = {
          cpu    = "1"
          memory = var.frontend_memory_limit
        }
      }

      ports {
        container_port = 3000
      }

      env {
        name  = "BACKEND_API_ORIGIN"
        value = var.private_backend_invocation_enabled ? google_cloud_run_v2_service.serve.uri : local.frontend_backend_api_origin
      }

      env {
        name  = "BACKEND_CONTROL_ORIGIN"
        value = var.private_backend_invocation_enabled ? google_cloud_run_v2_service.control.uri : local.frontend_backend_control_origin
      }

      env {
        name  = "CLOUD_RUN_BACKEND_IAM_AUTH"
        value = var.private_backend_invocation_enabled ? "true" : "false"
      }

      env {
        name  = "APP_AUTH_PROVIDER"
        value = var.app_auth_provider
      }

      env {
        name  = "APP_ACCOUNT_ENFORCEMENT_ENABLED"
        value = var.app_account_enforcement_enabled ? "true" : "false"
      }

      env {
        name  = "APP_SHARED_AUTH_ACCEPT_LEGACY"
        value = var.app_shared_auth_accept_legacy ? "true" : "false"
      }

      env {
        name  = "NEON_AUTH_BASE_URL"
        value = var.neon_auth_base_url
      }

      env {
        name  = "NEON_AUTH_ISSUER"
        value = var.neon_auth_issuer
      }

      env {
        name  = "NEON_AUTH_AUDIENCE"
        value = var.neon_auth_audience
      }

      env {
        name  = "NEON_AUTH_JWKS_JSON"
        value = var.neon_auth_jwks_json
      }

      env {
        name  = "NEON_AUTH_ALLOWED_EMAILS"
        value = local.neon_auth_allowed_emails_csv
      }

      env {
        name  = "NEON_AUTH_BOOTSTRAP_ADMINS"
        value = local.neon_auth_bootstrap_admins_csv
      }

      env {
        name = "CEIORA_SHARED_LOGIN_USERNAME"
        value_source {
          secret_key_ref {
            secret  = module.secret_manager.secret_ids["shared_login_username"]
            version = "latest"
          }
        }
      }

      env {
        name = "CEIORA_SHARED_LOGIN_PASSWORD"
        value_source {
          secret_key_ref {
            secret  = module.secret_manager.secret_ids["shared_login_password"]
            version = "latest"
          }
        }
      }

      env {
        name = "CEIORA_SESSION_SECRET"
        value_source {
          secret_key_ref {
            secret  = module.secret_manager.secret_ids["session_secret"]
            version = "latest"
          }
        }
      }

      env {
        name = "CEIORA_PRIMARY_ACCOUNT_USERNAME"
        value_source {
          secret_key_ref {
            secret  = module.secret_manager.secret_ids["primary_account_username"]
            version = "latest"
          }
        }
      }

    }
  }

  depends_on = [
    module.project_services,
    module.service_accounts,
    google_artifact_registry_repository_iam_member.runtime_image_readers,
  ]
}

resource "google_cloud_run_v2_service" "serve" {
  name     = "${local.name_prefix}-serve"
  project  = var.project_id
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  scaling {
    min_instance_count    = 0
    manual_instance_count = 0
  }

  template {
    service_account = module.service_accounts.email_by_key["serve"]
    timeout         = "300s"

    scaling {
      max_instance_count = var.serve_max_instances
    }

    containers {
      image = local.serve_image_ref

      resources {
        cpu_idle = true
        limits = {
          cpu    = "1"
          memory = var.serve_memory_limit
        }
      }

      ports {
        container_port = 8000
      }

      env {
        name  = "APP_RUNTIME_ROLE"
        value = "cloud-serve"
      }

      env {
        name  = "DATA_BACKEND"
        value = "neon"
      }

      env {
        name  = "NEON_AUTHORITATIVE_REBUILDS"
        value = "true"
      }

      env {
        name  = "CORS_ALLOW_ORIGINS"
        value = local.public_cors_allow_origins
      }

      env {
        name  = "APP_ACCOUNT_ENFORCEMENT_ENABLED"
        value = var.app_account_enforcement_enabled ? "true" : "false"
      }

      env {
        name  = "APP_AUTH_BOOTSTRAP_ENABLED"
        value = var.app_auth_bootstrap_enabled ? "true" : "false"
      }

      env {
        name  = "APP_ADMIN_SETTINGS_ENABLED"
        value = var.app_admin_settings_enabled ? "true" : "false"
      }

      env {
        name  = "APP_SHARED_AUTH_ACCEPT_LEGACY"
        value = var.app_shared_auth_accept_legacy ? "true" : "false"
      }

      env {
        name  = "NEON_AUTH_ALLOWED_EMAILS"
        value = local.neon_auth_allowed_emails_csv
      }

      env {
        name  = "NEON_AUTH_BOOTSTRAP_ADMINS"
        value = local.neon_auth_bootstrap_admins_csv
      }

      env {
        name = "NEON_DATABASE_URL"
        value_source {
          secret_key_ref {
            secret  = module.secret_manager.secret_ids["neon_database_url"]
            version = "latest"
          }
        }
      }

      env {
        name = "OPERATOR_API_TOKEN"
        value_source {
          secret_key_ref {
            secret  = module.secret_manager.secret_ids["operator_api_token"]
            version = "latest"
          }
        }
      }

      env {
        name = "CEIORA_SESSION_SECRET"
        value_source {
          secret_key_ref {
            secret  = module.secret_manager.secret_ids["session_secret"]
            version = "latest"
          }
        }
      }

      env {
        name = "EDITOR_API_TOKEN"
        value_source {
          secret_key_ref {
            secret  = module.secret_manager.secret_ids["editor_api_token"]
            version = "latest"
          }
        }
      }
    }
  }

  depends_on = [
    module.project_services,
    module.service_accounts,
    module.secret_manager,
    google_secret_manager_secret_iam_member.secret_accessor,
    google_artifact_registry_repository_iam_member.runtime_image_readers,
  ]
}

resource "google_cloud_run_v2_service" "control" {
  name     = "${local.name_prefix}-control"
  project  = var.project_id
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  scaling {
    min_instance_count    = 0
    manual_instance_count = 0
  }

  template {
    service_account = module.service_accounts.email_by_key["control"]
    timeout         = "300s"

    scaling {
      max_instance_count = var.control_max_instances
    }

    containers {
      image = local.control_image_ref

      resources {
        cpu_idle = true
        limits = {
          cpu    = "1"
          memory = var.control_memory_limit
        }
      }

      ports {
        container_port = 8000
      }

      env {
        name  = "APP_RUNTIME_ROLE"
        value = "cloud-serve"
      }

      env {
        name  = "DATA_BACKEND"
        value = "neon"
      }

      env {
        name  = "NEON_AUTHORITATIVE_REBUILDS"
        value = "true"
      }

      env {
        name  = "CORS_ALLOW_ORIGINS"
        value = local.public_cors_allow_origins
      }

      env {
        name  = "CLOUD_RUN_JOBS_ENABLED"
        value = "true"
      }

      env {
        name  = "CLOUD_RUN_PROJECT_ID"
        value = var.project_id
      }

      env {
        name  = "CLOUD_RUN_REGION"
        value = var.region
      }

      env {
        name  = "SERVE_REFRESH_CLOUD_RUN_JOB_NAME"
        value = google_cloud_run_v2_job.serve_refresh.name
      }

      env {
        name  = "CORE_WEEKLY_CLOUD_RUN_JOB_NAME"
        value = google_cloud_run_v2_job.core_weekly.name
      }

      env {
        name  = "COLD_CORE_CLOUD_RUN_JOB_NAME"
        value = google_cloud_run_v2_job.cold_core.name
      }

      env {
        name  = "CPAR_BUILD_CLOUD_RUN_JOB_NAME"
        value = google_cloud_run_v2_job.cpar_build.name
      }

      env {
        name = "NEON_DATABASE_URL"
        value_source {
          secret_key_ref {
            secret  = module.secret_manager.secret_ids["neon_database_url"]
            version = "latest"
          }
        }
      }

      env {
        name = "OPERATOR_API_TOKEN"
        value_source {
          secret_key_ref {
            secret  = module.secret_manager.secret_ids["operator_api_token"]
            version = "latest"
          }
        }
      }
    }
  }

  depends_on = [
    module.project_services,
    module.service_accounts,
    module.secret_manager,
    google_secret_manager_secret_iam_member.secret_accessor,
    google_artifact_registry_repository_iam_member.runtime_image_readers,
  ]
}

resource "google_cloud_run_v2_service_iam_member" "public_invoker" {
  for_each = (
    var.private_backend_invocation_enabled
    ? {
      frontend = google_cloud_run_v2_service.frontend.name
    }
    : {
      frontend = google_cloud_run_v2_service.frontend.name
      serve    = google_cloud_run_v2_service.serve.name
      control  = google_cloud_run_v2_service.control.name
    }
  )

  project  = var.project_id
  location = var.region
  name     = each.value
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_cloud_run_v2_service_iam_member" "frontend_private_backend_invoker" {
  for_each = (
    var.private_backend_invocation_enabled
    ? {
      serve   = google_cloud_run_v2_service.serve.name
      control = google_cloud_run_v2_service.control.name
    }
    : {}
  )

  project  = var.project_id
  location = var.region
  name     = each.value
  role     = "roles/run.invoker"
  member   = "serviceAccount:${module.service_accounts.email_by_key["frontend"]}"
}

resource "google_cloud_run_v2_job_iam_member" "control_dispatch_invoker" {
  for_each = {
    serve_refresh = google_cloud_run_v2_job.serve_refresh.name
    core_weekly   = google_cloud_run_v2_job.core_weekly.name
    cold_core     = google_cloud_run_v2_job.cold_core.name
    cpar_build    = google_cloud_run_v2_job.cpar_build.name
  }

  project  = var.project_id
  location = var.region
  name     = each.value
  role     = "roles/run.jobsExecutorWithOverrides"
  member   = "serviceAccount:${module.service_accounts.email_by_key["control"]}"
}

resource "google_cloud_run_v2_job_iam_member" "control_execution_viewer" {
  for_each = {
    serve_refresh = google_cloud_run_v2_job.serve_refresh.name
    core_weekly   = google_cloud_run_v2_job.core_weekly.name
    cold_core     = google_cloud_run_v2_job.cold_core.name
    cpar_build    = google_cloud_run_v2_job.cpar_build.name
  }

  project  = var.project_id
  location = var.region
  name     = each.value
  role     = "roles/run.viewer"
  member   = "serviceAccount:${module.service_accounts.email_by_key["control"]}"
}
