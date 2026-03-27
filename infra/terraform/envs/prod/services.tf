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

  template {
    service_account = module.service_accounts.email_by_key["frontend"]
    timeout         = "300s"

    scaling {
      min_instance_count = 0
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

      # The frontend image bakes BACKEND_API_ORIGIN into next.config rewrites.
      # Mirror the same value at runtime for the Next server-side proxy helpers.
      env {
        name  = "BACKEND_API_ORIGIN"
        value = local.frontend_backend_api_origin
      }

      env {
        name  = "BACKEND_CONTROL_ORIGIN"
        value = local.frontend_backend_control_origin
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

resource "google_cloud_run_v2_service" "serve" {
  name     = "${local.name_prefix}-serve"
  project  = var.project_id
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = module.service_accounts.email_by_key["serve"]
    timeout         = "300s"

    scaling {
      min_instance_count = 0
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

  template {
    service_account = module.service_accounts.email_by_key["control"]
    timeout         = "300s"

    scaling {
      min_instance_count = 0
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
  for_each = {
    frontend = google_cloud_run_v2_service.frontend.name
    serve    = google_cloud_run_v2_service.serve.name
    control  = google_cloud_run_v2_service.control.name
  }

  project  = var.project_id
  location = var.region
  name     = each.value
  role     = "roles/run.invoker"
  member   = "allUsers"
}

resource "google_cloud_run_v2_job_iam_member" "control_dispatch_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.serve_refresh.name
  role     = "roles/run.jobsExecutorWithOverrides"
  member   = "serviceAccount:${module.service_accounts.email_by_key["control"]}"
}

resource "google_cloud_run_v2_job_iam_member" "control_execution_viewer" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.serve_refresh.name
  role     = "roles/run.viewer"
  member   = "serviceAccount:${module.service_accounts.email_by_key["control"]}"
}
