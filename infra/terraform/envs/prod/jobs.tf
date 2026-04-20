resource "google_cloud_run_v2_job" "core_weekly" {
  provider = google-beta

  name     = "${local.name_prefix}-core-weekly"
  project  = var.project_id
  location = var.region

  deletion_protection = false

  template {
    template {
      service_account = module.service_accounts.email_by_key["jobs"]
      max_retries     = 1
      timeout         = "7200s"

      containers {
        image   = local.control_image_ref
        command = ["python", "-m", "backend.scripts.run_refresh_job"]

        resources {
          limits = {
            cpu    = "8"
            memory = "32Gi"
          }
        }

        env {
          name  = "APP_RUNTIME_ROLE"
          value = "cloud-job"
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
          name  = "REFRESH_PROFILE"
          value = "core-weekly"
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
      }
    }
  }
}

resource "google_cloud_run_v2_job" "cold_core" {
  provider = google-beta

  name     = "${local.name_prefix}-cold-core"
  project  = var.project_id
  location = var.region

  deletion_protection = false

  template {
    template {
      service_account = module.service_accounts.email_by_key["jobs"]
      max_retries     = 0
      timeout         = "9000s"

      containers {
        image   = local.control_image_ref
        command = ["python", "-m", "backend.scripts.run_refresh_job"]

        resources {
          limits = {
            cpu    = "8"
            memory = "32Gi"
          }
        }

        env {
          name  = "APP_RUNTIME_ROLE"
          value = "cloud-job"
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
          name  = "REFRESH_PROFILE"
          value = "cold-core"
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
      }
    }
  }
}

resource "google_cloud_run_v2_job" "cpar_build" {
  provider = google-beta

  name     = "${local.name_prefix}-cpar-build"
  project  = var.project_id
  location = var.region

  deletion_protection = false

  template {
    template {
      service_account = module.service_accounts.email_by_key["jobs"]
      max_retries     = 3
      timeout         = "3600s"

      containers {
        image   = local.control_image_ref
        command = ["python", "-m", "backend.scripts.run_cpar_pipeline_job"]

        resources {
          limits = {
            cpu    = "2"
            memory = "4Gi"
          }
        }

        env {
          name  = "APP_RUNTIME_ROLE"
          value = "cloud-job"
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
          name  = "CPAR_PROFILE"
          value = "cpar-weekly"
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
      }
    }
  }
}

resource "google_cloud_run_v2_job" "serve_refresh" {
  provider = google-beta

  name     = "${local.name_prefix}-serve-refresh"
  project  = var.project_id
  location = var.region

  deletion_protection = false

  template {
    template {
      service_account = module.service_accounts.email_by_key["jobs"]
      max_retries     = 0
      timeout         = "3600s"

      containers {
        image   = local.control_image_ref
        command = ["python", "-m", "backend.scripts.run_refresh_job"]

        resources {
          limits = {
            cpu    = "2"
            memory = "4Gi"
          }
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
          name  = "REFRESH_PROFILE"
          value = "serve-refresh"
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
      }
    }
  }
}
