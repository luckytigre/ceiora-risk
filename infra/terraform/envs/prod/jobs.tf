resource "google_cloud_run_v2_job" "serve_refresh" {
  provider = google-beta

  name     = "${local.name_prefix}-serve-refresh"
  project  = var.project_id
  location = var.region

  deletion_protection = false

  template {
    template {
      service_account = module.service_accounts.email_by_key["jobs"]
      timeout         = "3600s"

      containers {
        image   = local.control_image_ref
        command = ["python", "-m", "backend.scripts.run_refresh_job"]

        resources {
          limits = {
            cpu    = "1"
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
