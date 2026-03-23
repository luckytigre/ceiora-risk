terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

variable "project_id" {
  description = "GCP project id."
  type        = string
}

variable "secrets" {
  description = "Secret Manager secret ids keyed by logical secret name."
  type        = map(string)
}

resource "google_secret_manager_secret" "this" {
  for_each = var.secrets

  project   = var.project_id
  secret_id = each.value

  replication {
    auto {}
  }
}

output "secret_ids" {
  description = "Secret Manager secret ids keyed by logical secret name."
  value       = { for key, secret in google_secret_manager_secret.this : key => secret.secret_id }
}
