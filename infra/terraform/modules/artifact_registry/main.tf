terraform {
  required_providers {
    google-beta = {
      source = "hashicorp/google-beta"
    }
  }
}

variable "project_id" {
  description = "GCP project id."
  type        = string
}

variable "region" {
  description = "Artifact Registry region."
  type        = string
}

variable "repository_id" {
  description = "Artifact Registry repository id."
  type        = string
}

variable "description" {
  description = "Artifact Registry repository description."
  type        = string
  default     = ""
}

resource "google_artifact_registry_repository" "this" {
  provider      = google-beta
  project       = var.project_id
  location      = var.region
  repository_id = var.repository_id
  description   = var.description
  format        = "DOCKER"
}

output "repository_url" {
  description = "Base Artifact Registry URL for the repository."
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.this.repository_id}"
}
