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

variable "services" {
  description = "Google APIs to enable."
  type        = set(string)
}

resource "google_project_service" "this" {
  for_each = var.services

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}
