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

variable "accounts" {
  description = "Service accounts keyed by logical surface."
  type = map(object({
    account_id   = string
    display_name = string
    description  = string
  }))
}

resource "google_service_account" "this" {
  for_each = var.accounts

  project      = var.project_id
  account_id   = each.value.account_id
  display_name = each.value.display_name
  description  = each.value.description
}

output "email_by_key" {
  description = "Service account emails keyed by logical surface."
  value       = { for key, account in google_service_account.this : key => account.email }
}
