output "artifact_registry_repository" {
  description = "Artifact Registry repository for cloud app images."
  value       = module.artifact_registry.repository_url
}

output "service_account_emails" {
  description = "Service account emails for the cloud stack."
  value       = module.service_accounts.email_by_key
}

output "secret_ids" {
  description = "Secret Manager secret ids created for the cloud stack."
  value       = module.secret_manager.secret_ids
}

output "hostnames" {
  description = "Frozen public hostnames for the cloud stack."
  value       = local.hostnames
}
