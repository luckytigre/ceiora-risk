output "tf_state_bucket_name" {
  description = "Shared Terraform state bucket created by the bootstrap root."
  value       = google_storage_bucket.tf_state.name
}
