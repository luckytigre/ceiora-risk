resource "google_logging_project_bucket_config" "default_logs" {
  project        = var.project_id
  location       = "global"
  bucket_id      = "_Default"
  retention_days = var.default_log_retention_days
}
