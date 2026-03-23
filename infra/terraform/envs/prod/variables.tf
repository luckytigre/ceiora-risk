variable "project_id" {
  description = "Rollout GCP project."
  type        = string
  default     = "project-4e18de12-63a3-4206-aaa"
}

variable "environment" {
  description = "Environment name."
  type        = string
  default     = "prod"
}

variable "region" {
  description = "Primary Cloud Run and Artifact Registry region."
  type        = string
  default     = "us-east4"
}

variable "gcp_services" {
  description = "Required Google APIs for the cloud stack foundation."
  type        = set(string)
  default = [
    "artifactregistry.googleapis.com",
    "certificatemanager.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "cloudscheduler.googleapis.com",
    "compute.googleapis.com",
    "dns.googleapis.com",
    "iam.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
    "serviceusage.googleapis.com",
    "storage.googleapis.com",
  ]
}

variable "artifact_registry_repository_id" {
  description = "Artifact Registry repository for app images."
  type        = string
  default     = "ceiora-images"
}

variable "image_tag" {
  description = "Default image tag for Cloud Run resources before CI/CD exists."
  type        = string
  default     = "latest"
}

variable "frontend_image_ref" {
  description = "Optional explicit image ref for the frontend image."
  type        = string
  default     = ""
}

variable "serve_image_ref" {
  description = "Optional explicit image ref for the serve image."
  type        = string
  default     = ""
}

variable "control_image_ref" {
  description = "Optional explicit image ref for the control image."
  type        = string
  default     = ""
}

variable "frontend_backend_api_origin" {
  description = "Origin baked into and exposed by the frontend service for API proxying. Override with the serve run.app URL for smoke images."
  type        = string
  default     = ""
}

variable "frontend_backend_control_origin" {
  description = "Origin exposed by the frontend service for control-plane proxying."
  type        = string
  default     = ""
}

variable "frontend_max_instances" {
  description = "Maximum Cloud Run instances for the frontend service."
  type        = number
  default     = 4
}

variable "serve_max_instances" {
  description = "Maximum Cloud Run instances for the serve API."
  type        = number
  default     = 4
}

variable "control_max_instances" {
  description = "Maximum Cloud Run instances for the control API."
  type        = number
  default     = 3
}

variable "frontend_memory_limit" {
  description = "Memory limit for the frontend Cloud Run service."
  type        = string
  default     = "1Gi"
}

variable "serve_memory_limit" {
  description = "Memory limit for the serve Cloud Run service."
  type        = string
  default     = "1Gi"
}

variable "control_memory_limit" {
  description = "Memory limit for the control Cloud Run service."
  type        = string
  default     = "1Gi"
}

variable "cloudflare_zone_name" {
  description = "Cloudflare zone name for public DNS."
  type        = string
  default     = "ceiora.com"
}

variable "cloudflare_proxied" {
  description = "Whether the public DNS records should be proxied through Cloudflare. Keep false for the first Google-managed TLS cutover."
  type        = bool
  default     = false
}

variable "default_log_retention_days" {
  description = "Retention period for the project's default Cloud Logging bucket."
  type        = number
  default     = 30
}
