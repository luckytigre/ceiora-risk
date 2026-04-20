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

variable "endpoint_mode" {
  description = "Public topology contract for the cloud app. custom_domains preserves the legacy app/api/control edge; run_app expects explicit frontend and backend origins."
  type        = string
  default     = "custom_domains"

  validation {
    condition     = contains(["custom_domains", "run_app"], var.endpoint_mode)
    error_message = "endpoint_mode must be one of: custom_domains, run_app."
  }
}

variable "edge_enabled" {
  description = "Whether the legacy custom-domain edge resources should remain provisioned. custom_domains requires true; run_app may use true for soak or false for Firebase/no-edge steady state."
  type        = bool
  default     = true
}

variable "frontend_public_origin" {
  description = "Canonical browser-facing frontend origin. Required when endpoint_mode=run_app; may be a custom domain or a run.app origin."
  type        = string
  default     = ""
}

variable "frontend_backend_api_origin" {
  description = "Origin exposed by the frontend service for API proxying. Required when endpoint_mode=run_app."
  type        = string
  default     = ""
}

variable "frontend_backend_control_origin" {
  description = "Origin exposed by the frontend service for control-plane proxying. Required when endpoint_mode=run_app."
  type        = string
  default     = ""
}

variable "private_backend_invocation_enabled" {
  description = "When true, the frontend calls serve/control through their Cloud Run service URLs with IAM auth and serve/control stop granting unauthenticated invoker access."
  type        = bool
  default     = false
}

variable "app_auth_provider" {
  description = "Frontend auth provider contract."
  type        = string
  default     = "shared"
}

variable "app_account_enforcement_enabled" {
  description = "Whether authenticated app reads and writes are account-scoped."
  type        = bool
  default     = false
}

variable "app_auth_bootstrap_enabled" {
  description = "Whether backend account bootstrap is enabled for authenticated users."
  type        = bool
  default     = false
}

variable "app_admin_settings_enabled" {
  description = "Whether privileged admin/settings surfaces are enabled on the backend."
  type        = bool
  default     = false
}

variable "app_shared_auth_accept_legacy" {
  description = "Whether the runtime still accepts the legacy shared-auth path."
  type        = bool
  default     = true
}

variable "neon_auth_base_url" {
  description = "Base URL for the active Neon Auth tenant."
  type        = string
  default     = ""
}

variable "neon_auth_issuer" {
  description = "Issuer URL for Neon Auth JWT verification."
  type        = string
  default     = ""
}

variable "neon_auth_audience" {
  description = "Expected Neon Auth JWT audience."
  type        = string
  default     = ""
}

variable "neon_auth_jwks_json" {
  description = "JWKS JSON used by the frontend auth bootstrap contract."
  type        = string
  default     = ""
}

variable "neon_auth_allowed_emails" {
  description = "Email allowlist for the current friend-scale Neon auth rollout."
  type        = list(string)
  default     = []
}

variable "neon_auth_bootstrap_admins" {
  description = "Emails that should receive admin bootstrap in the current Neon auth rollout."
  type        = list(string)
  default     = []
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
