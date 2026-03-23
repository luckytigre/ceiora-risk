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

variable "cloudflare_zone_name" {
  description = "Cloudflare zone name for public DNS."
  type        = string
  default     = "ceiora.com"
}
