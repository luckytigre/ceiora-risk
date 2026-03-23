variable "project_id" {
  description = "GCP project for the Terraform bootstrap bucket."
  type        = string
  default     = "project-4e18de12-63a3-4206-aaa"
}

variable "region" {
  description = "Bootstrap region for Google provider operations."
  type        = string
  default     = "us-east4"
}

variable "tf_state_bucket_name" {
  description = "GCS bucket name for shared Terraform state."
  type        = string
  default     = "ceiora-risk-project-4e18de12-63a3-4206-aaa-tfstate"
}
