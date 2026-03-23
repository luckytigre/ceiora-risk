terraform {
  required_version = ">= 1.8.0"

  backend "gcs" {}

  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 5.8"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 6.28"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 6.28"
    }
  }
}
