module "project_services" {
  source = "../../modules/project_services"

  project_id = var.project_id
  services   = var.gcp_services
}

module "artifact_registry" {
  source = "../../modules/artifact_registry"

  project_id    = var.project_id
  region        = var.region
  repository_id = var.artifact_registry_repository_id
  description   = "Container images for the ceiora cloud stack."

  depends_on = [module.project_services]
}

module "service_accounts" {
  source = "../../modules/service_accounts"

  project_id = var.project_id
  accounts   = local.service_accounts

  depends_on = [module.project_services]
}

module "secret_manager" {
  source = "../../modules/secret_manager"

  project_id = var.project_id
  secrets    = local.secret_ids

  depends_on = [module.project_services]
}

resource "google_secret_manager_secret_iam_member" "secret_accessor" {
  for_each = local.secret_accessors

  project   = var.project_id
  secret_id = module.secret_manager.secret_ids[each.value.secret_key]
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${module.service_accounts.email_by_key[each.value.service_account_key]}"
}
