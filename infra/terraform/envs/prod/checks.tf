resource "terraform_data" "topology_contract" {
  input = {
    endpoint_mode = local.endpoint_mode
    edge_enabled  = local.edge_enabled
  }

  lifecycle {
    precondition {
      condition = local.endpoint_mode != "custom_domains" || alltrue([
        trimspace(var.frontend_public_origin) == "",
        trimspace(var.frontend_backend_api_origin) == "",
        trimspace(var.frontend_backend_control_origin) == "",
      ])
      error_message = "endpoint_mode=custom_domains does not allow explicit frontend_public_origin, frontend_backend_api_origin, or frontend_backend_control_origin overrides. Switch to endpoint_mode=run_app to use explicit run.app origins."
    }

    precondition {
      condition     = local.endpoint_mode != "custom_domains" || local.edge_enabled
      error_message = "endpoint_mode=custom_domains requires edge_enabled=true. Switch to endpoint_mode=run_app before disabling the custom-domain edge."
    }

    precondition {
      condition = local.endpoint_mode != "run_app" || alltrue([
        trimspace(var.frontend_public_origin) != "",
        trimspace(var.frontend_backend_api_origin) != "",
        trimspace(var.frontend_backend_control_origin) != "",
      ])
      error_message = "endpoint_mode=run_app requires explicit frontend_public_origin, frontend_backend_api_origin, and frontend_backend_control_origin."
    }

    precondition {
      condition = local.endpoint_mode != "run_app" || alltrue([
        length(regexall("^https://[^/]+$", local.normalized_frontend_public_origin)) > 0,
        length(regexall("^https://[^/]+$", local.normalized_frontend_backend_api_origin)) > 0,
        length(regexall("^https://[^/]+$", local.normalized_frontend_backend_control_origin)) > 0,
      ])
      error_message = "endpoint_mode=run_app requires frontend_public_origin, frontend_backend_api_origin, and frontend_backend_control_origin to be absolute https origins with no path."
    }

    precondition {
      condition = local.endpoint_mode != "run_app" || alltrue([
        trimspace(var.frontend_image_ref) != "",
        trimspace(var.serve_image_ref) != "",
        trimspace(var.control_image_ref) != "",
        !endswith(lower(trimspace(var.frontend_image_ref)), ":latest"),
        !endswith(lower(trimspace(var.serve_image_ref)), ":latest"),
        !endswith(lower(trimspace(var.control_image_ref)), ":latest"),
      ])
      error_message = "endpoint_mode=run_app requires explicit frontend_image_ref, serve_image_ref, and control_image_ref, and they must not use :latest."
    }
  }
}
