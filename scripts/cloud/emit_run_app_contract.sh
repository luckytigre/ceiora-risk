#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

TF_PROD_DIR="${TF_PROD_DIR:-${ROOT_DIR}/infra/terraform/envs/prod}"
PROD_TERRAFORM_OUTPUT_JSON="${PROD_TERRAFORM_OUTPUT_JSON:-}"
RUN_APP_PHASE="${RUN_APP_PHASE:-soak}"

case "${RUN_APP_PHASE}" in
  soak)
    EDGE_ENABLED_VALUE="true"
    ;;
  no-edge)
    EDGE_ENABLED_VALUE="false"
    ;;
  *)
    printf 'RUN_APP_PHASE must be one of: soak, no-edge.\n' >&2
    exit 1
    ;;
esac

load_terraform_outputs() {
  local destination="$1"
  if [[ -n "${PROD_TERRAFORM_OUTPUT_JSON}" ]]; then
    if [[ ! -f "${PROD_TERRAFORM_OUTPUT_JSON}" ]]; then
      printf 'PROD_TERRAFORM_OUTPUT_JSON does not exist: %s\n' "${PROD_TERRAFORM_OUTPUT_JSON}" >&2
      exit 1
    fi
    cp "${PROD_TERRAFORM_OUTPUT_JSON}" "${destination}"
    return 0
  fi

  if [[ ! -d "${TF_PROD_DIR}" ]]; then
    printf 'TF_PROD_DIR does not exist: %s\n' "${TF_PROD_DIR}" >&2
    exit 1
  fi

  if ! terraform -chdir="${TF_PROD_DIR}" output -json >"${destination}" 2>"${destination}.err"; then
    cat "${destination}.err" >&2
    printf '\nUnable to read terraform outputs. Run terraform init in %s or set PROD_TERRAFORM_OUTPUT_JSON to a saved terraform output -json file.\n' "${TF_PROD_DIR}" >&2
    exit 1
  fi
}

json_value() {
  local input_path="$1"
  local dotted_path="$2"
  python3 - "${input_path}" "${dotted_path}" <<'PY'
import json
import sys

value = json.load(open(sys.argv[1], "r", encoding="utf-8"))
for key in sys.argv[2].split("."):
    if isinstance(value, dict):
        value = value[key]
    else:
        raise KeyError(sys.argv[2])
if not isinstance(value, str):
    raise TypeError(f"{sys.argv[2]} did not resolve to a string")
print(value)
PY
}

applied_service_image_ref() {
  local input_path="$1"
  local service_key="$2"
  python3 - "${input_path}" "${service_key}" <<'PY'
import json
import sys

outputs = json.load(open(sys.argv[1], "r", encoding="utf-8"))
service_key = sys.argv[2]

applied = outputs.get("service_image_refs_applied", {}).get("value")
configured = outputs["service_image_refs"]["value"]
if applied and applied.get(service_key):
    print(applied[service_key])
else:
    print(configured[service_key])
PY
}

assert_control_surfaces_match() {
  local input_path="$1"
  python3 - "${input_path}" <<'PY'
import json
import sys

outputs = json.load(open(sys.argv[1], "r", encoding="utf-8"))
control_surfaces = outputs.get("control_surface_image_refs_applied", {}).get("value")
if not control_surfaces:
    raise SystemExit(0)
service_image = control_surfaces["service"]
mismatches = {
    key: value
    for key, value in control_surfaces.items()
    if key != "service" and value != service_image
}
if mismatches:
    raise SystemExit(
        "Live control service image and one or more control-surface job images differ. "
        "Reconcile them before emitting a shared control_image_ref contract.\n"
        f"control service: {service_image}\n"
        + "\n".join(f"{key}: {value}" for key, value in sorted(mismatches.items()))
    )
PY
}

tmp_output="$(mktemp)"
trap 'rm -f "${tmp_output}" "${tmp_output}.err"' EXIT
load_terraform_outputs "${tmp_output}"
assert_control_surfaces_match "${tmp_output}"

frontend_public_origin="$(json_value "${tmp_output}" "service_urls.value.frontend")"
frontend_backend_api_origin="$(json_value "${tmp_output}" "service_urls.value.serve")"
frontend_backend_control_origin="$(json_value "${tmp_output}" "service_urls.value.control")"
frontend_image_ref="$(applied_service_image_ref "${tmp_output}" "frontend")"
serve_image_ref="$(applied_service_image_ref "${tmp_output}" "serve")"
control_image_ref="$(applied_service_image_ref "${tmp_output}" "control")"
current_endpoint_mode="$(json_value "${tmp_output}" "endpoint_mode.value")"
current_edge_enabled="$(python3 - "${tmp_output}" <<'PY'
import json
import sys
value = json.load(open(sys.argv[1], "r", encoding="utf-8"))["edge_enabled"]["value"]
print("true" if value else "false")
PY
)"

cat <<EOF
# Generated from terraform outputs in ${TF_PROD_DIR}
# Current live topology: endpoint_mode=${current_endpoint_mode}, edge_enabled=${current_edge_enabled}
endpoint_mode = "run_app"
edge_enabled = ${EDGE_ENABLED_VALUE}
frontend_public_origin = "${frontend_public_origin}"
frontend_backend_api_origin = "${frontend_backend_api_origin}"
frontend_backend_control_origin = "${frontend_backend_control_origin}"
frontend_image_ref = "${frontend_image_ref}"
serve_image_ref = "${serve_image_ref}"
control_image_ref = "${control_image_ref}"
EOF
