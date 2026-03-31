#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

TERRAFORM_BIN="${TERRAFORM_BIN:-terraform}"
TF_PROD_DIR="${TF_PROD_DIR:-${ROOT_DIR}/infra/terraform/envs/prod}"
BACKEND_HCL_PATH="${BACKEND_HCL_PATH:-}"
TF_AUTO_INIT="${TF_AUTO_INIT:-0}"
ROLLOUT_BUNDLE_DIR="${ROLLOUT_BUNDLE_DIR:-}"
CUTOVER_ACTION="${CUTOVER_ACTION:-${1:-}}"
CUTOVER_PHASE="${CUTOVER_PHASE:-${2:-}}"
ALLOW_TERRAFORM_APPLY="${ALLOW_TERRAFORM_APPLY:-0}"
ALLOW_EDGE_DISABLE="${ALLOW_EDGE_DISABLE:-0}"
AUTO_APPROVE="${AUTO_APPROVE:-0}"
VERIFY_REQUEST_BILLING="${VERIFY_REQUEST_BILLING:-0}"
RUN_APP_FRONTEND_IMAGE_REF="${RUN_APP_FRONTEND_IMAGE_REF:-}"
ALLOW_STALE_ROLLOUT_BUNDLE="${ALLOW_STALE_ROLLOUT_BUNDLE:-0}"
LIVE_TERRAFORM_OUTPUT_JSON="${LIVE_TERRAFORM_OUTPUT_JSON:-}"

CAPTURE_ROLLOUT_BUNDLE_SCRIPT="${CAPTURE_ROLLOUT_BUNDLE_SCRIPT:-./scripts/cloud/capture_run_app_rollout_bundle.sh}"
CLOUD_IMAGES_PUSH_SCRIPT="${CLOUD_IMAGES_PUSH_SCRIPT:-./scripts/cloud/build_and_push_images.sh}"
TOPOLOGY_CHECK_SCRIPT="${TOPOLOGY_CHECK_SCRIPT:-./scripts/cloud/topology_check.sh}"
REQUEST_BILLING_CHECK_SCRIPT="${REQUEST_BILLING_CHECK_SCRIPT:-./scripts/cloud/check_request_billing.sh}"

usage() {
  cat <<'EOF'
Usage:
  CUTOVER_ACTION=bundle [ROLLOUT_BUNDLE_DIR=...] make cloud-run-app-cutover
  CUTOVER_ACTION=build-frontend ROLLOUT_BUNDLE_DIR=... [IMAGE_TAG=...] make cloud-run-app-cutover
  CUTOVER_ACTION=plan CUTOVER_PHASE=soak|no-edge|rollback ROLLOUT_BUNDLE_DIR=... make cloud-run-app-cutover
  CUTOVER_ACTION=apply CUTOVER_PHASE=soak|no-edge|rollback ROLLOUT_BUNDLE_DIR=... ALLOW_TERRAFORM_APPLY=1 make cloud-run-app-cutover
  CUTOVER_ACTION=verify [ROLLOUT_BUNDLE_DIR=...] OPERATOR_API_TOKEN=... make cloud-run-app-cutover

Environment:
  ROLLOUT_BUNDLE_DIR         Required for build-frontend, plan, and apply; optional for verify
  TF_PROD_DIR                Terraform prod root. Default: infra/terraform/envs/prod
  TERRAFORM_BIN              Terraform binary. Default: terraform
  TF_AUTO_INIT               Set to 1 to run terraform init automatically when .terraform/ is missing
  BACKEND_HCL_PATH           Optional backend.hcl path used only when TF_AUTO_INIT=1
  RUN_APP_FRONTEND_IMAGE_REF Required for run_app plan/apply unless the bundle already contains run_app_frontend_image_ref.txt
  ALLOW_TERRAFORM_APPLY      Set to 1 for CUTOVER_ACTION=apply
  ALLOW_EDGE_DISABLE         Set to 1 for CUTOVER_ACTION=apply with CUTOVER_PHASE=no-edge
  ALLOW_STALE_ROLLOUT_BUNDLE Set to 1 to bypass the live-vs-bundle freshness guard for soak/no-edge plan/apply
  AUTO_APPROVE               Set to 1 to append -auto-approve on terraform apply
  VERIFY_REQUEST_BILLING     Set to 1 to run the request-billing check after verify
  LIVE_TERRAFORM_OUTPUT_JSON Optional saved terraform output -json used only for the bundle freshness guard instead of live terraform output

Actions:
  bundle         Capture a new rollout bundle from the current prod Terraform outputs
  build-frontend Build and push a run.app-targeted frontend image using the bundle's serve run.app URL
  plan           Run terraform plan for soak, no-edge, or rollback using the bundle contract
  apply          Run terraform apply for soak, no-edge, or rollback using the bundle contract
  verify         Run topology-aware live verification, and optionally request-billing checks
EOF
}

case "${CUTOVER_ACTION}" in
  ""|-h|--help|help)
    usage
    exit 0
    ;;
esac

ensure_bundle_dir() {
  if [[ -z "${ROLLOUT_BUNDLE_DIR}" ]]; then
    printf 'ROLLOUT_BUNDLE_DIR is required for CUTOVER_ACTION=%s.\n' "${CUTOVER_ACTION}" >&2
    exit 1
  fi
  if [[ ! -d "${ROLLOUT_BUNDLE_DIR}" ]]; then
    printf 'ROLLOUT_BUNDLE_DIR does not exist: %s\n' "${ROLLOUT_BUNDLE_DIR}" >&2
    exit 1
  fi
  if [[ ! -f "${ROLLOUT_BUNDLE_DIR}/terraform-output.json" ]]; then
    printf 'ROLLOUT_BUNDLE_DIR is missing terraform-output.json: %s\n' "${ROLLOUT_BUNDLE_DIR}" >&2
    exit 1
  fi
  if [[ ! -f "${ROLLOUT_BUNDLE_DIR}/manifest.json" ]]; then
    printf 'ROLLOUT_BUNDLE_DIR is missing manifest.json: %s\n' "${ROLLOUT_BUNDLE_DIR}" >&2
    exit 1
  fi
}

ensure_terraform_init() {
  if [[ -d "${TF_PROD_DIR}/.terraform" ]]; then
    return 0
  fi

  if [[ "${TF_AUTO_INIT}" != "1" ]]; then
    printf 'Terraform is not initialized in %s. Run terraform init -backend-config=backend.hcl there, or set TF_AUTO_INIT=1 and optionally BACKEND_HCL_PATH.\n' "${TF_PROD_DIR}" >&2
    exit 1
  fi

  init_cmd=("${TERRAFORM_BIN}" "-chdir=${TF_PROD_DIR}" init)
  if [[ -n "${BACKEND_HCL_PATH}" ]]; then
    init_cmd+=("-backend-config=${BACKEND_HCL_PATH}")
  fi
  "${init_cmd[@]}"
}

load_live_terraform_outputs() {
  local destination="$1"

  if [[ -n "${LIVE_TERRAFORM_OUTPUT_JSON}" ]]; then
    if [[ ! -f "${LIVE_TERRAFORM_OUTPUT_JSON}" ]]; then
      printf 'LIVE_TERRAFORM_OUTPUT_JSON does not exist: %s\n' "${LIVE_TERRAFORM_OUTPUT_JSON}" >&2
      exit 1
    fi
    cp "${LIVE_TERRAFORM_OUTPUT_JSON}" "${destination}"
    return 0
  fi

  ensure_terraform_init
  if ! "${TERRAFORM_BIN}" -chdir="${TF_PROD_DIR}" output -json >"${destination}" 2>"${destination}.err"; then
    cat "${destination}.err" >&2
    printf '\nUnable to read live terraform outputs for bundle freshness verification.\n' >&2
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

resolved_run_app_frontend_image_ref() {
  if [[ -n "${RUN_APP_FRONTEND_IMAGE_REF}" ]]; then
    printf '%s\n' "${RUN_APP_FRONTEND_IMAGE_REF}"
    return 0
  fi

  local stored_ref_file="${ROLLOUT_BUNDLE_DIR}/run_app_frontend_image_ref.txt"
  if [[ -f "${stored_ref_file}" ]]; then
    local stored_ref
    stored_ref="$(tr -d '\n' <"${stored_ref_file}")"
    if [[ -n "${stored_ref}" ]]; then
      printf '%s\n' "${stored_ref}"
      return 0
    fi
  fi

  printf 'run_app cutover requires an explicit run.app-built frontend image ref. Set RUN_APP_FRONTEND_IMAGE_REF or run CUTOVER_ACTION=build-frontend first.\n' >&2
  exit 1
}

phase_base_var_file() {
  case "${CUTOVER_PHASE}" in
    soak)
      printf '%s\n' "${ROLLOUT_BUNDLE_DIR}/run_app_soak.base.tfvars"
      ;;
    no-edge)
      printf '%s\n' "${ROLLOUT_BUNDLE_DIR}/run_app_no_edge.base.tfvars"
      ;;
    rollback)
      printf '%s\n' "${ROLLOUT_BUNDLE_DIR}/rollback_custom_domains.tfvars"
      ;;
    *)
      printf 'CUTOVER_PHASE must be one of: soak, no-edge, rollback.\n' >&2
      exit 1
      ;;
  esac
}

render_effective_var_file() {
  local base_var_file="$1"
  local output_dir="${ROLLOUT_BUNDLE_DIR}/generated"
  local effective_var_file="${output_dir}/${CUTOVER_PHASE}.effective.tfvars"
  mkdir -p "${output_dir}"

  cp "${base_var_file}" "${effective_var_file}"

  if [[ "${CUTOVER_PHASE}" == "soak" || "${CUTOVER_PHASE}" == "no-edge" ]]; then
    local frontend_image_ref
    frontend_image_ref="$(resolved_run_app_frontend_image_ref)"
    {
      printf '\n'
      printf '# Injected by run_app_cutover.sh for the active run_app phase.\n'
      printf 'frontend_image_ref = "%s"\n' "${frontend_image_ref}"
    } >>"${effective_var_file}"
  fi

  printf '%s\n' "${effective_var_file}"
}

verify_bundle_freshness() {
  if [[ "${CUTOVER_PHASE}" == "rollback" || "${ALLOW_STALE_ROLLOUT_BUNDLE}" == "1" ]]; then
    return 0
  fi

  local live_output_file
  live_output_file="$(mktemp)"
  load_live_terraform_outputs "${live_output_file}"

  python3 - "${ROLLOUT_BUNDLE_DIR}/manifest.json" "${live_output_file}" "${CUTOVER_PHASE}" <<'PY'
import json
import sys

manifest = json.load(open(sys.argv[1], "r", encoding="utf-8"))
live_outputs = json.load(open(sys.argv[2], "r", encoding="utf-8"))
phase = sys.argv[3]

live_control_surfaces = live_outputs.get("control_surface_image_refs_applied", {}).get("value")
if live_control_surfaces:
    control_surface_mismatches = {
        key: value
        for key, value in live_control_surfaces.items()
        if key != "service" and value != live_control_surfaces["service"]
    }
else:
    control_surface_mismatches = {}
if control_surface_mismatches:
    raise SystemExit(
        "Live control service image and one or more control-surface job images differ. "
        "Reconcile them before using a shared control-image rollout bundle.\n"
        f"control service: {live_control_surfaces['service']}\n"
        + "\n".join(f"{key}: {value}" for key, value in sorted(control_surface_mismatches.items()))
    )

live_images = live_outputs.get("service_image_refs_applied", {}).get("value") or live_outputs["service_image_refs"]["value"]
live_control_job_images = live_control_surfaces or {}
live_topology = {
    "endpoint_mode": live_outputs["endpoint_mode"]["value"],
    "edge_enabled": bool(live_outputs["edge_enabled"]["value"]),
    "public_origins": live_outputs["public_origins"]["value"],
}
bundle_topology = manifest["source_topology"]
bundle_images = manifest["service_image_refs"]
bundle_control_job_images = manifest.get("control_surface_image_refs") or {}

mismatches = []
if phase in {"soak", "no-edge"}:
    if bundle_topology["endpoint_mode"] != live_topology["endpoint_mode"]:
        mismatches.append(
            f"endpoint_mode bundle={bundle_topology['endpoint_mode']} live={live_topology['endpoint_mode']}"
        )
    if bool(bundle_topology["edge_enabled"]) != live_topology["edge_enabled"]:
        mismatches.append(
            f"edge_enabled bundle={str(bundle_topology['edge_enabled']).lower()} live={str(live_topology['edge_enabled']).lower()}"
        )
    if bundle_topology["public_origins"] != live_topology["public_origins"]:
        mismatches.append(
            "public_origins bundle="
            + json.dumps(bundle_topology["public_origins"], sort_keys=True)
            + " live="
            + json.dumps(live_topology["public_origins"], sort_keys=True)
        )
for key in ("frontend", "serve", "control"):
    if bundle_images[key] != live_images[key]:
        mismatches.append(f"{key}_image bundle={bundle_images[key]} live={live_images[key]}")
for key, live_value in sorted(live_control_job_images.items()):
    if key == "service":
        continue
    bundle_value = bundle_control_job_images.get(key)
    if bundle_value and bundle_value != live_value:
        mismatches.append(f"{key}_image bundle={bundle_value} live={live_value}")

if mismatches:
    raise SystemExit(
        f"Refusing {phase}: the rollout bundle is stale for the current live topology.\n"
        + "\n".join(f"- {entry}" for entry in mismatches)
        + "\nRecapture a fresh bundle from the current topology before continuing, "
          "or set ALLOW_STALE_ROLLOUT_BUNDLE=1 only for an intentional replay."
    )
PY
  rm -f "${live_output_file}" "${live_output_file}.err"
}

run_terraform_phase() {
  local action="$1"
  local base_var_file
  local effective_var_file

  ensure_bundle_dir
  ensure_terraform_init
  base_var_file="$(phase_base_var_file)"
  if [[ ! -f "${base_var_file}" ]]; then
    printf 'Missing bundle phase contract: %s\n' "${base_var_file}" >&2
    exit 1
  fi
  verify_bundle_freshness
  effective_var_file="$(render_effective_var_file "${base_var_file}")"

  if [[ "${action}" == "apply" ]]; then
    if [[ "${ALLOW_TERRAFORM_APPLY}" != "1" ]]; then
      printf 'CUTOVER_ACTION=apply requires ALLOW_TERRAFORM_APPLY=1.\n' >&2
      exit 1
    fi
    if [[ "${CUTOVER_PHASE}" == "no-edge" && "${ALLOW_EDGE_DISABLE}" != "1" ]]; then
      printf 'CUTOVER_PHASE=no-edge apply requires ALLOW_EDGE_DISABLE=1.\n' >&2
      exit 1
    fi
  fi

  cmd=("${TERRAFORM_BIN}" "-chdir=${TF_PROD_DIR}" "${action}" "-var-file=${effective_var_file}")
  if [[ "${action}" == "apply" && "${AUTO_APPROVE}" == "1" ]]; then
    cmd+=("-auto-approve")
  fi

  printf 'Using Terraform var file: %s\n' "${effective_var_file}"
  "${cmd[@]}"
}

bundle_action() {
  "${CAPTURE_ROLLOUT_BUNDLE_SCRIPT}"
}

build_frontend_action() {
  ensure_bundle_dir
  local serve_run_app_origin
  serve_run_app_origin="$(json_value "${ROLLOUT_BUNDLE_DIR}/terraform-output.json" "service_urls.value.serve")"

  if [[ ! "${serve_run_app_origin}" =~ ^https://[^/]+\.run\.app$ ]]; then
    printf 'Bundle service_urls.value.serve must be a run.app origin: %s\n' "${serve_run_app_origin}" >&2
    exit 1
  fi

  local project_id="${PROJECT_ID:-project-4e18de12-63a3-4206-aaa}"
  local region="${REGION:-us-east4}"
  local repository="${REPOSITORY:-ceiora-images}"
  local image_tag="${IMAGE_TAG:-$(git rev-parse --short HEAD)}"
  local registry_base="${region}-docker.pkg.dev/${project_id}/${repository}"
  local frontend_image="${FRONTEND_IMAGE:-${registry_base}/frontend:${image_tag}}"

  PROJECT_ID="${project_id}" \
  REGION="${region}" \
  REPOSITORY="${repository}" \
  IMAGE_TAG="${image_tag}" \
  FRONTEND_IMAGE="${frontend_image}" \
  ENDPOINT_MODE="run_app" \
  BACKEND_API_ORIGIN="${serve_run_app_origin}" \
  BUILD_TARGETS="frontend" \
  "${CLOUD_IMAGES_PUSH_SCRIPT}"

  printf '%s\n' "${frontend_image}" >"${ROLLOUT_BUNDLE_DIR}/run_app_frontend_image_ref.txt"
  printf 'Recorded run.app frontend image ref in %s\n' "${ROLLOUT_BUNDLE_DIR}/run_app_frontend_image_ref.txt"
}

verify_action() {
  ensure_terraform_init
  "${TOPOLOGY_CHECK_SCRIPT}"
  if [[ "${VERIFY_REQUEST_BILLING}" == "1" ]]; then
    "${REQUEST_BILLING_CHECK_SCRIPT}"
  fi
}

case "${CUTOVER_ACTION}" in
  bundle)
    bundle_action
    ;;
  build-frontend)
    build_frontend_action
    ;;
  plan)
    run_terraform_phase plan
    ;;
  apply)
    run_terraform_phase apply
    ;;
  verify)
    verify_action
    ;;
  *)
    printf 'Unsupported CUTOVER_ACTION: %s\n' "${CUTOVER_ACTION}" >&2
    usage >&2
    exit 1
    ;;
esac
