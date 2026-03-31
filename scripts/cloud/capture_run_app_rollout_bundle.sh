#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

TERRAFORM_BIN="${TERRAFORM_BIN:-terraform}"
TF_PROD_DIR="${TF_PROD_DIR:-${ROOT_DIR}/infra/terraform/envs/prod}"
ROLLOUT_SOURCE_OUTPUT_JSON="${ROLLOUT_SOURCE_OUTPUT_JSON:-}"
ROLLOUT_BUNDLE_DIR="${ROLLOUT_BUNDLE_DIR:-}"
ROLLOUT_CAPTURE_MODE="${ROLLOUT_CAPTURE_MODE:-cutover-source}"

usage() {
  cat <<'EOF'
Usage:
  ROLLOUT_BUNDLE_DIR=backend/runtime/cloud_rollouts/<name> ./scripts/cloud/capture_run_app_rollout_bundle.sh

Environment:
  TF_PROD_DIR                     Terraform prod root. Default: infra/terraform/envs/prod
  TERRAFORM_BIN                   Terraform binary. Default: terraform
  ROLLOUT_SOURCE_OUTPUT_JSON      Optional saved terraform output -json file to capture from instead of the live prod root
  ROLLOUT_BUNDLE_DIR              Destination bundle directory. Default: backend/runtime/cloud_rollouts/run_app_<timestamp>
  ROLLOUT_CAPTURE_MODE            One of:
                                  - cutover-source: require custom_domains + edge_enabled=true and emit cutover/rollback tfvars
                                  - steady-state: require endpoint_mode=run_app and emit a current-topology pin bundle
EOF
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

load_terraform_outputs() {
  local destination="$1"
  if [[ -n "${ROLLOUT_SOURCE_OUTPUT_JSON}" ]]; then
    if [[ ! -f "${ROLLOUT_SOURCE_OUTPUT_JSON}" ]]; then
      printf 'ROLLOUT_SOURCE_OUTPUT_JSON does not exist: %s\n' "${ROLLOUT_SOURCE_OUTPUT_JSON}" >&2
      exit 1
    fi
    cp "${ROLLOUT_SOURCE_OUTPUT_JSON}" "${destination}"
    return 0
  fi

  if [[ ! -d "${TF_PROD_DIR}" ]]; then
    printf 'TF_PROD_DIR does not exist: %s\n' "${TF_PROD_DIR}" >&2
    exit 1
  fi

  if ! "${TERRAFORM_BIN}" -chdir="${TF_PROD_DIR}" output -json >"${destination}" 2>"${destination}.err"; then
    cat "${destination}.err" >&2
    printf '\nUnable to read terraform outputs. Run terraform init in %s or set ROLLOUT_SOURCE_OUTPUT_JSON to a saved terraform output -json file.\n' "${TF_PROD_DIR}" >&2
    exit 1
  fi
}

timestamp_utc() {
  date -u +"%Y%m%dT%H%M%SZ"
}

if [[ -z "${ROLLOUT_BUNDLE_DIR}" ]]; then
  ROLLOUT_BUNDLE_DIR="${ROOT_DIR}/backend/runtime/cloud_rollouts/run_app_$(timestamp_utc)"
fi

mkdir -p "${ROLLOUT_BUNDLE_DIR}"

tmp_output="$(mktemp)"
trap 'rm -f "${tmp_output}" "${tmp_output}.err"' EXIT
load_terraform_outputs "${tmp_output}"
cp "${tmp_output}" "${ROLLOUT_BUNDLE_DIR}/terraform-output.json"

python3 - "${ROLLOUT_BUNDLE_DIR}" "${ROLLOUT_CAPTURE_MODE}" <<'PY'
import json
import os
import pathlib
import sys
from datetime import datetime, timezone

bundle_dir = pathlib.Path(sys.argv[1]).resolve()
capture_mode = sys.argv[2]
outputs_path = bundle_dir / "terraform-output.json"
outputs = json.loads(outputs_path.read_text(encoding="utf-8"))

endpoint_mode = outputs["endpoint_mode"]["value"]
edge_enabled = bool(outputs["edge_enabled"]["value"])

service_urls = outputs["service_urls"]["value"]
service_image_refs = outputs.get("service_image_refs_applied", {}).get("value") or outputs["service_image_refs"]["value"]
control_surface_image_refs = outputs.get("control_surface_image_refs_applied", {}).get("value")
if control_surface_image_refs:
    control_surface_mismatches = {
        key: value
        for key, value in control_surface_image_refs.items()
        if key != "service" and value != control_surface_image_refs["service"]
    }
    if control_surface_mismatches:
        raise SystemExit(
            "Live control service image and one or more control-surface job images differ. "
            "Reconcile them before capturing a shared control-image bundle.\n"
            f"control service: {control_surface_image_refs['service']}\n"
            + "\n".join(f"{key}: {value}" for key, value in sorted(control_surface_mismatches.items()))
        )
service_names = outputs["service_names"]["value"]
public_origins = outputs["public_origins"]["value"]
hostnames = outputs["hostnames"]["value"]

captured_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

if capture_mode == "cutover-source":
    if not (endpoint_mode == "custom_domains" and edge_enabled):
        raise SystemExit(
            "ROLLOUT_CAPTURE_MODE=cutover-source requires a live custom_domains + edge_enabled=true source topology."
        )
elif capture_mode == "steady-state":
    if endpoint_mode != "run_app":
        raise SystemExit(
            "ROLLOUT_CAPTURE_MODE=steady-state requires endpoint_mode=run_app."
        )
else:
    raise SystemExit("ROLLOUT_CAPTURE_MODE must be one of: cutover-source, steady-state.")

manifest = {
    "captured_at": captured_at,
    "capture_mode": capture_mode,
    "source_kind": "saved-output-json" if "ROLLOUT_SOURCE_OUTPUT_JSON" in os.environ else "live-terraform-output",
    "source_topology": {
        "endpoint_mode": endpoint_mode,
        "edge_enabled": edge_enabled,
        "public_origins": public_origins,
        "hostnames": hostnames,
    },
    "service_names": service_names,
    "service_urls": service_urls,
    "service_image_refs": service_image_refs,
    "control_surface_image_refs": control_surface_image_refs,
    "bundle_files": {
        "terraform_output_json": "terraform-output.json",
        "run_app_frontend_image_ref": "run_app_frontend_image_ref.txt",
    },
}

if capture_mode == "cutover-source":
    manifest["bundle_files"].update({
        "rollback_tfvars": "rollback_custom_domains.tfvars",
        "run_app_soak_base_tfvars": "run_app_soak.base.tfvars",
        "run_app_no_edge_base_tfvars": "run_app_no_edge.base.tfvars",
    })

    rollback_tfvars = f"""# Captured from {outputs_path.name} at {captured_at}
# Roll back to the pre-cutover custom-domain topology with pinned image refs.
endpoint_mode = "custom_domains"
edge_enabled = true
frontend_image_ref = "{service_image_refs['frontend']}"
serve_image_ref = "{service_image_refs['serve']}"
control_image_ref = "{service_image_refs['control']}"
"""

    run_app_soak_base = f"""# Captured from {outputs_path.name} at {captured_at}
# Base contract for the run_app soak cutover.
# Provide the run.app-built frontend image at plan/apply time through RUN_APP_FRONTEND_IMAGE_REF
# or by creating {manifest['bundle_files']['run_app_frontend_image_ref']} with the built image ref.
endpoint_mode = "run_app"
edge_enabled = true
frontend_public_origin = "{service_urls['frontend']}"
frontend_backend_api_origin = "{service_urls['serve']}"
frontend_backend_control_origin = "{service_urls['control']}"
serve_image_ref = "{service_image_refs['serve']}"
control_image_ref = "{service_image_refs['control']}"
"""

    run_app_no_edge_base = f"""# Captured from {outputs_path.name} at {captured_at}
# Base contract for the final run_app no-edge steady state.
# Provide the run.app-built frontend image at plan/apply time through RUN_APP_FRONTEND_IMAGE_REF
# or by creating {manifest['bundle_files']['run_app_frontend_image_ref']} with the built image ref.
endpoint_mode = "run_app"
edge_enabled = false
frontend_public_origin = "{service_urls['frontend']}"
frontend_backend_api_origin = "{service_urls['serve']}"
frontend_backend_control_origin = "{service_urls['control']}"
serve_image_ref = "{service_image_refs['serve']}"
control_image_ref = "{service_image_refs['control']}"
"""

    readme = f"""Run.app rollout bundle
=====================

Bundle directory: {bundle_dir}
Captured at: {captured_at}
Capture mode: {capture_mode}
Source topology: endpoint_mode={endpoint_mode}, edge_enabled={str(edge_enabled).lower()}

Files:
- terraform-output.json
- manifest.json
- rollback_custom_domains.tfvars
- run_app_soak.base.tfvars
- run_app_no_edge.base.tfvars

Recommended next steps:
1. Build the run.app frontend image:
   CUTOVER_ACTION=build-frontend ROLLOUT_BUNDLE_DIR="{bundle_dir}" make cloud-run-app-cutover
2. Plan the soak cutover:
   CUTOVER_ACTION=plan CUTOVER_PHASE=soak ROLLOUT_BUNDLE_DIR="{bundle_dir}" make cloud-run-app-cutover
3. Apply the soak cutover:
   CUTOVER_ACTION=apply CUTOVER_PHASE=soak ROLLOUT_BUNDLE_DIR="{bundle_dir}" ALLOW_TERRAFORM_APPLY=1 make cloud-run-app-cutover
4. Verify the soak state:
   CUTOVER_ACTION=verify ROLLOUT_BUNDLE_DIR="{bundle_dir}" OPERATOR_API_TOKEN=... make cloud-run-app-cutover
5. After soak, recapture a fresh bundle from the verified soak topology before planning no-edge.

Important:
- run_app plan/apply will fail closed until a run.app-built frontend image ref is supplied.
- rollback_custom_domains.tfvars is the pinned rollback contract captured from the pre-cutover state.
"""

    (bundle_dir / "rollback_custom_domains.tfvars").write_text(rollback_tfvars, encoding="utf-8")
    (bundle_dir / "run_app_soak.base.tfvars").write_text(run_app_soak_base, encoding="utf-8")
    (bundle_dir / "run_app_no_edge.base.tfvars").write_text(run_app_no_edge_base, encoding="utf-8")
else:
    manifest["bundle_files"].update({
        "current_topology_tfvars": "run_app_current_topology.tfvars",
        "rollback_tfvars": "rollback_custom_domains.tfvars",
        "run_app_no_edge_base_tfvars": "run_app_no_edge.base.tfvars",
    })

    current_topology_tfvars = f"""# Captured from {outputs_path.name} at {captured_at}
# Current run_app topology pin file for post-cutover config-only changes and rebundling.
endpoint_mode = "run_app"
edge_enabled = {"true" if edge_enabled else "false"}
frontend_public_origin = "{service_urls['frontend']}"
frontend_backend_api_origin = "{service_urls['serve']}"
frontend_backend_control_origin = "{service_urls['control']}"
frontend_image_ref = "{service_image_refs['frontend']}"
serve_image_ref = "{service_image_refs['serve']}"
control_image_ref = "{service_image_refs['control']}"
"""

    rollback_tfvars = f"""# Captured from {outputs_path.name} at {captured_at}
# Roll back to the custom-domain topology with the current applied image refs.
endpoint_mode = "custom_domains"
edge_enabled = true
frontend_image_ref = "{service_image_refs['frontend']}"
serve_image_ref = "{service_image_refs['serve']}"
control_image_ref = "{service_image_refs['control']}"
"""

    run_app_no_edge_base = f"""# Captured from {outputs_path.name} at {captured_at}
# Base contract for the final run_app no-edge steady state using the current applied refs.
# Provide the run.app-built frontend image at plan/apply time through RUN_APP_FRONTEND_IMAGE_REF
# or by creating {manifest['bundle_files']['run_app_frontend_image_ref']} with the built image ref.
endpoint_mode = "run_app"
edge_enabled = false
frontend_public_origin = "{service_urls['frontend']}"
frontend_backend_api_origin = "{service_urls['serve']}"
frontend_backend_control_origin = "{service_urls['control']}"
serve_image_ref = "{service_image_refs['serve']}"
control_image_ref = "{service_image_refs['control']}"
"""

    readme = f"""Run.app steady-state bundle
=========================

Bundle directory: {bundle_dir}
Captured at: {captured_at}
Capture mode: {capture_mode}
Source topology: endpoint_mode={endpoint_mode}, edge_enabled={str(edge_enabled).lower()}

Files:
- terraform-output.json
- manifest.json
- run_app_current_topology.tfvars
- rollback_custom_domains.tfvars
- run_app_no_edge.base.tfvars

Recommended next steps:
1. Use run_app_current_topology.tfvars as the pinned image/origin contract for config-only Terraform work.
2. Use run_app_no_edge.base.tfvars for the next no-edge plan/apply if you are still in soak.
3. Use rollback_custom_domains.tfvars if you need to restore the custom-domain topology with the current applied image refs.
4. Recapture this bundle after any targeted image apply or manual emergency rollout before using helper-generated contracts again.

Important:
- This bundle is for post-cutover run_app operations, not the original custom-domain cutover.
- The current-topology pin file uses live-applied image refs when Terraform state exposes them.
"""

    (bundle_dir / "run_app_current_topology.tfvars").write_text(current_topology_tfvars, encoding="utf-8")
    (bundle_dir / "rollback_custom_domains.tfvars").write_text(rollback_tfvars, encoding="utf-8")
    (bundle_dir / "run_app_no_edge.base.tfvars").write_text(run_app_no_edge_base, encoding="utf-8")

(bundle_dir / "manifest.json").write_text(
    json.dumps(manifest, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
(bundle_dir / "README.txt").write_text(readme, encoding="utf-8")
PY

printf 'Captured run_app rollout bundle at %s\n' "${ROLLOUT_BUNDLE_DIR}"
printf 'Bundle files:\n'
find "${ROLLOUT_BUNDLE_DIR}" -maxdepth 1 -type f -print | sort | sed "s#^#  - #"
