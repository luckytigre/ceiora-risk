#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

TF_PROD_DIR="${TF_PROD_DIR:-${ROOT_DIR}/infra/terraform/envs/prod}"
PROD_TERRAFORM_OUTPUT_JSON="${PROD_TERRAFORM_OUTPUT_JSON:-}"
OPERATOR_CHECK_SCRIPT="${OPERATOR_CHECK_SCRIPT:-./scripts/operator_check.sh}"
TOPOLOGY_CHECK_RUN_REFRESH_DISPATCH="${TOPOLOGY_CHECK_RUN_REFRESH_DISPATCH:-0}"
TOPOLOGY_CHECK_DISPATCH_SURFACE="${TOPOLOGY_CHECK_DISPATCH_SURFACE:-active}"
TOPOLOGY_CHECK_REFRESH_EXPECTED_OUTCOME="${TOPOLOGY_CHECK_REFRESH_EXPECTED_OUTCOME:-success}"

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
if isinstance(value, bool):
    print("true" if value else "false")
elif isinstance(value, str):
    print(value)
else:
    raise TypeError(f"{sys.argv[2]} did not resolve to a scalar string/bool")
PY
}

run_operator_check() {
  local label="$1"
  local surface="$2"
  local app_url="$3"
  local control_url="$4"
  local skip_local="$5"
  local run_refresh_dispatch="0"

  printf 'Running topology check [%s]\n' "${label}"
  printf '  APP_BASE_URL=%s\n' "${app_url}"
  printf '  CONTROL_BASE_URL=%s\n' "${control_url}"
  printf '  surface=%s\n' "${surface}"

  if [[ "${TOPOLOGY_CHECK_RUN_REFRESH_DISPATCH}" == "1" ]]; then
    case "${TOPOLOGY_CHECK_DISPATCH_SURFACE}" in
      active)
        case "${endpoint_mode}:${edge_enabled}" in
          custom_domains:true)
            [[ "${surface}" == "edge" ]] && run_refresh_dispatch="1"
            ;;
          run_app:true|run_app:false)
            [[ "${surface}" == "run_app" ]] && run_refresh_dispatch="1"
            ;;
        esac
        ;;
      run_app|edge)
        [[ "${surface}" == "${TOPOLOGY_CHECK_DISPATCH_SURFACE}" ]] && run_refresh_dispatch="1"
        ;;
    esac
  fi

  APP_BASE_URL="${app_url}" \
  CONTROL_BASE_URL="${control_url}" \
  OPERATOR_CHECK_REQUIRE_LIVE=1 \
  OPERATOR_CHECK_SKIP_LOCAL="${skip_local}" \
  RUN_REFRESH_DISPATCH="${run_refresh_dispatch}" \
  RUN_REFRESH_EXPECTED_OUTCOME="${TOPOLOGY_CHECK_REFRESH_EXPECTED_OUTCOME}" \
  "${OPERATOR_CHECK_SCRIPT}"
}

tmp_output="$(mktemp)"
trap 'rm -f "${tmp_output}" "${tmp_output}.err"' EXIT
load_terraform_outputs "${tmp_output}"

endpoint_mode="$(json_value "${tmp_output}" "endpoint_mode.value")"
edge_enabled="$(json_value "${tmp_output}" "edge_enabled.value")"

case "${TOPOLOGY_CHECK_RUN_REFRESH_DISPATCH}" in
  0|1)
    ;;
  *)
    printf 'TOPOLOGY_CHECK_RUN_REFRESH_DISPATCH must be 0 or 1.\n' >&2
    exit 1
    ;;
esac

case "${TOPOLOGY_CHECK_DISPATCH_SURFACE}" in
  active|run_app|edge)
    ;;
  *)
    printf 'TOPOLOGY_CHECK_DISPATCH_SURFACE must be one of: active, run_app, edge.\n' >&2
    exit 1
    ;;
esac

case "${TOPOLOGY_CHECK_REFRESH_EXPECTED_OUTCOME}" in
  success|core_due_refusal|terminal_only)
    ;;
  *)
    printf 'TOPOLOGY_CHECK_REFRESH_EXPECTED_OUTCOME must be one of: success, core_due_refusal, terminal_only.\n' >&2
    exit 1
    ;;
esac

runapp_app_url="$(json_value "${tmp_output}" "public_origins.value.frontend")"
runapp_control_url="$(json_value "${tmp_output}" "public_origins.value.control")"
custom_app_url="https://$(json_value "${tmp_output}" "hostnames.value.frontend")"
custom_control_url="https://$(json_value "${tmp_output}" "hostnames.value.control")"

case "${endpoint_mode}:${edge_enabled}" in
  custom_domains:true)
    run_operator_check "custom-domains" "edge" "${runapp_app_url}" "${runapp_control_url}" "0"
    ;;
  run_app:true)
    run_operator_check "run-app-soak" "run_app" "${runapp_app_url}" "${runapp_control_url}" "0"
    run_operator_check "custom-domain-rollback" "edge" "${custom_app_url}" "${custom_control_url}" "1"
    ;;
  run_app:false)
    run_operator_check "run-app-no-edge" "run_app" "${runapp_app_url}" "${runapp_control_url}" "0"
    ;;
  *)
    printf 'Unsupported topology contract from terraform outputs: endpoint_mode=%s edge_enabled=%s\n' "${endpoint_mode}" "${edge_enabled}" >&2
    exit 1
    ;;
esac
