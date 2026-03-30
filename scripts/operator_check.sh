#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

BACKEND_PYTHON="${BACKEND_PYTHON:-${ROOT_DIR}/backend/.venv/bin/python}"
CONTROL_BASE_URL="${CONTROL_BASE_URL:-}"
OPERATOR_CHECK_REQUIRE_LIVE="${OPERATOR_CHECK_REQUIRE_LIVE:-0}"
OPERATOR_CHECK_SKIP_LOCAL="${OPERATOR_CHECK_SKIP_LOCAL:-0}"
INVALID_OPERATOR_TOKEN="${INVALID_OPERATOR_TOKEN:-not-the-real-token}"
RUN_REFRESH_DISPATCH="${RUN_REFRESH_DISPATCH:-0}"
RUN_REFRESH_DISPATCH_TARGET="${RUN_REFRESH_DISPATCH_TARGET:-proxy}"
REFRESH_POLL_SECONDS="${REFRESH_POLL_SECONDS:-5}"
REFRESH_POLL_ATTEMPTS="${REFRESH_POLL_ATTEMPTS:-36}"

if [[ "${OPERATOR_CHECK_SKIP_LOCAL}" != "1" ]]; then
  if [[ ! -x "${BACKEND_PYTHON}" ]]; then
    printf 'Missing backend Python executable: %s\n' "${BACKEND_PYTHON}" >&2
    exit 1
  fi

  "${BACKEND_PYTHON}" -m pytest \
    backend/tests/test_cloud_auth_and_runtime_roles.py \
    backend/tests/test_operator_status_route.py \
    backend/tests/test_refresh_auth.py \
    backend/tests/test_refresh_control_service.py \
    -q
fi

if [[ -z "${APP_BASE_URL:-}" ]]; then
  if [[ "${OPERATOR_CHECK_REQUIRE_LIVE}" == "1" ]]; then
    printf 'APP_BASE_URL is required when OPERATOR_CHECK_REQUIRE_LIVE=1.\n' >&2
    exit 1
  fi
  printf 'Skipping live operator smoke because APP_BASE_URL is not set.\n'
  printf 'Set APP_BASE_URL and OPERATOR_API_TOKEN to exercise /api/operator/status and /api/refresh/status.\n'
  exit 0
fi

if [[ -z "${OPERATOR_API_TOKEN:-}" ]]; then
  printf 'OPERATOR_API_TOKEN is required when APP_BASE_URL is set.\n' >&2
  exit 1
fi

if [[ -z "${CONTROL_BASE_URL}" ]]; then
  printf 'CONTROL_BASE_URL is required when APP_BASE_URL is set.\n' >&2
  exit 1
fi

curl_status() {
  local output_path="$1"
  shift
  curl -sS -o "${output_path}" -w "%{http_code}" "$@"
}

expect_status() {
  local expected="$1"
  local actual="$2"
  local label="$3"
  local output_path="$4"
  if [[ "${actual}" != "${expected}" ]]; then
    printf '%s failed (expected http %s, got %s): %s\n' \
      "${label}" \
      "${expected}" \
      "${actual}" \
      "$(tr -d '\n' <"${output_path}")" >&2
    exit 1
  fi
}

json_path() {
  local input_path="$1"
  local dotted_path="$2"
  python3 - "${input_path}" "${dotted_path}" <<'PY'
import json
import sys

value = json.load(open(sys.argv[1], "r", encoding="utf-8"))
for key in sys.argv[2].split("."):
    if isinstance(value, dict):
        value = value.get(key)
    else:
        value = None
        break
print(json.dumps(value, sort_keys=True))
PY
}

assert_json_equal() {
  local label="$1"
  local left_path="$2"
  local right_path="$3"
  local dotted_path="$4"
  local left_value
  local right_value
  left_value="$(json_path "${left_path}" "${dotted_path}")"
  right_value="$(json_path "${right_path}" "${dotted_path}")"
  if [[ "${left_value}" != "${right_value}" ]]; then
    printf '%s mismatch for %s: left=%s right=%s\n' \
      "${label}" \
      "${dotted_path}" \
      "${left_value}" \
      "${right_value}" >&2
    exit 1
  fi
}

expect_status "401" "$(curl_status /tmp/ceiora_operator_status_anon.json "${APP_BASE_URL%/}/api/operator/status")" \
  "proxied operator status without token" \
  /tmp/ceiora_operator_status_anon.json
expect_status "401" "$(curl_status /tmp/ceiora_refresh_status_anon.json "${APP_BASE_URL%/}/api/refresh/status")" \
  "proxied refresh status without token" \
  /tmp/ceiora_refresh_status_anon.json
expect_status "401" "$(curl_status /tmp/ceiora_control_operator_status_anon.json "${CONTROL_BASE_URL%/}/api/operator/status")" \
  "direct operator status without token" \
  /tmp/ceiora_control_operator_status_anon.json
expect_status "401" "$(curl_status /tmp/ceiora_control_refresh_status_anon.json "${CONTROL_BASE_URL%/}/api/refresh/status")" \
  "direct refresh status without token" \
  /tmp/ceiora_control_refresh_status_anon.json
expect_status "401" "$(
  curl_status /tmp/ceiora_operator_status_bad_token.json \
    -H "X-Operator-Token: ${INVALID_OPERATOR_TOKEN}" \
    "${APP_BASE_URL%/}/api/operator/status"
)" "proxied operator status with invalid token" /tmp/ceiora_operator_status_bad_token.json
expect_status "401" "$(
  curl_status /tmp/ceiora_refresh_status_bad_token.json \
    -H "X-Operator-Token: ${INVALID_OPERATOR_TOKEN}" \
    "${APP_BASE_URL%/}/api/refresh/status"
)" "proxied refresh status with invalid token" /tmp/ceiora_refresh_status_bad_token.json
expect_status "401" "$(
  curl_status /tmp/ceiora_control_operator_status_bad_token.json \
    -H "X-Operator-Token: ${INVALID_OPERATOR_TOKEN}" \
    "${CONTROL_BASE_URL%/}/api/operator/status"
)" "direct operator status with invalid token" /tmp/ceiora_control_operator_status_bad_token.json
expect_status "401" "$(
  curl_status /tmp/ceiora_control_refresh_status_bad_token.json \
    -H "X-Operator-Token: ${INVALID_OPERATOR_TOKEN}" \
    "${CONTROL_BASE_URL%/}/api/refresh/status"
)" "direct refresh status with invalid token" /tmp/ceiora_control_refresh_status_bad_token.json
expect_status "401" "$(
  curl_status /tmp/ceiora_operator_status_refresh_token.json \
    -H "X-Refresh-Token: ${OPERATOR_API_TOKEN}" \
    "${APP_BASE_URL%/}/api/operator/status"
)" "proxied operator status with legacy refresh token" /tmp/ceiora_operator_status_refresh_token.json
expect_status "401" "$(
  curl_status /tmp/ceiora_refresh_status_refresh_token.json \
    -H "X-Refresh-Token: ${OPERATOR_API_TOKEN}" \
    "${APP_BASE_URL%/}/api/refresh/status"
)" "proxied refresh status with legacy refresh token" /tmp/ceiora_refresh_status_refresh_token.json
expect_status "401" "$(
  curl_status /tmp/ceiora_control_operator_status_refresh_token.json \
    -H "X-Refresh-Token: ${OPERATOR_API_TOKEN}" \
    "${CONTROL_BASE_URL%/}/api/operator/status"
)" "direct operator status with legacy refresh token" /tmp/ceiora_control_operator_status_refresh_token.json
expect_status "401" "$(
  curl_status /tmp/ceiora_control_refresh_status_refresh_token.json \
    -H "X-Refresh-Token: ${OPERATOR_API_TOKEN}" \
    "${CONTROL_BASE_URL%/}/api/refresh/status"
)" "direct refresh status with legacy refresh token" /tmp/ceiora_control_refresh_status_refresh_token.json
expect_status "401" "$(
  curl_status /tmp/ceiora_refresh_dispatch_anon.json \
    -X POST \
    "${APP_BASE_URL%/}/api/refresh?profile=serve-refresh"
)" "proxied refresh dispatch without token" /tmp/ceiora_refresh_dispatch_anon.json
expect_status "401" "$(
  curl_status /tmp/ceiora_refresh_dispatch_bad_token.json \
    -X POST \
    -H "X-Operator-Token: ${INVALID_OPERATOR_TOKEN}" \
    "${APP_BASE_URL%/}/api/refresh?profile=serve-refresh"
)" "proxied refresh dispatch with invalid token" /tmp/ceiora_refresh_dispatch_bad_token.json
expect_status "401" "$(
  curl_status /tmp/ceiora_control_refresh_dispatch_anon.json \
    -X POST \
    "${CONTROL_BASE_URL%/}/api/refresh?profile=serve-refresh"
)" "direct refresh dispatch without token" /tmp/ceiora_control_refresh_dispatch_anon.json
expect_status "401" "$(
  curl_status /tmp/ceiora_control_refresh_dispatch_bad_token.json \
    -X POST \
    -H "X-Operator-Token: ${INVALID_OPERATOR_TOKEN}" \
    "${CONTROL_BASE_URL%/}/api/refresh?profile=serve-refresh"
)" "direct refresh dispatch with invalid token" /tmp/ceiora_control_refresh_dispatch_bad_token.json
expect_status "401" "$(
  curl_status /tmp/ceiora_refresh_dispatch_refresh_token.json \
    -X POST \
    -H "X-Refresh-Token: ${OPERATOR_API_TOKEN}" \
    "${APP_BASE_URL%/}/api/refresh?profile=serve-refresh"
)" "proxied refresh dispatch with legacy refresh token" /tmp/ceiora_refresh_dispatch_refresh_token.json
expect_status "401" "$(
  curl_status /tmp/ceiora_control_refresh_dispatch_refresh_token.json \
    -X POST \
    -H "X-Refresh-Token: ${OPERATOR_API_TOKEN}" \
    "${CONTROL_BASE_URL%/}/api/refresh?profile=serve-refresh"
)" "direct refresh dispatch with legacy refresh token" /tmp/ceiora_control_refresh_dispatch_refresh_token.json

expect_status "200" "$(
  curl_status /tmp/ceiora_operator_status.json \
    -H "X-Operator-Token: ${OPERATOR_API_TOKEN}" \
    "${APP_BASE_URL%/}/api/operator/status"
)" "proxied operator status with token" /tmp/ceiora_operator_status.json
expect_status "200" "$(
  curl_status /tmp/ceiora_refresh_status.json \
    -H "X-Operator-Token: ${OPERATOR_API_TOKEN}" \
    "${APP_BASE_URL%/}/api/refresh/status"
)" "proxied refresh status with token" /tmp/ceiora_refresh_status.json
expect_status "200" "$(
  curl_status /tmp/ceiora_control_operator_status.json \
    -H "X-Operator-Token: ${OPERATOR_API_TOKEN}" \
    "${CONTROL_BASE_URL%/}/api/operator/status"
)" "direct operator status with token" /tmp/ceiora_control_operator_status.json
expect_status "200" "$(
  curl_status /tmp/ceiora_control_refresh_status.json \
    -H "X-Operator-Token: ${OPERATOR_API_TOKEN}" \
    "${CONTROL_BASE_URL%/}/api/refresh/status"
)" "direct refresh status with token" /tmp/ceiora_control_refresh_status.json
expect_status "200" "$(
  curl_status /tmp/ceiora_operator_status_bearer.json \
    -H "Authorization: Bearer ${OPERATOR_API_TOKEN}" \
    "${APP_BASE_URL%/}/api/operator/status"
)" "proxied operator status with bearer token" /tmp/ceiora_operator_status_bearer.json
expect_status "200" "$(
  curl_status /tmp/ceiora_refresh_status_bearer.json \
    -H "Authorization: Bearer ${OPERATOR_API_TOKEN}" \
    "${APP_BASE_URL%/}/api/refresh/status"
)" "proxied refresh status with bearer token" /tmp/ceiora_refresh_status_bearer.json
expect_status "200" "$(
  curl_status /tmp/ceiora_control_operator_status_bearer.json \
    -H "Authorization: Bearer ${OPERATOR_API_TOKEN}" \
    "${CONTROL_BASE_URL%/}/api/operator/status"
)" "direct operator status with bearer token" /tmp/ceiora_control_operator_status_bearer.json
expect_status "200" "$(
  curl_status /tmp/ceiora_control_refresh_status_bearer.json \
    -H "Authorization: Bearer ${OPERATOR_API_TOKEN}" \
    "${CONTROL_BASE_URL%/}/api/refresh/status"
)" "direct refresh status with bearer token" /tmp/ceiora_control_refresh_status_bearer.json
expect_status "401" "$(curl_status /tmp/ceiora_health_diag_anon.json "${APP_BASE_URL%/}/api/health/diagnostics")" \
  "proxied health diagnostics without token" \
  /tmp/ceiora_health_diag_anon.json
expect_status "401" "$(curl_status /tmp/ceiora_control_health_diag_anon.json "${CONTROL_BASE_URL%/}/api/health/diagnostics")" \
  "direct health diagnostics without token" \
  /tmp/ceiora_control_health_diag_anon.json
expect_status "200" "$(
  curl_status /tmp/ceiora_health_diag.json \
    -H "X-Operator-Token: ${OPERATOR_API_TOKEN}" \
    "${APP_BASE_URL%/}/api/health/diagnostics"
)" "proxied health diagnostics with token" /tmp/ceiora_health_diag.json
expect_status "200" "$(
  curl_status /tmp/ceiora_control_health_diag.json \
    -H "X-Operator-Token: ${OPERATOR_API_TOKEN}" \
    "${CONTROL_BASE_URL%/}/api/health/diagnostics"
)" "direct health diagnostics with token" /tmp/ceiora_control_health_diag.json
expect_status "200" "$(curl_status /tmp/ceiora_data_diag_anon.json "${APP_BASE_URL%/}/api/data/diagnostics")" \
  "proxied data diagnostics without gated flags" \
  /tmp/ceiora_data_diag_anon.json
expect_status "200" "$(curl_status /tmp/ceiora_control_data_diag_anon.json "${CONTROL_BASE_URL%/}/api/data/diagnostics")" \
  "direct data diagnostics without gated flags" \
  /tmp/ceiora_control_data_diag_anon.json
expect_status "401" "$(curl_status /tmp/ceiora_data_diag_gated_anon.json "${APP_BASE_URL%/}/api/data/diagnostics?include_paths=true")" \
  "proxied gated data diagnostics without token" \
  /tmp/ceiora_data_diag_gated_anon.json
expect_status "401" "$(curl_status /tmp/ceiora_control_data_diag_gated_anon.json "${CONTROL_BASE_URL%/}/api/data/diagnostics?include_paths=true")" \
  "direct gated data diagnostics without token" \
  /tmp/ceiora_control_data_diag_gated_anon.json
expect_status "200" "$(
  curl_status /tmp/ceiora_data_diag_gated.json \
    -H "X-Operator-Token: ${OPERATOR_API_TOKEN}" \
    "${APP_BASE_URL%/}/api/data/diagnostics?include_paths=true"
)" "proxied gated data diagnostics with token" /tmp/ceiora_data_diag_gated.json
expect_status "200" "$(
  curl_status /tmp/ceiora_control_data_diag_gated.json \
    -H "X-Operator-Token: ${OPERATOR_API_TOKEN}" \
    "${CONTROL_BASE_URL%/}/api/data/diagnostics?include_paths=true"
)" "direct gated data diagnostics with token" /tmp/ceiora_control_data_diag_gated.json

printf 'proxied operator status: %s\n' "$(tr -d '\n' </tmp/ceiora_operator_status.json)"
printf 'proxied refresh status: %s\n' "$(tr -d '\n' </tmp/ceiora_refresh_status.json)"
printf 'direct operator status: %s\n' "$(tr -d '\n' </tmp/ceiora_control_operator_status.json)"
printf 'direct refresh status: %s\n' "$(tr -d '\n' </tmp/ceiora_control_refresh_status.json)"

assert_json_equal "operator status parity" /tmp/ceiora_operator_status.json /tmp/ceiora_control_operator_status.json "runtime.app_runtime_role"
assert_json_equal "operator status parity" /tmp/ceiora_operator_status.json /tmp/ceiora_control_operator_status.json "runtime.allowed_profiles"
assert_json_equal "refresh status parity" /tmp/ceiora_refresh_status.json /tmp/ceiora_control_refresh_status.json "refresh.status"

if [[ "${RUN_REFRESH_DISPATCH}" != "1" ]]; then
  exit 0
fi

case "${RUN_REFRESH_DISPATCH_TARGET}" in
  proxy)
    dispatch_url="${APP_BASE_URL%/}/api/refresh?profile=serve-refresh"
    dispatch_output="/tmp/ceiora_refresh_dispatch.json"
    dispatch_label="proxied refresh dispatch"
    ;;
  direct)
    dispatch_url="${CONTROL_BASE_URL%/}/api/refresh?profile=serve-refresh"
    dispatch_output="/tmp/ceiora_control_refresh_dispatch.json"
    dispatch_label="direct refresh dispatch"
    ;;
  *)
    printf 'Unsupported RUN_REFRESH_DISPATCH_TARGET: %s\n' "${RUN_REFRESH_DISPATCH_TARGET}" >&2
    exit 1
    ;;
esac

dispatch_status="$(
  curl_status "${dispatch_output}" \
    -X POST \
    -H "X-Operator-Token: ${OPERATOR_API_TOKEN}" \
    "${dispatch_url}"
)"

if [[ "${dispatch_status}" != "202" && "${dispatch_status}" != "409" ]]; then
  printf '%s failed (http %s): %s\n' \
    "${dispatch_label}" \
    "${dispatch_status}" \
    "$(tr -d '\n' <"${dispatch_output}")" >&2
  exit 1
fi

printf '%s: %s\n' "${dispatch_label}" "$(tr -d '\n' <"${dispatch_output}")"

attempt=1
while (( attempt <= REFRESH_POLL_ATTEMPTS )); do
  curl -fsS \
    -H "X-Operator-Token: ${OPERATOR_API_TOKEN}" \
    "${APP_BASE_URL%/}/api/refresh/status" >/tmp/ceiora_refresh_poll.json

  curl -fsS \
    -H "X-Operator-Token: ${OPERATOR_API_TOKEN}" \
    "${CONTROL_BASE_URL%/}/api/refresh/status" >/tmp/ceiora_control_refresh_poll.json

  refresh_status="$(
    python3 - <<'PY'
import json
with open("/tmp/ceiora_refresh_poll.json", "r", encoding="utf-8") as fh:
    payload = json.load(fh)
print(payload.get("refresh", {}).get("status", ""))
PY
  )"

  control_refresh_status="$(
    python3 - <<'PY'
import json
with open("/tmp/ceiora_control_refresh_poll.json", "r", encoding="utf-8") as fh:
    payload = json.load(fh)
print(payload.get("refresh", {}).get("status", ""))
PY
  )"

  printf 'refresh poll %s/%s: proxied=%s direct=%s\n' \
    "${attempt}" \
    "${REFRESH_POLL_ATTEMPTS}" \
    "${refresh_status:-<missing>}" \
    "${control_refresh_status:-<missing>}"

  if [[ -z "${refresh_status}" || -z "${control_refresh_status}" ]]; then
    printf 'refresh poll payload missing refresh.status\n' >&2
    exit 1
  fi

  if [[ "${refresh_status}" != "running" && "${control_refresh_status}" != "running" ]]; then
    exit 0
  fi

  sleep "${REFRESH_POLL_SECONDS}"
  attempt=$((attempt + 1))
done

printf 'refresh remained running after %s polls\n' "${REFRESH_POLL_ATTEMPTS}" >&2
exit 1
