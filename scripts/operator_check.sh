#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

BACKEND_PYTHON="${BACKEND_PYTHON:-${ROOT_DIR}/backend/.venv/bin/python}"
CONTROL_BASE_URL="${CONTROL_BASE_URL:-${APP_BASE_URL:-}}"
RUN_REFRESH_DISPATCH="${RUN_REFRESH_DISPATCH:-0}"
REFRESH_POLL_SECONDS="${REFRESH_POLL_SECONDS:-5}"
REFRESH_POLL_ATTEMPTS="${REFRESH_POLL_ATTEMPTS:-36}"

if [[ ! -x "${BACKEND_PYTHON}" ]]; then
  printf 'Missing backend Python executable: %s\n' "${BACKEND_PYTHON}" >&2
  exit 1
fi

"${BACKEND_PYTHON}" -m pytest \
  backend/tests/test_operator_status_route.py \
  backend/tests/test_refresh_auth.py \
  backend/tests/test_refresh_control_service.py \
  -q

if [[ -z "${APP_BASE_URL:-}" ]]; then
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

curl -fsS \
  -H "X-Operator-Token: ${OPERATOR_API_TOKEN}" \
  "${APP_BASE_URL%/}/api/operator/status" >/tmp/ceiora_operator_status.json

curl -fsS \
  -H "X-Operator-Token: ${OPERATOR_API_TOKEN}" \
  "${APP_BASE_URL%/}/api/refresh/status" >/tmp/ceiora_refresh_status.json

printf 'operator status: %s\n' "$(tr -d '\n' </tmp/ceiora_operator_status.json)"
printf 'refresh status: %s\n' "$(tr -d '\n' </tmp/ceiora_refresh_status.json)"

if [[ "${RUN_REFRESH_DISPATCH}" != "1" ]]; then
  exit 0
fi

dispatch_status="$(
  curl -sS \
    -o /tmp/ceiora_refresh_dispatch.json \
    -w "%{http_code}" \
    -X POST \
    -H "X-Operator-Token: ${OPERATOR_API_TOKEN}" \
    "${CONTROL_BASE_URL%/}/api/refresh?profile=serve-refresh"
)"

if [[ "${dispatch_status}" != "202" && "${dispatch_status}" != "409" ]]; then
  printf 'refresh dispatch failed (http %s): %s\n' \
    "${dispatch_status}" \
    "$(tr -d '\n' </tmp/ceiora_refresh_dispatch.json)" >&2
  exit 1
fi

printf 'refresh dispatch: %s\n' "$(tr -d '\n' </tmp/ceiora_refresh_dispatch.json)"

attempt=1
while (( attempt <= REFRESH_POLL_ATTEMPTS )); do
  curl -fsS \
    -H "X-Operator-Token: ${OPERATOR_API_TOKEN}" \
    "${CONTROL_BASE_URL%/}/api/refresh/status" >/tmp/ceiora_refresh_poll.json

  refresh_status="$(
    python3 - <<'PY'
import json
with open("/tmp/ceiora_refresh_poll.json", "r", encoding="utf-8") as fh:
    payload = json.load(fh)
print(payload.get("refresh", {}).get("status", ""))
PY
  )"

  printf 'refresh poll %s/%s: %s\n' "${attempt}" "${REFRESH_POLL_ATTEMPTS}" "${refresh_status:-<missing>}"

  if [[ -z "${refresh_status}" ]]; then
    printf 'refresh poll payload missing refresh.status\n' >&2
    exit 1
  fi

  if [[ "${refresh_status}" != "running" ]]; then
    exit 0
  fi

  sleep "${REFRESH_POLL_SECONDS}"
  attempt=$((attempt + 1))
done

printf 'refresh remained running after %s polls\n' "${REFRESH_POLL_ATTEMPTS}" >&2
exit 1
