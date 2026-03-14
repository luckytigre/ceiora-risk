#!/usr/bin/env bash
set -euo pipefail

FRONTEND_ORIGIN="${FRONTEND_ORIGIN:-http://127.0.0.1:3000}"
BACKEND_ORIGIN="${BACKEND_ORIGIN:-http://127.0.0.1:8000}"

echo "[smoke] frontend=${FRONTEND_ORIGIN} backend=${BACKEND_ORIGIN}"

check_http() {
  local url="$1"
  local code
  code="$(curl -s -o /dev/null -w "%{http_code}" "$url")"
  if [[ "$code" != "200" ]]; then
    echo "[FAIL] ${url} -> HTTP ${code}"
    return 1
  fi
  echo "[OK]   ${url}"
}

check_http_follow() {
  local url="$1"
  local code
  code="$(curl -Ls -o /dev/null -w "%{http_code}" "$url")"
  if [[ "$code" != "200" ]]; then
    echo "[FAIL] ${url} -> HTTP ${code} after redirects"
    return 1
  fi
  echo "[OK]   ${url} (redirects)"
}

check_json_key() {
  local url="$1"
  local key="$2"
  local body
  body="$(curl -s "$url")"
  if ! printf '%s' "$body" | grep -q "\"${key}\""; then
    echo "[FAIL] ${url} missing key '${key}'"
    return 1
  fi
  echo "[OK]   ${url} includes '${key}'"
}

check_http "${BACKEND_ORIGIN}/api/health"
check_json_key "${BACKEND_ORIGIN}/api/refresh/status" "refresh"
check_json_key "${BACKEND_ORIGIN}/api/portfolio" "positions"
check_json_key "${BACKEND_ORIGIN}/api/risk" "risk_shares"
check_json_key "${BACKEND_ORIGIN}/api/operator/status" "lanes"

check_http_follow "${FRONTEND_ORIGIN}/"
check_http_follow "${FRONTEND_ORIGIN}/overview"
check_http "${FRONTEND_ORIGIN}/exposures"
check_http "${FRONTEND_ORIGIN}/data"
check_http "${FRONTEND_ORIGIN}/positions"
check_http "${FRONTEND_ORIGIN}/explore"
check_http "${FRONTEND_ORIGIN}/health"

check_json_key "${FRONTEND_ORIGIN}/api/refresh/status" "refresh"
check_json_key "${FRONTEND_ORIGIN}/api/portfolio" "positions"
check_json_key "${FRONTEND_ORIGIN}/api/risk" "risk_shares"

echo "[smoke] PASS"
