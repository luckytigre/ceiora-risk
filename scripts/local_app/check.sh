#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

backend_pid="$(read_pid "${BACKEND_PID_FILE}")"
frontend_pid="$(read_pid "${FRONTEND_PID_FILE}")"
backend_session="$(read_text_file "${BACKEND_SESSION_FILE}")"
frontend_session="$(read_text_file "${FRONTEND_SESSION_FILE}")"

require_screen

if [[ -z "${backend_session}" ]] || ! screen_session_exists "${backend_session}"; then
  fail "Backend screen session is not alive"
fi
if [[ -z "${frontend_session}" ]] || ! screen_session_exists "${frontend_session}"; then
  fail "Frontend screen session is not alive"
fi

if ! pid_alive "${backend_pid}"; then
  fail "Backend PID is not alive"
fi
if ! pid_alive "${frontend_pid}"; then
  fail "Frontend PID is not alive"
fi

curl -fsS "${BACKEND_URL}/api/health" >/dev/null
curl -LfsS "${FRONTEND_URL}/" >/dev/null
curl -LfsS "${FRONTEND_URL}/overview" >/dev/null
curl -fsS "${FRONTEND_URL}/exposures" >/dev/null
curl -fsS "${FRONTEND_URL}/data" >/dev/null
curl -fsS "${FRONTEND_URL}/api/portfolio" >/dev/null
curl -fsS "${FRONTEND_URL}/api/risk" >/dev/null
curl -fsS "${FRONTEND_URL}/api/operator/status" >/dev/null

log "Local app check passed"
print_status
