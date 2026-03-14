#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

print_status

backend_session="$(read_text_file "${BACKEND_SESSION_FILE}")"
frontend_session="$(read_text_file "${FRONTEND_SESSION_FILE}")"

if [[ -n "${backend_session}" ]] && command_exists screen && screen_session_exists "${backend_session}"; then
  printf 'backend_session_health=ok\n'
else
  printf 'backend_session_health=down\n'
fi

if [[ -n "${frontend_session}" ]] && command_exists screen && screen_session_exists "${frontend_session}"; then
  printf 'frontend_session_health=ok\n'
else
  printf 'frontend_session_health=down\n'
fi

if curl -fsS "${BACKEND_URL}/api/health" >/dev/null 2>&1; then
  printf 'backend_health=ok\n'
else
  printf 'backend_health=down\n'
fi

if curl -LfsS "${FRONTEND_URL}/" >/dev/null 2>&1 \
  && curl -LfsS "${FRONTEND_URL}/overview" >/dev/null 2>&1 \
  && curl -fsS "${FRONTEND_URL}/exposures" >/dev/null 2>&1; then
  printf 'frontend_health=ok\n'
else
  printf 'frontend_health=down\n'
fi
