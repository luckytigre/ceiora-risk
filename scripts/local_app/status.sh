#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

print_status

if curl -fsS "${BACKEND_URL}/api/health" >/dev/null 2>&1; then
  printf 'backend_health=ok\n'
else
  printf 'backend_health=down\n'
fi

if curl -fsS "${FRONTEND_URL}/overview" >/dev/null 2>&1; then
  printf 'frontend_health=ok\n'
else
  printf 'frontend_health=down\n'
fi
