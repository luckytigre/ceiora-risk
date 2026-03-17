#!/usr/bin/env bash
set -euo pipefail

retry() {
  local tries="$1"
  shift
  local delay="$1"
  shift
  local attempt=1
  while (( attempt <= tries )); do
    if "$@"; then
      return 0
    fi
    sleep "$delay"
    attempt=$((attempt + 1))
  done
  return 1
}

backend_status=""
frontend_head=""

if retry 10 1 bash -lc "curl -fsS http://127.0.0.1:8000/api/health >/tmp/barra_backend_health.json"; then
  backend_status="$(cat /tmp/barra_backend_health.json)"
else
  backend_status="unreachable"
fi

if retry 10 1 bash -lc "curl -fsSI http://127.0.0.1:3000 >/tmp/barra_frontend_head.txt"; then
  frontend_head="$(sed -n '1p' /tmp/barra_frontend_head.txt)"
else
  frontend_head="unreachable"
fi

echo "backend: ${backend_status:-unreachable}"
echo "frontend: ${frontend_head:-unreachable}"
