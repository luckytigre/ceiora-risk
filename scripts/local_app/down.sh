#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PID_DIR="$ROOT_DIR/backend/runtime/local_app/pids"
BACKEND_PID_FILE="$PID_DIR/backend.pid"
FRONTEND_PID_FILE="$PID_DIR/frontend.pid"

stop_pid_file() {
  local name="$1"
  local pid_file="$2"
  if [[ ! -f "$pid_file" ]]; then
    echo "$name not running"
    return 0
  fi
  local pid
  pid="$(cat "$pid_file")"
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
    for _ in $(seq 1 20); do
      if ! kill -0 "$pid" >/dev/null 2>&1; then
        break
      fi
      sleep 0.25
    done
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
    echo "stopped $name (pid $pid)"
  else
    echo "$name pid file was stale ($pid)"
  fi
  rm -f "$pid_file"
}

stop_pid_file "frontend" "$FRONTEND_PID_FILE"
stop_pid_file "backend" "$BACKEND_PID_FILE"

stop_listener() {
  local name="$1"
  local port="$2"
  local pids
  pids="$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -z "$pids" ]]; then
    return 0
  fi
  for pid in $pids; do
    kill "$pid" >/dev/null 2>&1 || true
    sleep 0.25
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
    echo "stopped $name listener (pid $pid port $port)"
  done
}

stop_listener "frontend" 3000
stop_listener "backend" 8000
