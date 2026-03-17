#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PID_DIR="$ROOT_DIR/backend/runtime/local_app/pids"
LOG_DIR="$ROOT_DIR/backend/runtime/local_app/logs"

print_status() {
  local name="$1"
  local pid_file="$2"
  if [[ -f "$pid_file" ]]; then
    local pid
    pid="$(cat "$pid_file")"
    if kill -0 "$pid" >/dev/null 2>&1; then
      echo "$name: running (pid $pid)"
      return 0
    fi
    echo "$name: stale pid file ($pid)"
    return 0
  fi
  echo "$name: stopped"
}

print_status "backend" "$PID_DIR/backend.pid"
print_status "frontend" "$PID_DIR/frontend.pid"
echo "backend log: $LOG_DIR/backend.log"
echo "frontend log: $LOG_DIR/frontend.log"
echo "backend url: http://127.0.0.1:8000"
echo "frontend url: http://127.0.0.1:3000"
echo "backend listener pids: $(lsof -tiTCP:8000 -sTCP:LISTEN 2>/dev/null | tr '\n' ' ' | sed 's/ $//' || true)"
echo "frontend listener pids: $(lsof -tiTCP:3000 -sTCP:LISTEN 2>/dev/null | tr '\n' ' ' | sed 's/ $//' || true)"
