#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUNTIME_DIR="$ROOT_DIR/backend/runtime/local_app"
LOG_DIR="$RUNTIME_DIR/logs"
PID_DIR="$RUNTIME_DIR/pids"
BACKEND_PID_FILE="$PID_DIR/backend.pid"
FRONTEND_PID_FILE="$PID_DIR/frontend.pid"
BACKEND_LOG="$LOG_DIR/backend.log"
FRONTEND_LOG="$LOG_DIR/frontend.log"
BACKEND_CMD=("$ROOT_DIR/.venv_local/bin/python" -m uvicorn backend.main:app --host 127.0.0.1 --port 8000 --workers 1)

mkdir -p "$LOG_DIR" "$PID_DIR"

is_running() {
  local pid="$1"
  kill -0 "$pid" >/dev/null 2>&1
}

start_backend() {
  if [[ -f "$BACKEND_PID_FILE" ]] && is_running "$(cat "$BACKEND_PID_FILE")"; then
    echo "backend already running (pid $(cat "$BACKEND_PID_FILE"))"
    return 0
  fi
  : > "$BACKEND_LOG"
  (
    cd "$ROOT_DIR"
    nohup "${BACKEND_CMD[@]}" >>"$BACKEND_LOG" 2>&1 &
    echo $! > "$BACKEND_PID_FILE"
  )
  echo "started backend (pid $(cat "$BACKEND_PID_FILE"))"
}

start_frontend() {
  if [[ -f "$FRONTEND_PID_FILE" ]] && is_running "$(cat "$FRONTEND_PID_FILE")"; then
    echo "frontend already running (pid $(cat "$FRONTEND_PID_FILE"))"
    return 0
  fi
  : > "$FRONTEND_LOG"
  (
    cd "$ROOT_DIR/frontend"
    nohup npm run dev -- --hostname 127.0.0.1 --port 3000 >>"$FRONTEND_LOG" 2>&1 &
    echo $! > "$FRONTEND_PID_FILE"
  )
  echo "started frontend (pid $(cat "$FRONTEND_PID_FILE"))"
}

start_backend
start_frontend

