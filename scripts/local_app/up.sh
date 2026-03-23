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
BACKEND_CMD=(env PYTHONUNBUFFERED=1 "$ROOT_DIR/.venv_local/bin/python" -m uvicorn backend.main:app --host 127.0.0.1 --port 8000 --workers 1)
FRONTEND_CMD=(env NEXT_TELEMETRY_DISABLED=1 npm run dev -- --hostname 127.0.0.1 --port 3000)

mkdir -p "$LOG_DIR" "$PID_DIR"

if [[ ! -x "$ROOT_DIR/.venv_local/bin/python" ]]; then
  echo "missing .venv_local backend environment"
  echo "run ./scripts/setup_local_env.sh first"
  exit 1
fi

is_running() {
  local pid="$1"
  kill -0 "$pid" >/dev/null 2>&1
}

listener_pid() {
  local port="$1"
  lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null | head -n 1 || true
}

print_log_tail() {
  local log_file="$1"
  if [[ -s "$log_file" ]]; then
    echo "--- tail $log_file ---"
    tail -n 80 "$log_file" || true
    echo "--- end tail ---"
    return 0
  fi
  echo "log is empty: $log_file"
}

wait_for_startup() {
  local name="$1"
  local pid_file="$2"
  local port="$3"
  local log_file="$4"
  local timeout_secs="${5:-20}"
  local checks=$(( timeout_secs * 2 ))

  for _ in $(seq 1 "$checks"); do
    if [[ ! -f "$pid_file" ]]; then
      echo "$name failed to create pid file"
      print_log_tail "$log_file"
      return 1
    fi
    local pid
    pid="$(cat "$pid_file")"
    if ! is_running "$pid"; then
      echo "$name exited during startup (pid $pid)"
      rm -f "$pid_file"
      print_log_tail "$log_file"
      return 1
    fi
    local listener
    listener="$(listener_pid "$port")"
    if [[ -n "$listener" ]]; then
      echo "$name ready on port $port (pid $listener)"
      return 0
    fi
    sleep 0.5
  done

  echo "$name did not open port $port within ${timeout_secs}s"
  local pid
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  if [[ -n "$pid" ]]; then
    kill "$pid" >/dev/null 2>&1 || true
    sleep 0.5
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
  fi
  rm -f "$pid_file"
  print_log_tail "$log_file"
  return 1
}

start_backend() {
  if [[ -f "$BACKEND_PID_FILE" ]] && is_running "$(cat "$BACKEND_PID_FILE")"; then
    echo "backend already running (pid $(cat "$BACKEND_PID_FILE"))"
    return 0
  fi
  : > "$BACKEND_LOG"
  local old_pwd="$PWD"
  cd "$ROOT_DIR"
  nohup "${BACKEND_CMD[@]}" >>"$BACKEND_LOG" 2>&1 &
  local pid=$!
  cd "$old_pwd"
  echo "$pid" > "$BACKEND_PID_FILE"
  echo "started backend (pid $(cat "$BACKEND_PID_FILE"))"
  wait_for_startup "backend" "$BACKEND_PID_FILE" 8000 "$BACKEND_LOG" 20
}

start_frontend() {
  if [[ -f "$FRONTEND_PID_FILE" ]] && is_running "$(cat "$FRONTEND_PID_FILE")"; then
    echo "frontend already running (pid $(cat "$FRONTEND_PID_FILE"))"
    return 0
  fi
  : > "$FRONTEND_LOG"
  local old_pwd="$PWD"
  cd "$ROOT_DIR/frontend"
  nohup "${FRONTEND_CMD[@]}" >>"$FRONTEND_LOG" 2>&1 &
  local pid=$!
  cd "$old_pwd"
  echo "$pid" > "$FRONTEND_PID_FILE"
  echo "started frontend (pid $(cat "$FRONTEND_PID_FILE"))"
  wait_for_startup "frontend" "$FRONTEND_PID_FILE" 3000 "$FRONTEND_LOG" 30
}

start_backend
start_frontend
