#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUNTIME_DIR="${ROOT_DIR}/backend/runtime/local_app"
PID_DIR="${RUNTIME_DIR}/pids"
LOG_DIR="${RUNTIME_DIR}/logs"
BACKEND_VENV_DIR="${ROOT_DIR}/backend/.venv"
BACKEND_PYTHON="${BACKEND_VENV_DIR}/bin/python"
BACKEND_PIP="${BACKEND_VENV_DIR}/bin/pip"
BACKEND_DEPS_STAMP="${BACKEND_VENV_DIR}/.deps_installed"
BACKEND_PID_FILE="${PID_DIR}/backend.pid"
FRONTEND_PID_FILE="${PID_DIR}/frontend.pid"
BACKEND_LOG="${LOG_DIR}/backend.log"
FRONTEND_LOG="${LOG_DIR}/frontend.log"
BACKEND_HOST="${BACKEND_HOST:-127.0.0.1}"
BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_HOST="${FRONTEND_HOST:-127.0.0.1}"
FRONTEND_PORT="${FRONTEND_PORT:-3000}"
BACKEND_URL="http://${BACKEND_HOST}:${BACKEND_PORT}"
FRONTEND_URL="http://${FRONTEND_HOST}:${FRONTEND_PORT}"

mkdir -p "${PID_DIR}" "${LOG_DIR}"

log() {
  printf '[local-app] %s\n' "$*"
}

fail() {
  printf '[local-app] ERROR: %s\n' "$*" >&2
  exit 1
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

read_pid() {
  local file="$1"
  if [[ -f "${file}" ]]; then
    tr -d '[:space:]' <"${file}"
  fi
  return 0
}

pid_alive() {
  local pid="$1"
  [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1
}

remove_pid_file() {
  local file="$1"
  if [[ -f "${file}" ]]; then
    rm -f "${file}"
  fi
  return 0
}

kill_pid_file() {
  local file="$1"
  local pid
  pid="$(read_pid "${file}")"
  if pid_alive "${pid}"; then
    kill "${pid}" >/dev/null 2>&1 || true
    sleep 1
    if pid_alive "${pid}"; then
      kill -9 "${pid}" >/dev/null 2>&1 || true
    fi
  fi
  remove_pid_file "${file}"
}

kill_port_listener() {
  local port="$1"
  local pids
  if command_exists lsof; then
    pids="$(lsof -tiTCP:${port} -sTCP:LISTEN 2>/dev/null || true)"
    if [[ -n "${pids}" ]]; then
      log "Killing stale listener(s) on port ${port}: ${pids//$'\n'/ }"
      kill ${pids} >/dev/null 2>&1 || true
      sleep 1
      pids="$(lsof -tiTCP:${port} -sTCP:LISTEN 2>/dev/null || true)"
      if [[ -n "${pids}" ]]; then
        kill -9 ${pids} >/dev/null 2>&1 || true
      fi
    fi
  fi
}

wait_for_http() {
  local url="$1"
  local timeout_seconds="${2:-30}"
  local start_ts
  start_ts="$(date +%s)"
  while true; do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    if (( "$(date +%s)" - start_ts >= timeout_seconds )); then
      return 1
    fi
    sleep 1
  done
}

wait_for_frontend() {
  local timeout_seconds="${1:-45}"
  local start_ts
  start_ts="$(date +%s)"
  while true; do
    if curl -fsS "${FRONTEND_URL}/overview" >/dev/null 2>&1; then
      return 0
    fi
    if (( "$(date +%s)" - start_ts >= timeout_seconds )); then
      return 1
    fi
    sleep 1
  done
}

ensure_backend_env() {
  if [[ ! -x "${BACKEND_PYTHON}" ]]; then
    log "Creating backend virtualenv at ${BACKEND_VENV_DIR}"
    python3 -m venv "${BACKEND_VENV_DIR}"
  fi

  if [[ ! -f "${BACKEND_DEPS_STAMP}" || "${ROOT_DIR}/backend/pyproject.toml" -nt "${BACKEND_DEPS_STAMP}" ]]; then
    log "Installing backend dependencies into project virtualenv"
    "${BACKEND_PYTHON}" -m pip install --upgrade pip >>"${BACKEND_LOG}" 2>&1
    (cd "${ROOT_DIR}/backend" && "${BACKEND_PYTHON}" -m pip install -e ".[dev]") >>"${BACKEND_LOG}" 2>&1
    touch "${BACKEND_DEPS_STAMP}"
  fi
}

ensure_frontend_build() {
  log "Building frontend"
  (cd "${ROOT_DIR}" && npm --prefix frontend run build) >>"${FRONTEND_LOG}" 2>&1
}

start_backend() {
  ensure_backend_env
  log "Starting backend on ${BACKEND_URL}"
  nohup bash -lc "cd '${ROOT_DIR}' && '${BACKEND_PYTHON}' -m uvicorn backend.main:app --host ${BACKEND_HOST} --port ${BACKEND_PORT}" >>"${BACKEND_LOG}" 2>&1 &
  echo $! >"${BACKEND_PID_FILE}"
  if ! wait_for_http "${BACKEND_URL}/api/health" 30; then
    tail -n 80 "${BACKEND_LOG}" >&2 || true
    fail "Backend did not become healthy"
  fi
}

start_frontend() {
  ensure_frontend_build
  log "Starting frontend on ${FRONTEND_URL}"
  nohup bash -lc "cd '${ROOT_DIR}' && npm --prefix frontend run start -- --hostname ${FRONTEND_HOST} --port ${FRONTEND_PORT}" >>"${FRONTEND_LOG}" 2>&1 &
  echo $! >"${FRONTEND_PID_FILE}"
  if ! wait_for_frontend 45; then
    tail -n 120 "${FRONTEND_LOG}" >&2 || true
    fail "Frontend did not become healthy"
  fi
}

stop_all() {
  kill_pid_file "${FRONTEND_PID_FILE}"
  kill_pid_file "${BACKEND_PID_FILE}"
  kill_port_listener "${FRONTEND_PORT}"
  kill_port_listener "${BACKEND_PORT}"
}

print_status() {
  local backend_pid frontend_pid
  backend_pid="$(read_pid "${BACKEND_PID_FILE}")"
  frontend_pid="$(read_pid "${FRONTEND_PID_FILE}")"
  printf 'backend_pid=%s\n' "${backend_pid:-}"
  printf 'frontend_pid=%s\n' "${frontend_pid:-}"
  printf 'backend_url=%s\n' "${BACKEND_URL}"
  printf 'frontend_url=%s\n' "${FRONTEND_URL}"
  printf 'backend_log=%s\n' "${BACKEND_LOG}"
  printf 'frontend_log=%s\n' "${FRONTEND_LOG}"
  printf 'backend_python=%s\n' "${BACKEND_PYTHON}"
}
