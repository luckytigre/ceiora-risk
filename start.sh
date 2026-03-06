#!/bin/bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
BACKEND_PORT=8000
FRONTEND_PORT=3000
BACKEND_HOST="${BACKEND_HOST:-127.0.0.1}"
FRONTEND_HOST="${FRONTEND_HOST:-127.0.0.1}"
BACKEND_PID=""
FRONTEND_PID=""
FORCE_KILL_PORTS="${FORCE_KILL_PORTS:-0}"

# Keep app storage in one explicit location unless caller overrides.
export APP_DATA_DIR="${APP_DATA_DIR:-$DIR/backend/runtime}"

cleanup() {
  echo ""
  echo "Shutting down..."
  [ -n "$BACKEND_PID" ] && kill "$BACKEND_PID" 2>/dev/null
  [ -n "$FRONTEND_PID" ] && kill "$FRONTEND_PID" 2>/dev/null
  wait 2>/dev/null
  echo "Done."
}
trap cleanup EXIT INT TERM

print_port_listener() {
  local port="$1"
  lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null || true
}

ensure_port_available() {
  local port="$1"
  local pids
  pids="$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
  if [ -z "$pids" ]; then
    return 0
  fi

  echo "Port $port is already in use:"
  print_port_listener "$port"

  if [ "$FORCE_KILL_PORTS" != "1" ]; then
    echo "Refusing to kill external processes by default."
    echo "Free the port manually, or re-run with FORCE_KILL_PORTS=1."
    exit 1
  fi

  echo "FORCE_KILL_PORTS=1 set. Stopping listener(s) on $port..."
  kill $pids 2>/dev/null || true
  sleep 1
  pids="$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
  if [ -n "$pids" ]; then
    echo "Process still bound to $port. Force-stopping: $pids"
    kill -9 $pids 2>/dev/null || true
    sleep 1
  fi
  if lsof -tiTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "ERROR: Failed to free port $port."
    exit 1
  fi
}

# --- Check .env ---
if [ ! -f "$DIR/backend/.env" ]; then
  if [ -f "$DIR/.env" ]; then
    cp "$DIR/.env" "$DIR/backend/.env"
  else
    echo "No .env found. Copying .env.example → backend/.env"
    echo "Edit backend/.env with your Postgres credentials, then re-run."
    cp "$DIR/.env.example" "$DIR/backend/.env"
    exit 1
  fi
fi

# --- Install deps if needed ---
if [ ! -d "$DIR/frontend/node_modules" ] || [ ! -e "$DIR/frontend/node_modules/.bin/next" ]; then
  echo "Installing frontend dependencies..."
  (cd "$DIR/frontend" && npm install --silent)
fi
chmod +x "$DIR/frontend/node_modules/.bin/"* 2>/dev/null || true

if ! python3 -c "import fastapi; import psycopg; import pydantic" 2>/dev/null; then
  echo "Installing backend dependencies..."
  if ! (cd "$DIR/backend" && python3 -m pip install -e ".[dev]" -q); then
    python3 -m pip install -q fastapi "uvicorn[standard]" "psycopg[binary]" pydantic pandas numpy scipy python-dotenv
  fi
fi

if ! python3 -c "import exchange_calendars" 2>/dev/null; then
  echo "Installing trading-calendar dependency..."
  python3 -m pip install -q exchange-calendars
fi

# --- Ensure target ports are available ---
ensure_port_available "$BACKEND_PORT"
ensure_port_available "$FRONTEND_PORT"

# --- Start backend ---
echo "Starting backend on :$BACKEND_PORT..."
(cd "$DIR" && uvicorn backend.main:app --reload --port "$BACKEND_PORT") &
BACKEND_PID=$!

# --- Start frontend ---
echo "Starting frontend on :$FRONTEND_PORT..."
(cd "$DIR/frontend" && npm run dev) &
FRONTEND_PID=$!

# --- Wait for backend ---
echo "Waiting for backend..."
BACKEND_READY=0
for i in $(seq 1 30); do
  if curl -sf "http://$BACKEND_HOST:$BACKEND_PORT/api/health" >/dev/null 2>&1; then
    BACKEND_READY=1
    break
  fi
  sleep 1
done

if [ "$BACKEND_READY" -ne 1 ]; then
  echo "ERROR: Backend did not become ready on :$BACKEND_PORT"
  exit 1
fi

# --- Wait for frontend ---
echo "Waiting for frontend..."
FRONTEND_READY=0
for i in $(seq 1 30); do
  if curl -sfI "http://$FRONTEND_HOST:$FRONTEND_PORT" >/dev/null 2>&1; then
    FRONTEND_READY=1
    break
  fi
  sleep 1
done
if [ "$FRONTEND_READY" -ne 1 ]; then
  echo "ERROR: Frontend did not become ready on :$FRONTEND_PORT"
  exit 1
fi

# --- Trigger non-blocking light refresh ---
echo "Triggering background light refresh..."
REFRESH_TOKEN="${REFRESH_API_TOKEN:-}"
if [ -z "$REFRESH_TOKEN" ] && [ -f "$DIR/backend/.env" ]; then
  REFRESH_TOKEN="$(grep -E '^REFRESH_API_TOKEN=' "$DIR/backend/.env" | tail -n1 | cut -d= -f2- | tr -d '\r' || true)"
fi
if [ -n "$REFRESH_TOKEN" ]; then
  if ! curl -sf -X POST -H "X-Refresh-Token: $REFRESH_TOKEN" "http://$BACKEND_HOST:$BACKEND_PORT/api/refresh?mode=light" >/dev/null; then
    echo "WARNING: Could not trigger background refresh. App is running, but data may be stale."
  fi
else
  if ! curl -sf -X POST "http://$BACKEND_HOST:$BACKEND_PORT/api/refresh?mode=light" >/dev/null; then
    echo "WARNING: Could not trigger background refresh. App is running, but data may be stale."
  fi
fi

echo ""
echo "==================================="
echo "  Open http://$FRONTEND_HOST:$FRONTEND_PORT"
echo "  Refresh status: http://$BACKEND_HOST:$BACKEND_PORT/api/refresh/status"
echo "  Ctrl+C to stop"
echo "==================================="

wait
