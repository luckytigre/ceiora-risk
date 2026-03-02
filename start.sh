#!/bin/bash
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
BACKEND_PORT=8001
FRONTEND_PORT=3002
BACKEND_PID=""
FRONTEND_PID=""

cleanup() {
  echo ""
  echo "Shutting down..."
  [ -n "$BACKEND_PID" ] && kill "$BACKEND_PID" 2>/dev/null
  [ -n "$FRONTEND_PID" ] && kill "$FRONTEND_PID" 2>/dev/null
  wait 2>/dev/null
  echo "Done."
}
trap cleanup EXIT INT TERM

kill_port_listener() {
  local port="$1"
  local pids
  pids="$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
  if [ -n "$pids" ]; then
    echo "Port $port in use by PID(s): $pids. Stopping stale listener(s)..."
    kill $pids 2>/dev/null || true
    sleep 1
    pids="$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
    if [ -n "$pids" ]; then
      echo "Force stopping PID(s) on port $port: $pids"
      kill -9 $pids 2>/dev/null || true
      sleep 1
    fi
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
if [ ! -d "$DIR/frontend/node_modules" ]; then
  echo "Installing frontend dependencies..."
  (cd "$DIR/frontend" && npm install --silent)
fi

if ! python3 -c "import fastapi; import psycopg; import pydantic" 2>/dev/null; then
  echo "Installing backend dependencies..."
  (cd "$DIR/backend" && pip install -e ".[dev]" -q)
fi

# --- Clear stale listeners on target ports ---
kill_port_listener "$BACKEND_PORT"
kill_port_listener "$FRONTEND_PORT"

# --- Start backend ---
echo "Starting backend on :$BACKEND_PORT..."
(cd "$DIR/backend" && uvicorn main:app --reload --port "$BACKEND_PORT") &
BACKEND_PID=$!

# --- Start frontend ---
echo "Starting frontend on :$FRONTEND_PORT..."
(cd "$DIR/frontend" && npx next dev --port "$FRONTEND_PORT") &
FRONTEND_PID=$!

# --- Wait for backend, then refresh ---
echo "Waiting for backend..."
for i in $(seq 1 30); do
  if curl -s "http://localhost:$BACKEND_PORT/api/health" >/dev/null 2>&1; then
    echo "Backend ready. Triggering data refresh..."
    curl -s -X POST "http://localhost:$BACKEND_PORT/api/refresh" | python3 -m json.tool 2>/dev/null || true
    break
  fi
  sleep 1
done

# --- Validate refresh/cache state ---
echo "Checking cached portfolio payload..."
curl -sf "http://localhost:$BACKEND_PORT/api/portfolio" >/dev/null

echo ""
echo "==================================="
echo "  Open http://localhost:$FRONTEND_PORT"
echo "  Ctrl+C to stop"
echo "==================================="

wait
