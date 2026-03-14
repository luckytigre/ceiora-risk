# Operations Hardening Checklist

Use this checklist before and after backend/frontend refreshes to keep runtime stable.

## 1) Pre-Run Hygiene
- Confirm clean startup ports:
  - Backend: `127.0.0.1:8000`
  - Frontend: `127.0.0.1:3000`
- Remove stale dev build artifacts before starting frontend:
  - `cd frontend && rm -rf .next`
- Ensure required env vars are set (`.env`/shell):
  - `LSEG_APP_KEY` (for local LSEG pulls)
  - Neon vars if read-through/cutover mode is enabled

## 2) Safe Startup
- Preferred startup command:
  - `make frontend-safe` (frontend)
  - your normal backend start command, or `./start.sh`
- Avoid running multiple frontend dev servers simultaneously.
- Keep hostnames consistent (`127.0.0.1` preferred) to reduce Next dev chunk mismatch issues.

## 3) Smoke Check (Required)
- Run:
  - `make smoke-check`
- Pass criteria:
  - backend endpoints are `200`
  - root and legacy overview routes redirect cleanly (`/`, `/overview`)
  - frontend routes render (`/exposures`, `/data`, `/positions`, `/explore`, `/health`)
  - frontend API proxies return expected keys (`refresh`, `positions`, `risk_shares`)

## 4) Release Readiness (Local)
- Run targeted tests for touched behavior:
  - `pytest -q backend/tests/test_serving_output_route_preference.py`
  - `pytest -q backend/tests/test_cache_publisher_service.py`
  - `pytest -q backend/tests/test_neon_parity_value_checks.py`
- If refresh/holdings flows changed, add/execute a matching targeted test.
- If health/risk math changed, also run a broader backend slice before considering the runtime clean.
- If factor definitions or style membership changed, run `cold-core` once to rebuild factor history and then a follow-up `serve-refresh` to confirm the lightweight path serves the new factor set cleanly.
- Verify no accidental transient files are staged (`frontend/.next*`, temp exports, logs).

## 5) Rollback Pointers
- If frontend runtime breaks (`__webpack_modules__[moduleId]` / missing chunk):
  1. Stop frontend server(s)
  2. `cd frontend && rm -rf .next`
  3. Restart with pinned host/port config
- If backend cache readiness errors appear:
  - Run refresh and verify `/api/refresh/status` moves to healthy state.
- If Health page semantics look stale:
  - verify `/api/health/diagnostics` is reading the expected durable current payload
  - verify the latest refresh staged and published `health_diagnostics`
