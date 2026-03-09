# Local App Control

This folder is the canonical local launcher for the full app.

Purpose:
- start backend and frontend in the correct order
- kill stale listeners on `3000` and `8000`
- keep PID files and logs in one place
- provide a repeatable local inspection workflow

Runtime files:
- PIDs: `backend/runtime/local_app/pids/`
- Logs: `backend/runtime/local_app/logs/`
- Backend venv: `backend/.venv/`

Commands:
- `./scripts/local_app/up.sh`
- `./scripts/local_app/down.sh`
- `./scripts/local_app/restart.sh`
- `./scripts/local_app/check.sh`
- `./scripts/local_app/status.sh`

Behavior:
- `up.sh` always starts from a clean state.
- It builds the frontend before launch, then starts:
  1. backend
  2. frontend
- It waits for backend health and frontend page readiness before reporting success.
- It bootstraps a project-local backend virtualenv automatically from `backend/pyproject.toml` when needed.

Why this exists:
- ad hoc `uvicorn`/`next` launches were leaving stale listeners and mismatched processes behind
- this module makes launch deterministic instead of session-dependent
