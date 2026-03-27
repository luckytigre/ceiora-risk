#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

BACKEND_PYTHON="${BACKEND_PYTHON:-${ROOT_DIR}/backend/.venv/bin/python}"

if [[ ! -x "${BACKEND_PYTHON}" ]]; then
  printf 'Missing backend Python executable: %s\n' "${BACKEND_PYTHON}" >&2
  exit 1
fi

"${BACKEND_PYTHON}" -m pytest \
  backend/tests/test_app_surfaces.py \
  backend/tests/test_cloud_run_jobs.py \
  backend/tests/test_refresh_control_service.py \
  backend/tests/test_refresh_auth.py \
  -q

(
  cd frontend
  npm run test:control-plane-proxies
)
