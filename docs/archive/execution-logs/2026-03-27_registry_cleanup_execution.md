# 2026-03-27 Registry Cleanup Execution

## Scope

- execute the reviewed boundary-freeze and readiness-gate plan on the live `ceiora-risk` worktree without widening into the shared universe-runtime no-go zone
- restore the repo-owned smoke/operator validation path
- run the Neon-backed operational checks that are executable from this machine

## Boundary Freeze

No-go overlap zone during this execution:

- `backend/universe/runtime_rows.py`
- `backend/universe/security_master_sync.py`
- `backend/universe/taxonomy_builder.py`
- `backend/universe/selectors.py`
- `backend/data/source_reads.py`
- `backend/data/core_reads.py`
- `backend/risk_model/cuse_membership.py`
- directly coupled tests in the same seams
- the active plan file `docs/architecture/UNIVERSE_REGISTRY_AND_MODEL_GATING_PLAN.md`

Reason:

- those files were already dirty in the worktree and were known to be under concurrent runtime-contract work

## Environment Readiness Gate

Verified locally:

- `gcloud`, `docker`, `make`, and repo virtualenvs are present
- `backend/.env` exists and, when sourced with the repo venv, resolves a valid Neon DSN
- `backend/.env` does not provide the full Cloud Run/control-plane execution surface:
  - no app/control base URLs
  - no operator token
  - no Cloud Run project/region/job env in the current shell
- `gcloud auth list` and `gcloud config get-value project` were empty in this shell

Operational implication:

- Neon-backed local checks and sync were executable
- Cloud Run deploy and live control-plane refresh dispatch were not executable from this shell

## Repo Slice: Smoke/Operator Validation Hygiene

Problem found:

- `make smoke-check` and `make operator-check` were stalling/failing during pytest collection because `backend/ops/cloud_run_jobs.py` imported `google.auth` at module import time

Change made:

- deferred the `google.auth` and `Request` imports behind local helper functions in `backend/ops/cloud_run_jobs.py`
- updated the direct adapter tests in `backend/tests/test_cloud_run_jobs.py` to patch those helpers instead of patching `cloud_run_jobs.google.auth`

Validation:

- `python3 -m py_compile backend/ops/cloud_run_jobs.py backend/tests/test_cloud_run_jobs.py`
- `git diff --check -- backend/ops/cloud_run_jobs.py backend/tests/test_cloud_run_jobs.py`
- `backend/.venv/bin/python -m pytest backend/tests/test_cloud_run_jobs.py backend/tests/test_refresh_auth.py backend/tests/test_refresh_control_service.py backend/tests/test_operator_status_route.py backend/tests/test_app_surfaces.py -q`
- `BACKEND_PYTHON=backend/.venv/bin/python make operator-check`
- `BACKEND_PYTHON=backend/.venv/bin/python make smoke-check`

Result:

- the repo-owned smoke/operator script path is usable again in this environment
- live operator smoke still correctly skipped because `APP_BASE_URL` and `OPERATOR_API_TOKEN` are not set

## Operational Slice: Neon Authority Checks

Preflight:

- `backend/.venv/bin/python -m backend.scripts.neon_preflight_check --json`

Result:

- DSN valid
- Neon connectivity succeeded

Initial full parity audit:

- `backend/.venv/bin/python -m backend.scripts.neon_parity_audit --db-path backend/runtime/data.db --allow-mismatch --json`

Initial mismatch:

- `projected_instrument_loadings`: local `14760`, Neon `1530`
- `projected_instrument_meta`: local `328`, Neon `34`

Review conclusion:

- this was a narrow publication/mirroring gap in `projected_instrument_*`, not a `serve-refresh` or core-rebuild issue

Targeted sync run:

- `backend/.venv/bin/python -m backend.scripts.neon_sync_from_sqlite --db-path backend/runtime/data.db --tables projected_instrument_loadings,projected_instrument_meta --mode full --json`

Sync result:

- `projected_instrument_loadings`: Neon `1530 -> 14760`
- `projected_instrument_meta`: Neon `34 -> 328`

Post-sync focused parity:

- `backend/.venv/bin/python -m backend.scripts.neon_parity_audit --db-path backend/runtime/data.db --tables projected_instrument_loadings,projected_instrument_meta,serving_payload_current --allow-mismatch --json`

Post-sync full parity:

- `backend/.venv/bin/python -m backend.scripts.neon_parity_audit --db-path backend/runtime/data.db --allow-mismatch --json`

Result:

- full canonical parity returned `status=ok`

## Remaining Blockers Outside This Execution

- Cloud Run deploy was not executable because the shell still lacked:
  - `gcloud` auth
  - an active `gcloud` project
  - control/app base URLs
  - operator token
- the active migration plan file was intentionally not updated in this slice because it was already being edited concurrently
- the large `data/reference/security_master_seed.csv` CRLF/trailing-whitespace diff remains a separate hygiene problem and was intentionally not mixed into this slice because it already carried unrelated content changes
