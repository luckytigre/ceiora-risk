# AGENTS.md

This file is a short repo-local guardrail, not a second documentation system.

Read the canonical docs first:
- [docs/README.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/README.md)
- [ARCHITECTURE_AND_OPERATING_MODEL.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md)
- [OPERATIONS_PLAYBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/operations/OPERATIONS_PLAYBOOK.md)
- [CLOUD_NATIVE_RUNBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/operations/CLOUD_NATIVE_RUNBOOK.md)
- [CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md)
- [CPAR_OPERATIONS_PLAYBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/operations/CPAR_OPERATIONS_PLAYBOOK.md)

## Current Project Shape

This repo hosts two model families:
- `cUSE4`
  - incumbent/default model family
  - core logic in `backend/risk_model/*`
  - default app-facing routes and pages still largely map to cUSE
- `cPAR`
  - explicitly parallel model family
  - pure model logic in `backend/cpar/*`
  - integration surfaces stay explicitly namespaced

Normal integration layers:
- `backend/api/*`: thin transport
- `backend/services/*`: application-facing assembly and mutations
- `backend/orchestration/*`: staged workflows
- `backend/data/*`: persistence and authority adapters
- `frontend/src/features/cuse4/*`: cUSE-owned UI
- `frontend/src/features/cpar/*`: cPAR-owned UI

## Runtime And Data Authority

- Neon is the operating source of truth for app/runtime reads when `DATA_BACKEND=neon`.
- Local SQLite remains:
  - the direct LSEG ingest landing zone
  - the deep archive
  - the mirror/repair surface
  - workspace scratch during rebuilds
- `cloud-serve` should be fail-closed and Neon-authoritative.
- `local-ingest` is the only runtime that should own broad ingest/publish/rebuild work.

Do not introduce new logic that silently falls back from Neon to local SQLite in cloud-serving behavior unless the active docs explicitly permit it.

## Boundary Rules

- Keep routes thin.
- Let one service own each application-facing payload or mutation flow.
- Keep persistence and authority handling in `backend/data/*`.
- Keep long-running or staged job logic in `backend/orchestration/*`.
- Do not create vague catch-all modules like `shared.py`, `common.py`, or new god-manager files.

If an ownership exception is justified, update the active architecture docs in the same change.

## Documentation Rules

- Project docs belong under `docs/`.
- Root-level Markdown should stay minimal. `AGENTS.md` is allowed; planning notes and specs should not live at repo root.
- Completed trackers, one-time procedures, and historical investigations belong in `docs/archive/*`, not in the active architecture or reference surface.
- If behavior changes materially, update the relevant canonical docs instead of only adding a note somewhere else.

## Validation Minimums

Before commit, run the smallest meaningful validation for the touched surface.

Common minimums:
- `git diff --check`
- targeted backend tests for touched services/routes
- `cd frontend && npm run typecheck` for frontend changes

When runtime/ops contracts change, also check the relevant runbook/doc surface and any applicable smoke or repair path.
