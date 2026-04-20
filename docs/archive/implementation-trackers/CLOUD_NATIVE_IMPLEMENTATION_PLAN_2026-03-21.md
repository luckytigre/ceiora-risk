# Cloud-Native Implementation Plan

Date: 2026-03-21
Owner: Codex
Status: Active implementation tracker

## Update Discipline

- This file is the canonical tracker for the cloud-native prep workstream.
- Every implementation slice touching this workstream must update this file in the same change.
- Updates must either:
  - check off completed steps,
  - refine the wording of an in-progress step to match the approved implementation, or
  - add a brief dated note in `Progress Notes` when scope or sequencing changes.
- Do not leave completed work undocumented here.

## Objective

Prepare the app for a cloud-native runtime without deploying it yet.

Target outcome:
- a stateless serving app can run without owning refresh execution,
- control-plane refresh execution can run in a separate process/app surface,
- Neon remains the serving authority in cloud mode,
- frontend operator/control calls can target a separate backend origin,
- repo docs and deployment assets are ready for later cloud rollout.

Non-goals for this plan:
- no live cloud deployment,
- no infra-provider-specific rollout,
- no change to cPAR package/build ownership,
- no redesign of cUSE/cPAR frontend IA.

## Planned Slices

### Slice 1: Backend app-surface split

- [x] Freeze the router ownership matrix before refactoring:
  - serve-only: `portfolio`, `exposures`, `risk`, `holdings`, `universe`, `cpar`
  - control-only: `refresh`, `operator`, `health/diagnostics`, `data/diagnostics`
  - both: top-level `/api/health` process health endpoint only
- [x] Add explicit router bundles for:
  - full local app
  - serve app
  - control app
- [x] Add a reusable FastAPI app factory so the repo has clear entrypoints for:
  - `backend.main:app` (legacy/full local app)
  - `backend.serve_main:app` (cloud/stateless serving app)
  - `backend.control_main:app` (control-plane app)
- [x] Ensure the serve app excludes refresh execution routes.
- [x] Ensure the control app includes refresh routes and operator/control diagnostics routes.

### Slice 2: Refresh/control ownership cleanup

- [x] Freeze stable ownership before code changes:
  - `backend/services/refresh_manager.py` remains the control-plane execution owner for process-local refresh lifecycle
  - a new read-only refresh-status service owns persisted status reads for serve-facing surfaces
  - a small refresh-dispatch helper owns runtime-aware “request serve-refresh” behavior for holdings/editor flows
- [x] Inventory and cut over existing import seams that currently assume one in-process app:
  - readiness payload assembly
  - operator status assembly
  - holdings-triggered refresh dispatch
- [x] Split persisted refresh-status reads from process-local refresh execution ownership.
- [x] Stop serve-facing read helpers from importing refresh-manager behavior that can reconcile or mutate shared runtime state incorrectly in a separate process.
- [x] Make holdings-triggered refresh dispatch runtime-aware:
  - `local-ingest`: keep current inline/in-process trigger behavior
  - `cloud-serve`: do not start local refresh execution; return explicit control-plane-required metadata instead
- [x] Keep runtime/operator state Neon-authoritative in cloud mode.

### Slice 3: Frontend control-plane routing prep

- [x] Add a separate control-plane backend origin for frontend proxy routes.
- [x] Add the explicit helper contract in `frontend/src/app/api/_backend.ts`:
  - `backendOrigin()` for serve/public backend calls
  - `controlBackendOrigin()` (or equivalent explicit name) that falls back to `BACKEND_API_ORIGIN` when no separate control origin is configured
- [x] Keep control-origin selection inside the frontend API proxy/helper layer; do not leak origin selection into pages/components.
- [x] Route operator/control frontend API proxies through that control origin:
  - `/api/refresh`
  - `/api/refresh/status`
  - `/api/operator/status`
  - `/api/health/diagnostics`
  - `/api/data/diagnostics`
- [x] Apply the split minimally by updating the existing App Router proxy owners only:
  - `frontend/src/app/api/refresh/route.ts`
  - `frontend/src/app/api/refresh/status/route.ts`
  - `frontend/src/app/api/operator/status/route.ts`
  - `frontend/src/app/api/health/diagnostics/route.ts`
  - `frontend/src/app/api/data/diagnostics/route.ts`
- [x] Preserve existing same-origin behavior when no separate control origin is configured.

### Slice 4: Deployment-prep assets and docs

- [x] Add durable container/deployment prep assets:
  - backend serve Dockerfile
  - backend control Dockerfile
  - frontend Dockerfile
  - repo `.dockerignore`
- [x] Add a cloud-native runbook describing:
  - process split
  - environment variables
  - expected runtime roles
  - what remains intentionally out of scope until actual deployment
- [x] Update the canonical docs that define runtime/process ownership:
  - `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
  - `docs/architecture/architecture-invariants.md`
  - `docs/architecture/dependency-rules.md`
  - `docs/architecture/maintainer-guide.md` if operator/runtime maintenance workflow changes materially
  - `docs/operations/OPERATIONS_PLAYBOOK.md`

### Slice 5: Validation and pre-commit gate

- [x] Add/extend tests for:
  - router bundles / app surfaces
  - refresh status read-vs-execution ownership
  - cloud-mode holdings refresh dispatch behavior
  - frontend control-origin proxy behavior where practical
- [x] Run targeted backend/frontend validation plus full build/typecheck paths.
- [x] Run multi-agent pre-commit review before final commit.

## Validation Matrix

Planned validation before commit:
- backend route/app-surface tests
- backend refresh/runtime-state tests
- backend cloud-runtime-role tests
- `cd frontend && npm run typecheck`
- `cd frontend && npm run build`
- targeted frontend smoke coverage for operator/control proxies if affected
- `git diff --check`
- `git diff --cached --check`

## Progress Notes

- 2026-03-21: Initial plan drafted from current repo seams. Key blocker identified before implementation: `refresh_manager.get_refresh_status()` currently performs process-local orphan reconciliation, which would be unsafe if the serving app and control-plane app run as separate processes against shared runtime state.
- 2026-03-21: Round-1 review tightened the plan to name stable module owners, freeze the route matrix before refactoring, keep control-origin logic in the frontend proxy layer, and schedule explicit invariant/rule doc updates instead of treating them as optional follow-up.
- 2026-03-21: Round-2 review added the explicit frontend proxy ownership list and required the split-origin fallback contract to be defined in `frontend/src/app/api/_backend.ts` instead of left implicit.
- 2026-03-21: Implementation complete. Validation passed on the backend surface/runtime matrix, frontend `typecheck`, frontend production `build`, and route smokes. Container builds were not executed here because Docker is not installed in this environment.
- 2026-03-21: Final multi-agent pre-commit review cleared after tightening `/api/health` into a service-owned payload, adding explicit frontend control-origin contract coverage, and pinning the control-plane Make target to one worker.
