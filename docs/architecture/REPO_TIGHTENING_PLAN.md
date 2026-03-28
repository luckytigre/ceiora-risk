# Repo Tightening Plan

Date: 2026-03-28
Status: Active remediation plan
Owner: Codex

## Purpose

This plan turns the current codebase audit into an ordered cleanup program.

It maps the confirmed cleanup work that still matters after the registry-first
migration and defines an execution order that reduces residue without
destabilizing the active Neon-authoritative operating model.

## Confirmed Cleanup Targets

The current repository still has five meaningful cleanup buckets:

1. Frontend ownership drift between canonical `/cuse/*` pages and legacy root routes
2. Transitional frontend compatibility barrels used outside true compatibility seams
3. Duplicated cUSE4 service surfaces where both the default module and the
   explicit `cuse4_*` alias carry the same implementation
4. Stale active docs that still describe `security_master` as the canonical
   universe authority after the registry-first cutover
5. Large mixed-state runtime/data modules that still combine registry, compat,
   fallback, and payload assembly concerns in the same files

## Phase Order

### Phase 1: Route And Compatibility Cleanup

Goal:
- keep `/cuse/*` as the only cUSE4 implementation route family
- keep `/exposures`, `/explore`, and `/health` as redirect-only legacy entrypoints
- tighten cUSE4-owned imports away from the mixed-family frontend barrels

Implementation surface:
- `frontend/next.config.js`
- `frontend/scripts/family_redirect_contract_check.mjs`
- `frontend/scripts/family_routes_smoke.mjs`

Validation:
- `cd frontend && node scripts/family_redirect_contract_check.mjs`
- `cd frontend && node scripts/family_routes_smoke.mjs`

### Phase 2: cUSE4 Service De-dup

Goal:
- keep explicit `cuse4_*` imports as the route-facing owner surface
- stop carrying duplicate implementations in both the legacy and alias modules

Implementation surface:
- `backend/services/cuse4_universe_service.py`
- `backend/services/universe_service.py`
- `backend/services/factor_history_service.py`
- `backend/services/health_diagnostics_service.py`

Validation:
- targeted route/service tests for universe, factor-history, and health reads
- `git diff --check`

### Phase 3: Active Doc Realignment

Goal:
- align active docs with the current registry-first authority model
- stop documenting legacy root routes as implementation homes

Implementation surface:
- `docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`
- `docs/reference/specs/cUSE4_engine_spec.md`
- `docs/architecture/CPAR_ORCHESTRATION.md`
- `docs/README.md`

Validation:
- doc links and route statements checked against current code
- no new active-doc contradictions with the runbooks or registry plan

### Phase 4: Mixed-State Read-Layer Split

Goal:
- split large mixed-state modules by responsibility rather than by historical convenience

Priority files:
- `backend/universe/runtime_rows.py`
- `backend/data/source_reads.py`
- `backend/data/serving_outputs.py`

Required outcome:
- registry/taxonomy/policy/source-observation assembly separated from compatibility fallbacks
- cloud-serving fallback policy explicit and narrow
- selector logic owned in one place

This phase is intentionally separate from phases 1-3 because it touches live
runtime semantics and carries materially higher regression risk.

### Phase 5: Large-Module Decomposition

Goal:
- reduce maintenance concentration in oversized orchestration/persistence files

Priority files:
- `backend/services/neon_stage2.py`
- `backend/services/neon_mirror.py`
- `backend/analytics/pipeline.py`
- `backend/services/cpar_portfolio_snapshot_service.py`

Decomposition rule:
- split by job or contract, not by arbitrary helper dumping
- do not create new vague shared utility files

### Phase 6: Test Harness Hardening

Goal:
- reduce monkeypatch-heavy test coupling so structural cleanup stops being punished

Priority surfaces:
- refresh/orchestration tests
- service tests that patch deep module globals instead of injecting narrow loaders

Required outcome:
- route/service tests focus on public behavior
- orchestration tests isolate boundaries through explicit seams, not broad monkeypatch fan-out

## Current Slice

The current implementation batch covers the route contract hardening from phase 1 and the active doc realignment from phase 3.
Phase 2 remains deferred.

That is deliberate:
- the repo is already in a dirty migration state
- the registry-first cutover should not be reopened by a broad cleanup sweep
- this batch removes real residue, tightens docs, and improves ownership clarity
  without changing the fundamental authority model
- alias-wrapper cleanup still collides with monkeypatch-heavy backend test seams
  and should be handled in a separate, cleaner slice

## Adversarial Review Round 1

Rejected ideas:
- remove the cUSE4 alias modules outright and point routes back to the default modules
- add app-router redirect pages on top of the existing `next.config.js` redirects
- mix fail-closed behavior changes into the same cleanup

Why they were rejected:
- route and snapshot tests still patch alias-module globals directly, so deleting or flattening those modules would break the current test seam
- a second redirect layer would duplicate route ownership and risk query-string drift
- fallback-behavior changes are runtime-contract work, not low-risk ownership cleanup

Revision:
- keep explicit `cuse4_*` route imports
- keep legacy route redirects owned in `frontend/next.config.js`
- harden that redirect contract with runtime smoke coverage and a static config check

## Adversarial Review Round 2

Implementation findings:
- the single-source redirect contract is cleaner than adding duplicate app-router pages for the same legacy routes
- runtime smoke now covers query-string preservation for `/exposures`, `/explore`, and `/health`
- the static redirect contract now verifies that those legacy routes stay defined in `frontend/next.config.js` and are not reintroduced as duplicate app-router pages
- the active cUSE/cPAR docs now describe the registry-first authority model instead of the pre-cutover `security_master` contract
- cUSE alias-wrapper cleanup is still worth doing, but it remains out of scope for this batch because the current test seam and dirty worktree make it too easy to overreach

Validation results for this slice:
- `git diff --check` passed for the touched files
- `cd frontend && node scripts/family_redirect_contract_check.mjs` passed
- `node --check frontend/scripts/family_routes_smoke.mjs` passed
- targeted backend `pytest` and frontend `tsc` invocations were attempted, but both stalled behind existing long-running workspace processes in this environment
