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
- `git diff --check -- <touched paths>`

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

## Program Expansion: Full Cleanup Execution

Date: 2026-03-28
Status: Finalized after two adversarial review rounds
Owner: Codex

### Intent

This section expands the tightening plan from a partial cleanup tracker into a
full execution program for the remaining residue:

- transitional frontend compatibility drift
- duplicated cUSE4 alias-wrapper service surfaces
- legacy `security_master` compatibility leakage inside active cUSE flows
- mixed-state runtime/data readers that still combine authority and fallback concerns
- oversized orchestration and persistence modules that now carry too much policy

The execution order is designed to reduce maintenance concentration without
reopening the Neon-authoritative operating contract during active Gate G closeout.

### Non-Negotiable Guardrails

- Do not mix operational Gate G closeout work and structural cleanup in the same commit.
- Use one rollback-safe cleanup slice per commit.
- Do not change cloud fail-closed behavior as incidental fallout of an ownership cleanup slice.
- Every slice must update canonical docs in the same change when ownership,
  route semantics, runtime authority wording, or operator expectations change.
- Every slice must end with a path-scoped `git diff --check -- <touched paths>`.
- Frontend-touching slices must also run `cd frontend && npm run typecheck`.
- Runtime-contract slices must also run `make doctor` unless the environment is
  already known to be blocked; if blocked, record the blocker in the execution note.

### Review Protocol

This cleanup program requires two review loops at two levels:

1. Program-level review
   - round 1: adversarial review of the draft slice map, ordering, and validation
   - round 2: adversarial review of the revised plan after round-1 fixes
2. Slice-level review before execution
   - study the exact files, route owners, tests, and doc surfaces for the slice
   - run at least two focused sub-agent reviews against the slice plan before code edits
   - after implementation, run one more focused review before commit if the slice touches runtime authority, serving behavior, or orchestration

No structural slice should be executed or committed without finishing the
slice-specific study note and the pre-edit adversarial review loop.

### Execution Rules

- Keep code execution serial even when study and review are parallelized.
- Prefer parallel study only for non-overlapping analysis tasks.
- Treat the dirty worktree as hostile context: do not absorb unrelated files into
  a cleanup slice.
- If a slice cannot be validated narrowly, split it again before implementation.
- Prefer focused `pytest` file lists or node ids over monolithic contract files
  when those files are already known to stall in this environment.
- If the only relevant regression lives inside a too-large test file, extract a
  smaller focused regression test in the same slice before relying on it as a
  pre-commit gate.

### Required Slice Template

Before coding any slice, create a short slice note in the working thread or in a
doc update draft that includes all of:

- exact files in scope
- explicit out-of-scope files
- ownership target after the slice
- doc files that must be updated in the same change
- targeted validation commands
- rollback boundary
- commit message shape

If a slice still contains more than one independent rollback boundary after the
study pass, split it again before code edits begin.

### Executable Slice Map

#### Slice 0: Program Stabilization And Hygiene Baseline

Goal:
- establish a clean execution protocol before structural changes begin

Study first:
- confirm the current Gate G operational closeout boundary
- confirm which existing worktree files are unrelated and must stay untouched
- confirm the current smoke/test commands still run in this environment

Primary surfaces:
- `docs/README.md`
- `docs/architecture/REPO_TIGHTENING_PLAN.md`
- `docs/architecture/maintainer-guide.md`
- `docs/architecture/dependency-rules.md`
- `.gitignore`
- root-level stray artifacts and runtime-output hygiene rules

Required doc updates:
- execution protocol additions in active architecture docs
- cleanup-note placement guidance in `docs/README.md`
- explicit note that cleanup commits remain path-scoped while the repo is in a dirty migration state

Validation:
- `git diff --check -- <touched paths>`

Commit boundary:
- docs and hygiene only

Execution note:
- repo-hygiene ignore rules must be root-anchored and concrete; do not add broad ignore patterns that can hide legitimate in-repo artifacts

#### Slice 1: Legacy Route Ownership And Redirect Contract

Goal:
- keep `/cuse/*` as the only cUSE implementation route family
- keep `/exposures`, `/explore`, and `/health` redirect-only
- remove duplicate redirect ownership in app-router files

Study first:
- compare `frontend/next.config.js` against any app-router redirect pages
- confirm query-string preservation and current smoke coverage
- if duplicate root App Router redirect pages exist only as untracked local files, remove them as workspace hygiene before running validation; the tracked redirect owner remains `frontend/next.config.js`

Primary surfaces:
- `frontend/next.config.js`
- `frontend/src/app/explore/page.tsx`
- `frontend/src/app/exposures/page.tsx`
- `frontend/src/app/health/page.tsx`
- `frontend/scripts/family_redirect_contract_check.mjs`
- `frontend/scripts/family_routes_smoke.mjs`

Required doc updates:
- `docs/README.md` if route-entry wording changes
- `docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`
- `docs/architecture/CPAR_FRONTEND_SURFACES.md`
- `docs/operations/OPERATIONS_HARDENING_CHECKLIST.md`
- this plan file

Validation:
- `git diff --check -- <touched paths>`
- `cd frontend && node scripts/family_redirect_contract_check.mjs`
- `cd frontend && node scripts/family_routes_smoke.mjs`
- `cd frontend && npm run typecheck`

Commit boundary:
- redirect ownership and route-contract hardening only

#### Slice 2: Frontend Family API Barrel Split

Goal:
- stop using mixed-family compatibility barrels from active cUSE/cPAR pages

Study first:
- enumerate all imports of `@/lib/api` and `@/hooks/useApi`
- map current cUSE-only, cPAR-only, and intentionally shared consumers

Primary surfaces:
- `frontend/src/lib/api.ts`
- `frontend/src/lib/cuse4Api.ts`
- `frontend/src/lib/cparApi.ts`
- `frontend/src/hooks/useApi.ts`
- `frontend/src/hooks/useCuse4Api.ts`
- `frontend/src/hooks/useCparApi.ts`

Required doc updates:
- `docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`
- `docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/maintainer-guide.md`
- `docs/architecture/CPAR_FRONTEND_SURFACES.md`
- `docs/operations/CPAR_OPERATIONS_PLAYBOOK.md`

Validation:
- `git diff --check -- <touched paths>`
- `cd frontend && npm run typecheck`
- `cd frontend && npm run test:family-routes`
- `cd frontend && npm run test:cpar-pages`
- `cd frontend && npm run test:cpar-hedge`
- `cd frontend && npm run test:cpar-portfolio`
- `cd frontend && npm run test:cpar-portfolio-whatif`
- `cd frontend && npm run test:explore-whatif`
- `cd frontend && npm run test:explore-whatif-busy`
- `cd frontend && npm run test:control-plane-proxies`

Commit boundary:
- frontend import ownership only

Execution note:
- do not widen this slice to settle `/positions`
- the only allowed root-owner outcome is `frontend/next.config.js`; duplicate root App Router pages should remain absent

#### Slice 3: Shared Holdings Surface Decision For `/positions`

Goal:
- decide whether `/positions` remains an intentional shared cUSE/cPAR holdings surface or is split back into family-owned surfaces

Study first:
- inspect `/positions` and holdings components for true cross-family product intent
- confirm whether the current dual-method presentation is product truth or transition residue
- if no positions-focused smoke exists, add one before landing a shared-surface change

Primary surfaces:
- `frontend/src/app/positions/page.tsx`
- `frontend/src/features/holdings/components/HoldingsLedgerSection.tsx`
- `frontend/scripts/positions_surface_smoke.mjs`
- related holdings feature helpers if ownership changes

Required doc updates:
- `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`
- `docs/architecture/dependency-rules.md`
- `docs/architecture/maintainer-guide.md`
- `docs/architecture/CPAR_FRONTEND_SURFACES.md` if the page remains shared
- `docs/operations/OPERATIONS_HARDENING_CHECKLIST.md`
- `docs/operations/CPAR_OPERATIONS_PLAYBOOK.md` if cPAR participation in `/positions` remains intentional

Validation:
- `git diff --check -- <touched paths>`
- `cd frontend && node scripts/positions_surface_smoke.mjs`
- `cd frontend && npm run typecheck`
- `cd frontend && npm run test:cpar-portfolio`
- `cd frontend && npm run test:cpar-portfolio-whatif`
- `cd frontend && npm run test:cpar-hedge`

Commit boundary:
- `/positions` ownership decision only

#### Slice 4: Test Seam Hardening For cUSE4 De-Dup Part A

Goal:
- reduce monkeypatch-heavy coupling that blocks cUSE alias-wrapper cleanup for dashboard, factor history, and health

Study first:
- inventory route and service tests that patch alias-module globals directly
- identify stable public seams that can replace deep monkeypatch fan-out

Execution note:
- prefer route-level callable seams or public injected service kwargs over monkeypatching alias-module globals directly

Primary surfaces:
- `backend/tests/test_dashboard_payload_service.py`
- `backend/tests/test_exposure_history_route.py`
- `backend/tests/test_health_diagnostics.py`
- `backend/tests/test_health_diagnostics_scoping.py`
- `backend/tests/test_serving_output_route_preference.py`
- any helper seams introduced to support narrower injection

Required doc updates:
- this plan file
- `docs/architecture/maintainer-guide.md` if new stable test seams become part of maintainer guidance

Validation:
- `git diff --check -- <touched paths>`
- `./.venv_local/bin/pytest -q backend/tests/test_dashboard_payload_service.py backend/tests/test_exposure_history_route.py backend/tests/test_health_diagnostics.py backend/tests/test_health_diagnostics_scoping.py backend/tests/test_serving_output_route_preference.py`

Commit boundary:
- tests and narrow seam extraction for dashboard/factor-history/health only

#### Slice 5: cUSE4 Service Surface De-Dup Part A

Goal:
- collapse duplicate implementations across cUSE4 alias wrappers and legacy modules for dashboard, factor history, and health

Study first:
- verify which `cuse4_*` module should remain route-facing
- map current imports and monkeypatch seams after slice 4

Primary surfaces:
- `backend/services/cuse4_dashboard_payload_service.py`
- `backend/services/dashboard_payload_service.py`
- `backend/services/cuse4_factor_history_service.py`
- `backend/services/factor_history_service.py`
- `backend/services/cuse4_health_diagnostics_service.py`
- `backend/services/health_diagnostics_service.py`

Required doc updates:
- `docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`
- `docs/architecture/maintainer-guide.md`

Validation:
- `git diff --check -- <touched paths>`
- `./.venv_local/bin/pytest -q backend/tests/test_dashboard_payload_service.py backend/tests/test_exposure_history_route.py backend/tests/test_health_diagnostics.py backend/tests/test_health_diagnostics_scoping.py backend/tests/test_serving_output_route_preference.py backend/tests/test_architecture_boundaries.py backend/tests/test_model_family_ownership_boundaries.py`

Commit boundary:
- dashboard/factor-history/health only

#### Slice 6: Test Seam Hardening For cUSE4 De-Dup Part B

Goal:
- reduce monkeypatch-heavy coupling that blocks cUSE alias-wrapper cleanup for operator status

Study first:
- inventory route tests that still patch the legacy operator-status compatibility module instead of the cUSE4 route-facing owner
- identify the narrowest seam that keeps auth tests route-scoped while moving service-heavy tests onto the cUSE4 owner surface

Primary surfaces:
- `backend/tests/test_operator_status_route.py`
- `backend/tests/test_cloud_auth_and_runtime_roles.py`
- `backend/api/routes/operator.py`
- `backend/services/cuse4_operator_status_service.py`

Required doc updates:
- this plan file
- `docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

Validation:
- `git diff --check -- backend/api/routes/operator.py backend/services/cuse4_operator_status_service.py backend/tests/test_operator_status_route.py backend/tests/test_cloud_auth_and_runtime_roles.py docs/architecture/REPO_TIGHTENING_PLAN.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_operator_status_route.py backend/tests/test_cloud_auth_and_runtime_roles.py -k operator_status`

Commit boundary:
- operator-status seam hardening only

#### Slice 6B: Service Injection Hardening For Holdings

Goal:
- replace legacy module-global monkeypatching with public injected service dependencies for holdings mutations
- make the later holdings owner move safe without forcing service tests to patch private globals on the legacy module

Study first:
- confirm which holdings tests still patch legacy globals directly
- expose the smallest public dependency surface that preserves current behavior while making tests owner-agnostic

Primary surfaces:
- `backend/services/holdings_service.py`
- `backend/tests/test_holdings_service.py`

Required doc updates:
- this plan file
- `docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

Validation:
- `git diff --check -- backend/services/holdings_service.py backend/tests/test_holdings_service.py docs/architecture/REPO_TIGHTENING_PLAN.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_holdings_service.py backend/tests/test_holdings_route_dirty_state.py`

Commit boundary:
- holdings seam hardening only

#### Slice 7A: cUSE4 Holdings Owner Move

Goal:
- make `backend/services/cuse4_holdings_service.py` the concrete holdings owner
- reduce `backend/services/holdings_service.py` to a full compatibility shim without dropping any public holdings symbols

Study first:
- verify the full public holdings contract that older callers and tests still depend on
- confirm holdings consumers only rely on the public service surface, not legacy private globals

Primary surfaces:
- `backend/services/cuse4_holdings_service.py`
- `backend/services/holdings_service.py`
- `docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`
- `docs/architecture/maintainer-guide.md`
- this plan file
- `docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

Validation:
- `git diff --check -- backend/services/cuse4_holdings_service.py backend/services/holdings_service.py docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md docs/architecture/maintainer-guide.md docs/architecture/REPO_TIGHTENING_PLAN.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_holdings_service.py backend/tests/test_holdings_route_dirty_state.py backend/tests/test_portfolio_whatif_route.py::test_portfolio_whatif_apply_route_returns_service_payload backend/tests/test_model_family_ownership_boundaries.py`

Commit boundary:
- holdings owner move only

#### Slice 7B: cUSE4 Portfolio What-If Owner Move

Goal:
- make `backend/services/cuse4_portfolio_whatif.py` the concrete portfolio what-if owner
- reduce `backend/services/portfolio_whatif.py` to a compatibility shim without dropping any public preview symbols

Study first:
- verify the full public portfolio what-if contract that older callers and tests still depend on
- confirm route and service consumers only rely on the public preview surface plus injected dependency contract
- document that the shim keeps only the supported public import surface rather than broad legacy monkeypatch parity

Primary surfaces:
- `backend/services/cuse4_portfolio_whatif.py`
- `backend/services/portfolio_whatif.py`
- `backend/tests/test_portfolio_whatif_service.py`
- `docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`
- `docs/architecture/maintainer-guide.md`
- this plan file
- `docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

Validation:
- `git diff --check -- backend/services/cuse4_portfolio_whatif.py backend/services/portfolio_whatif.py backend/tests/test_portfolio_whatif_service.py docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md docs/architecture/maintainer-guide.md docs/architecture/REPO_TIGHTENING_PLAN.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_portfolio_whatif_service.py backend/tests/test_portfolio_whatif_route.py backend/tests/test_model_family_ownership_boundaries.py`

Commit boundary:
- portfolio what-if owner move only

#### Slice 7: cUSE4 Service Surface De-Dup Part B

Goal:
- finish alias-wrapper de-dup for holdings, operator status, universe, and portfolio what-if

Study first:
- verify route imports and service owner targets
- verify no residual direct imports depend on duplicate implementations

Primary surfaces:
- `backend/services/cuse4_holdings_service.py`
- `backend/services/holdings_service.py`
- `backend/services/cuse4_operator_status_service.py`
- `backend/services/operator_status_service.py`
- `backend/services/cuse4_universe_service.py`
- `backend/services/universe_service.py`
- `backend/services/cuse4_portfolio_whatif.py`
- `backend/services/portfolio_whatif.py`

Required doc updates:
- `docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`
- `docs/architecture/maintainer-guide.md`

Validation:
- `git diff --check -- <touched paths>`
- `./.venv_local/bin/pytest -q backend/tests/test_holdings_service.py backend/tests/test_holdings_route_dirty_state.py backend/tests/test_operator_status_route.py backend/tests/test_universe_search_route.py backend/tests/test_universe_history_route.py backend/tests/test_universe_loadings_service.py backend/tests/test_portfolio_whatif_service.py backend/tests/test_portfolio_whatif_route.py backend/tests/test_architecture_boundaries.py backend/tests/test_model_family_ownership_boundaries.py`

Commit boundary:
- holdings/operator/universe/portfolio-whatif only

#### Slice 8: Security-Master Authority Wording And Doc Containment

Goal:
- make `security_master` explicitly compatibility-only in active docs before code containment begins
- stop active runbooks from implying it is canonical authority

Study first:
- search active docs for `security_master` authority wording
- separate true compatibility references from stale primary-authority language

Primary surfaces:
- `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/architecture-invariants.md`
- `docs/architecture/GCP_CLOUD_RUN_TERRAFORM_PLAN.md`
- `docs/reference/specs/cUSE4_engine_spec.md`
- `docs/operations/CLOUD_NATIVE_RUNBOOK.md`
- `docs/operations/OPERATIONS_PLAYBOOK.md`

Required doc updates:
- any remaining active docs that still describe `security_master` as primary authority

Validation:
- `git diff --check -- <touched paths>`

Commit boundary:
- docs only

#### Slice 9: cPAR Snapshot Service Decomposition Part A

Goal:
- split aggregate risk assembly away from broader snapshot orchestration
- keep truly shared lower math only where it is stable and family-owned

Study first:
- map aggregate risk helper reuse between snapshot and risk routes
- identify the smallest helper set that can move without dragging hedge/what-if logic with it

Primary surfaces:
- `backend/services/cpar_portfolio_snapshot_service.py`
- `backend/services/cpar_risk_service.py`
- any extracted aggregate-risk helper modules

Required doc updates:
- `docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/CPAR_BACKEND_READ_SURFACES.md`
- `docs/operations/CPAR_OPERATIONS_PLAYBOOK.md`
- `docs/architecture/dependency-rules.md`
- `docs/architecture/maintainer-guide.md`

Validation:
- `git diff --check -- <touched paths>`
- `./.venv_local/bin/pytest -q backend/tests/test_cpar_portfolio_snapshot_service.py backend/tests/test_cpar_risk_service.py backend/tests/test_cpar_runtime_coverage_contract.py backend/tests/test_cpar_architecture_boundaries.py backend/tests/test_cpar_service_route_boundaries.py backend/tests/test_cpar_routes.py::test_cpar_risk_route_returns_payload backend/tests/test_cpar_routes.py::test_cpar_risk_route_maps_not_ready_to_503`
- `make doctor`

Commit boundary:
- cPAR aggregate-risk extraction only

#### Slice 10: cPAR Snapshot Service Decomposition Part B

Goal:
- split account-scoped hedge, portfolio what-if, and explore what-if assembly away from aggregate snapshot ownership
- leave shared lower helpers only where reuse is real and stable

Study first:
- map hedge/what-if/explore dependencies on snapshot helpers
- confirm which account-scoped helpers should live below the service layer and which should remain route-owned assembly

Primary surfaces:
- `backend/services/cpar_portfolio_snapshot_service.py`
- `backend/services/cpar_portfolio_hedge_service.py`
- `backend/services/cpar_portfolio_whatif_service.py`
- `backend/services/cpar_explore_whatif_service.py`
- any extracted account-scoped helper modules

Required doc updates:
- `docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/CPAR_BACKEND_READ_SURFACES.md`
- `docs/architecture/CPAR_FRONTEND_SURFACES.md` if route freshness or response semantics change
- `docs/operations/CPAR_OPERATIONS_PLAYBOOK.md`
- `docs/architecture/dependency-rules.md`
- `docs/architecture/maintainer-guide.md`
- `docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`

Validation:
- `git diff --check -- <touched paths>`
- `./.venv_local/bin/pytest -q backend/tests/test_cpar_portfolio_snapshot_service.py backend/tests/test_cpar_portfolio_hedge_service.py backend/tests/test_cpar_portfolio_whatif_service.py backend/tests/test_cpar_explore_whatif_service.py backend/tests/test_cpar_runtime_coverage_contract.py backend/tests/test_cpar_architecture_boundaries.py backend/tests/test_cpar_service_route_boundaries.py backend/tests/test_cpar_routes.py::test_cpar_portfolio_hedge_route_returns_payload backend/tests/test_cpar_routes.py::test_cpar_portfolio_whatif_route_returns_payload backend/tests/test_cpar_routes.py::test_cpar_explore_whatif_route_returns_payload`
- `make doctor`

Commit boundary:
- cPAR account-scoped hedge/what-if/explore extraction only

#### Slice 11: Mixed-State Read-Layer Split Part A

Goal:
- separate registry, taxonomy, policy, and source-observation assembly from compatibility fallback inside runtime rows

Study first:
- map `runtime_rows.py` call sites
- isolate which behavior is current-state authority, which is historical lookup, and which is compatibility fallback

Primary surfaces:
- `backend/universe/runtime_rows.py`
- any extracted narrow helper modules with concrete names

Required doc updates:
- `docs/architecture/dependency-rules.md`
- `docs/architecture/maintainer-guide.md`
- this plan file

Validation:
- `git diff --check -- <touched paths>`
- `./.venv_local/bin/pytest -q backend/tests/test_core_reads.py backend/tests/test_holdings_reads.py backend/tests/test_universe_selector_parity.py backend/tests/test_registry_first_diagnostics.py backend/tests/test_architecture_boundaries.py`
- `make doctor`

Commit boundary:
- runtime-row authority split only

#### Slice 12: Mixed-State Read-Layer Split Part B1

Goal:
- split `source_reads.py` authority assembly away from serving-payload and runtime-state fallback logic

Study first:
- map source-read consumers by payload type
- isolate source-date, observation, and archive-read responsibilities from serve-time fallback rules

Primary surfaces:
- `backend/data/source_reads.py`
- any extracted source-read helper modules

Required doc updates:
- `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/dependency-rules.md`
- `docs/architecture/maintainer-guide.md`
- `docs/operations/OPERATIONS_PLAYBOOK.md`

Validation:
- `git diff --check -- <touched paths>`
- `./.venv_local/bin/pytest -q backend/tests/test_core_reads.py backend/tests/test_holdings_reads.py backend/tests/test_cpar_source_reads.py backend/tests/test_registry_first_diagnostics.py backend/tests/test_architecture_boundaries.py`
- `make doctor`

Commit boundary:
- source-read authority split only

#### Slice 13: Mixed-State Read-Layer Split Part B2

Goal:
- split `serving_outputs.py` and paired runtime-state authority helpers by responsibility
- make cloud-serving fallback policy explicit and narrow

Study first:
- map serving-payload readers and writers by authority mode and fallback rules

Primary surfaces:
- `backend/data/serving_outputs.py`
- `backend/data/runtime_state.py`
- any extracted narrow helper modules

Required doc updates:
- `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/architecture-invariants.md`
- `docs/architecture/dependency-rules.md`
- `docs/operations/CLOUD_NATIVE_RUNBOOK.md`
- `docs/operations/OPERATIONS_PLAYBOOK.md`

Validation:
- `git diff --check -- <touched paths>`
- `./.venv_local/bin/pytest -q backend/tests/test_serving_outputs.py backend/tests/test_runtime_state.py backend/tests/test_serving_output_route_fallbacks.py backend/tests/test_serving_output_route_preference.py backend/tests/test_cloud_bootstrap_proof.py backend/tests/test_cloud_auth_and_runtime_roles.py backend/tests/test_architecture_boundaries.py`
- `make doctor`

Commit boundary:
- serving-output and runtime-state authority split only

#### Slice 14: Security-Master Code Containment

Goal:
- keep `security_master` code paths compatibility-only after the read-layer split
- isolate projection-only selector behavior away from canonical authority naming

Study first:
- map every active import of `security_master_sync`
- separate true compatibility behavior from still-live authority behavior after slices 11-13 land
- if projection-selector rewiring needs `pipeline.py`, defer that rewiring into slice 16 instead of widening this slice

Primary surfaces:
- `backend/universe/security_master_sync.py`
- relevant selector and migration helpers

Required doc updates:
- `docs/reference/specs/cUSE4_engine_spec.md`
- `docs/architecture/architecture-invariants.md`
- `docs/operations/OPERATIONS_PLAYBOOK.md`
- `docs/architecture/maintainer-guide.md`

Validation:
- `git diff --check -- <touched paths>`
- `./.venv_local/bin/pytest -q backend/tests/test_security_master_lineage.py backend/tests/test_security_master_demotion.py backend/tests/test_projection_only_exclusion.py backend/tests/test_registry_first_diagnostics.py backend/tests/test_universe_migration_scaffolding.py backend/tests/test_architecture_boundaries.py`
- `make doctor`

Commit boundary:
- compatibility containment only

#### Slice 15: cUSE Serving Pipeline Decomposition Part A

Goal:
- extract publish, persist, and diagnostics sequencing from `run_refresh` without changing serving semantics

Study first:
- map publish-order responsibilities inside `run_refresh`
- identify which state transitions are orchestration concerns versus lower-owner concerns

Primary surfaces:
- `backend/analytics/pipeline.py`
- supporting publish and persist helpers

Required doc updates:
- `docs/architecture/maintainer-guide.md`
- `docs/operations/OPERATIONS_PLAYBOOK.md`

Validation:
- `git diff --check -- <touched paths>`
- `./.venv_local/bin/pytest -q backend/tests/test_operating_model_contract.py::test_run_refresh_publish_only_republishes_cached_payloads_without_recompute backend/tests/test_operating_model_contract.py::test_run_refresh_publishes_before_deep_health_diagnostics backend/tests/test_operating_model_contract.py::test_load_publishable_payloads_prefers_durable_serving_payloads backend/tests/test_operating_model_contract.py::test_run_model_pipeline_clears_pending_after_serving_refresh backend/tests/test_projection_only_serving_cadence.py::test_validate_projection_only_serving_outputs_raises_on_native_downgrade backend/tests/test_projection_only_serving_cadence.py::test_publish_only_refresh_fails_when_projection_only_ticker_is_downgraded`
- `make doctor`

Commit boundary:
- publish/persist/diagnostics sequencing only

#### Slice 16: cUSE Serving Pipeline Decomposition Part B

Goal:
- extract refresh context, source-date, core-state, and projection assembly from `run_refresh` without changing serving semantics

Study first:
- map which `run_refresh` logic belongs to orchestration versus projection/core read owners
- preserve current local-ingest versus cloud-serve authority behavior

Primary surfaces:
- `backend/analytics/pipeline.py`
- supporting refresh-context, projection, and source-state helpers

Required doc updates:
- `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/architecture-invariants.md`
- `docs/architecture/maintainer-guide.md`
- `docs/operations/OPERATIONS_PLAYBOOK.md`

Validation:
- `git diff --check -- <touched paths>`
- `./.venv_local/bin/pytest -q backend/tests/test_projection_only_serving_cadence.py::test_run_serving_stage_requests_projection_refresh_on_core_lane backend/tests/test_projection_only_serving_cadence.py::test_run_serving_stage_does_not_request_projection_refresh_on_serving_only_lane backend/tests/test_projection_only_serving_cadence.py::test_run_refresh_uses_persisted_projection_outputs_on_serving_rebuild backend/tests/test_projection_only_serving_cadence.py::test_run_refresh_recomputes_projection_outputs_when_persisted_asof_is_stale backend/tests/test_projection_only_serving_cadence.py::test_run_refresh_uses_canonical_projection_rows_when_workspace_has_none backend/tests/test_operating_model_contract.py::test_pipeline_prefers_fundamentals_asof backend/tests/test_operating_model_contract.py::test_light_refresh_can_fail_closed_when_stable_core_package_is_required backend/tests/test_operating_model_contract.py::test_run_stage_serving_refresh_uses_local_source_archive_for_local_publish_profiles backend/tests/test_operating_model_contract.py::test_run_stage_serving_refresh_keeps_neon_backend_for_canonical_serve_refresh backend/tests/test_operating_model_contract.py::test_run_stage_serving_refresh_uses_local_backend_during_core_rebuild backend/tests/test_operating_model_contract.py::test_run_stage_serving_refresh_passes_workspace_paths_without_mutating_core_reads`
- `make doctor`

Commit boundary:
- refresh context, source-state, core-state, and projection assembly only

#### Slice 17: Neon Source-Sync Contract Decomposition

Goal:
- split Neon schema, sync, and identifier-backfill responsibilities by job contract

Study first:
- map `sync_from_sqlite_to_neon` responsibilities
- preserve current fail-closed semantics, sync metadata behavior, and source-date rules

Primary surfaces:
- `backend/services/neon_stage2.py`
- supporting scripts that call those entrypoints

Required doc updates:
- `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/dependency-rules.md`
- `docs/architecture/maintainer-guide.md`
- `docs/operations/OPERATIONS_PLAYBOOK.md`

Validation:
- `git diff --check -- <touched paths>`
- `./.venv_local/bin/pytest -q backend/tests/test_neon_stage2_model_tables.py backend/tests/test_neon_authority.py backend/tests/test_refresh_profiles.py::test_source_sync_stage_pushes_source_tables_only backend/tests/test_refresh_profiles.py::test_source_sync_stage_fails_closed_when_source_dates_cannot_be_loaded backend/tests/test_refresh_profiles.py::test_source_sync_stage_requires_non_empty_local_source_dates backend/tests/test_refresh_profiles.py::test_source_sync_stage_refuses_to_downgrade_neon_sources backend/tests/test_refresh_profiles.py::test_source_sync_stage_refuses_newer_than_target_neon_dates backend/tests/test_refresh_profiles.py::test_neon_readiness_stage_prepares_workspace backend/tests/test_refresh_profiles.py::test_neon_readiness_stage_surfaces_workspace_preparation_failure`
- `make doctor`

Commit boundary:
- Neon source-sync decomposition only

Execution note:
- do not widen this slice into mirror, prune, or post-run publication cleanup

#### Slice 18: Neon Mirror And Post-Run Publication Decomposition

Goal:
- split mirror, prune, parity, and report orchestration away from lower sync logic

Study first:
- map `run_neon_mirror_cycle` responsibilities
- map how finalize and post-run publication consume mirror results
- preserve current artifact, report, and fail-closed behavior

Primary surfaces:
- `backend/services/neon_mirror.py`
- `backend/orchestration/finalize_run.py`
- `backend/orchestration/post_run_publish.py`
- `backend/tests/test_stage_execution.py`
- any mirror/report helper modules extracted from those owners

Required doc updates:
- `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/dependency-rules.md`
- `docs/architecture/maintainer-guide.md`
- `docs/operations/CLOUD_NATIVE_RUNBOOK.md`
- `docs/operations/OPERATIONS_PLAYBOOK.md`

Validation:
- `git diff --check -- <touched paths>`
- `./.venv_local/bin/pytest -q backend/tests/test_neon_mirror_integration.py backend/tests/test_post_run_publish.py backend/tests/test_stage_execution.py`
- `make doctor`

Commit boundary:
- Neon mirror, finalize, and post-run publication decomposition only

Execution note:
- parity ownership lives in this slice, not slice 17
- if the mirror result contract consumed by `finalize_run.py` or `post_run_publish.py` must change, split this into two commits:
  - `18A`: `neon_mirror.py` extraction with consumer contract unchanged
  - `18B`: consumer rewiring in `finalize_run.py` and `post_run_publish.py`

#### Slice 19: Final Doc Sweep And Acceptance

Goal:
- reconcile active docs with the new ownership reality after all structural cleanup slices land
- close any stale references left behind by the execution sequence

Study first:
- run a final repo search for retired ownership terms, stale route homes, and obsolete compatibility wording
- compare active docs against actual imports and entrypoints

Primary surfaces:
- `docs/README.md`
- `docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`
- `docs/architecture/architecture-invariants.md`
- `docs/architecture/*`
- `docs/operations/*`
- selective archive updates only where historical context is worth preserving

Required doc updates:
- all active docs touched by the landed cleanup slices

Validation:
- `git diff --check -- <touched paths>`
- `cd frontend && npm run typecheck` if any frontend docs or scripts changed alongside code

Commit boundary:
- docs only

### Program-Level Pre-Commit Minimum

Every cleanup commit must run the smallest meaningful validation bundle for the
touched slice, and at minimum:

- path-scoped `git diff --check -- <touched paths>`
- targeted backend `./.venv_local/bin/pytest -q` for the touched slice
- `cd frontend && npm run typecheck` for frontend changes
- `make doctor` for runtime-authority, Neon, refresh, or ops-contract changes

Docs-only and hygiene-only slices such as 0, 8, and 19 are exempt from backend
`pytest` and `make doctor` unless they also change executable scripts.

When ownership boundaries change, also run the applicable boundary suite:

- `./.venv_local/bin/pytest -q backend/tests/test_architecture_boundaries.py`
- `./.venv_local/bin/pytest -q backend/tests/test_model_family_ownership_boundaries.py`
- `./.venv_local/bin/pytest -q backend/tests/test_cpar_architecture_boundaries.py backend/tests/test_cpar_service_route_boundaries.py`

When a slice changes active docs, update those docs in the same commit instead
of leaving the cleanup reflected only in a tracker note.

If a command is blocked by an environmental issue such as long-running orphaned
processes, record the exact blocker in the execution note before commit.

### Sequencing Notes

- Slices 1-7 should land before the mixed-state read split.
- Slice 8 is an early docs-only correction so runbooks stop overstating `security_master` authority before code containment begins.
- Slices 9-10 can land before the highest-risk cUSE runtime and Neon slices because they are more self-contained.
- Slices 11-14 are the read-layer and compatibility-containment prerequisite chain.
- Slices 15-16 are the cUSE serving decomposition chain and should not be merged into one commit.
- Slices 17-18 are the Neon decomposition chain; keep source-sync separate from mirror/finalize/post-run ownership.
- Slices 9-18 are high-risk structural slices and must each get fresh slice-specific multi-agent review before any code changes.
- Slice 19 happens only after the code slices are landed.
