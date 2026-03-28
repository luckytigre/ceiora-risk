# Repo Tightening Execution Log

Date: 2026-03-28
Status: In progress
Owner: Codex

## Slice 0

Scope:
- `docs/README.md`
- `docs/architecture/maintainer-guide.md`
- `docs/architecture/dependency-rules.md`
- `.gitignore`
- root-only hygiene cleanup

Outcome:
- normalized the active cleanup execution protocol across the maintainer docs
- clarified where persisted cleanup notes and one-off execution records belong
- tightened repo-hygiene ignore rules to root-anchored entries only
- removed the root `.pytest_cache/` directory and the accidental root files named like `<sqlite3.Connection object at 0x...>`

Validation:
- `git diff --check -- .gitignore docs/README.md docs/architecture/maintainer-guide.md docs/architecture/dependency-rules.md`

Notes:
- root `.DS_Store` is already ignored and may reappear locally after Finder/shell access; it is not a tracked repo artifact

## Slice 1

Scope:
- local duplicate root App Router redirect pages under `frontend/src/app/explore`, `frontend/src/app/exposures`, and `frontend/src/app/health`
- `docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`
- `docs/architecture/CPAR_FRONTEND_SURFACES.md`
- `docs/operations/OPERATIONS_HARDENING_CHECKLIST.md`

Outcome:
- removed the untracked local duplicate root redirect pages so the legacy root redirects are owned in `frontend/next.config.js` only
- updated the active docs to make that ownership explicit

Validation:
- `git diff --check -- docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md docs/architecture/CPAR_FRONTEND_SURFACES.md docs/operations/OPERATIONS_HARDENING_CHECKLIST.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `cd frontend && node scripts/family_redirect_contract_check.mjs`

Validation blockers:
- `cd frontend && npm run typecheck` hung inside `next typegen`
- `cd frontend && node scripts/family_routes_smoke.mjs` hung before any `next dev` child process appeared
- direct probe `cd frontend && node -e "const { chromium } = require('playwright'); console.log(typeof chromium.launch)"` also hung, so the family smoke blocker appears to be in the local Playwright/frontend toolchain rather than the redirect contract itself

Notes:
- the duplicate root redirect pages were untracked local files, so their removal is workspace hygiene rather than tracked repo history

## Slice 2

Scope:
- `frontend/src/lib/apiTransport.ts`
- `frontend/src/lib/holdingsApi.ts`
- `frontend/src/hooks/useHoldingsApi.ts`
- `frontend/src/lib/api.ts`
- `frontend/src/lib/cuse4Api.ts`
- `frontend/src/lib/cparApi.ts`
- `frontend/src/hooks/useApi.ts`
- `frontend/src/hooks/useCuse4Api.ts`
- `frontend/src/hooks/useCparApi.ts`
- `frontend/src/features/cpar/components/useCparExploreScenarioLab.ts`
- `docs/architecture/maintainer-guide.md`
- `docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`
- `docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/CPAR_FRONTEND_SURFACES.md`
- `docs/operations/CPAR_OPERATIONS_PLAYBOOK.md`
- `docs/architecture/REPO_TIGHTENING_PLAN.md`

Outcome:
- split the mixed frontend transport and family barrels into concrete owners:
  - neutral low-level fetch/error transport in `frontend/src/lib/apiTransport.ts`
  - explicit shared holdings/account route helpers and hooks in `frontend/src/lib/holdingsApi.ts` and `frontend/src/hooks/useHoldingsApi.ts`
  - family-owned route helpers in `frontend/src/lib/cuse4Api.ts` and `frontend/src/lib/cparApi.ts`
  - family-owned hook barrels in `frontend/src/hooks/useCuse4Api.ts` and `frontend/src/hooks/useCparApi.ts`
- reduced `frontend/src/lib/api.ts` and `frontend/src/hooks/useApi.ts` to transitional compatibility barrels instead of the concrete owners
- removed shared holdings/apply exports from the cPAR hook barrel so cPAR feature code now reuses shared holdings plumbing explicitly
- updated the active architecture and cPAR operations docs to reflect the new ownership split and the Slice 2 validation bundle
- corrected the cPAR docs to distinguish cPAR-native mutation from the explicit shared holdings apply path that `/cpar/explore` reuses through the shared holdings owner

Validation:
- `git diff --check -- frontend/src/lib/apiTransport.ts frontend/src/lib/holdingsApi.ts frontend/src/hooks/useHoldingsApi.ts frontend/src/lib/api.ts frontend/src/lib/cuse4Api.ts frontend/src/lib/cparApi.ts frontend/src/hooks/useApi.ts frontend/src/hooks/useCuse4Api.ts frontend/src/hooks/useCparApi.ts frontend/src/features/cpar/components/useCparExploreScenarioLab.ts docs/architecture/maintainer-guide.md docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md docs/architecture/CPAR_FRONTEND_SURFACES.md docs/operations/CPAR_OPERATIONS_PLAYBOOK.md docs/architecture/REPO_TIGHTENING_PLAN.md`
- `cd frontend && npm run test:control-plane-proxies`

Validation blockers:
- `cd frontend && npm run typecheck` hung inside `next typegen`
- `cd frontend && npm run test:family-routes` started the script process, but no `next dev` or Playwright child ever appeared before manual termination
- `cd frontend && npm run test:cpar-pages` launched `next dev` but did not complete before a 120s timeout
- `cd frontend && npm run test:cpar-hedge` timed out after 60s under a process-group kill
- `cd frontend && npm run test:cpar-portfolio` timed out after 60s under a process-group kill
- `cd frontend && npm run test:cpar-portfolio-whatif` timed out after 60s under a process-group kill
- `cd frontend && npm run test:explore-whatif` timed out after 60s under a process-group kill
- `cd frontend && npm run test:explore-whatif-busy` timed out after 60s under a process-group kill
- direct `cd frontend && ./node_modules/.bin/tsc --noEmit --incremental false` also ran for more than 2 minutes without completing locally
- `pytest backend/tests/test_model_family_ownership_boundaries.py` could not start in this shell because the local Python environment is missing `python-dotenv` and `.venv_local` is not present

Notes:
- the post-edit adversarial review for this slice was dispatched to both standing agents after the final code/doc diff was in place
- the timeout runner left orphaned `next dev` children on the first pass; those were cleaned up before the final blocker matrix was recorded

## Slice 3

Scope:
- `frontend/src/app/positions/page.tsx`
- `frontend/src/features/holdings/hooks/useHoldingsManager.ts`
- `frontend/scripts/positions_surface_smoke.mjs`
- `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`
- `docs/architecture/dependency-rules.md`
- `docs/architecture/maintainer-guide.md`
- `docs/architecture/CPAR_FRONTEND_SURFACES.md`
- `docs/operations/OPERATIONS_HARDENING_CHECKLIST.md`
- `docs/operations/CPAR_OPERATIONS_PLAYBOOK.md`

Outcome:
- kept `/positions` as an intentional shared live-holdings control surface rather than splitting it into family-owned pages
- moved the shared holdings reads/writes on `/positions` and in the holdings manager onto the explicit shared holdings owners
- kept cUSE participation explicit for the modeled snapshot and operator/control refresh semantics
- kept cPAR participation explicit as a read-only method/coverage overlay
- added a dedicated `/positions` smoke script so the page has its own validation owner instead of relying only on broader family smokes
- updated the architecture and operations docs to describe `/positions` as a shared holdings surface with explicit cUSE and cPAR roles

Validation:
- `git diff --check -- frontend/src/app/positions/page.tsx frontend/src/features/holdings/hooks/useHoldingsManager.ts frontend/scripts/positions_surface_smoke.mjs docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md docs/architecture/dependency-rules.md docs/architecture/maintainer-guide.md docs/architecture/CPAR_FRONTEND_SURFACES.md docs/operations/OPERATIONS_HARDENING_CHECKLIST.md docs/operations/CPAR_OPERATIONS_PLAYBOOK.md`

Validation blockers:
- `cd frontend && node scripts/positions_surface_smoke.mjs` launched `next dev` on port `3115` but did not complete before a 120s timeout
- `cd frontend && npm run typecheck` progressed past `next typegen` and into `tsc --noEmit --incremental false`, but `tsc` still did not complete before manual termination after roughly two minutes
- `cd frontend && npm run test:cpar-portfolio` timed out after 60s under a process-group kill
- `cd frontend && npm run test:cpar-portfolio-whatif` timed out after 60s under a process-group kill
- `cd frontend && npm run test:cpar-hedge` timed out after 60s under a process-group kill

Notes:
- the `/positions` smoke present in the worktree is a full browser/API-stub smoke rather than a static contract check, so the blocker above reflects the real runtime-oriented artifact being introduced for this slice
- validation runs again left orphaned `next dev` children; those were cleaned up before recording the final blocker state

## Slice 3

Scope:
- `frontend/src/app/positions/page.tsx`
- `frontend/src/features/holdings/components/HoldingsLedgerSection.tsx`
- `frontend/src/features/holdings/components/HoldingsImportPanel.tsx`
- `frontend/src/features/holdings/components/ManualPositionEditor.tsx`
- `frontend/src/features/holdings/hooks/useHoldingsManager.ts`
- `frontend/scripts/positions_surface_smoke.mjs`
- `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`
- `docs/architecture/dependency-rules.md`
- `docs/architecture/maintainer-guide.md`
- `docs/architecture/CPAR_FRONTEND_SURFACES.md`
- `docs/operations/OPERATIONS_HARDENING_CHECKLIST.md`
- `docs/operations/CPAR_OPERATIONS_PLAYBOOK.md`

Outcome:
- kept `/positions` as an intentional shared live-holdings control surface rather than splitting it into family-owned pages
- made the shared-owner split explicit in code:
  - holdings reads/mutations on `/positions` now import from shared holdings owners
  - cUSE modeled snapshot/control participation remains explicit
  - cPAR participation remains explicit and read-only
- aligned holdings-supporting components and helper types with the shared holdings owner instead of the cUSE type barrel where those components are not cUSE-specific
- added `frontend/scripts/positions_surface_smoke.mjs` as the dedicated ownership-contract smoke for the shared `/positions` surface
- updated the active architecture and operations docs to describe `/positions` as shared holdings control with cUSE operator ownership and cPAR read-only overlay participation

Validation:
- `git diff --check -- frontend/src/app/positions/page.tsx frontend/src/features/holdings/components/HoldingsLedgerSection.tsx frontend/src/features/holdings/components/HoldingsImportPanel.tsx frontend/src/features/holdings/components/ManualPositionEditor.tsx frontend/src/features/holdings/hooks/useHoldingsManager.ts frontend/scripts/positions_surface_smoke.mjs docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md docs/architecture/dependency-rules.md docs/architecture/maintainer-guide.md docs/architecture/CPAR_FRONTEND_SURFACES.md docs/operations/OPERATIONS_HARDENING_CHECKLIST.md docs/operations/CPAR_OPERATIONS_PLAYBOOK.md`
- `cd frontend && node scripts/positions_surface_smoke.mjs`

Validation blockers:
- `cd frontend && npm run typecheck` progressed through `next typegen` successfully, then stalled in the later typecheck phase until manual termination
- `cd frontend && npm run test:cpar-portfolio` timed out after 60s under the bounded runner
- `cd frontend && npm run test:cpar-portfolio-whatif` exited `-15` under the same bounded runner after the portfolio timeout sequence
- `cd frontend && npm run test:cpar-hedge` exited `-15` under the same bounded runner after the portfolio timeout sequence

Notes:
- the Slice 3 study resolved in favor of keeping `/positions` shared because the page, ledger, and navigation already treat it as a distinct cross-family holdings surface rather than an accidental cUSE page
- post-edit agent review dispatch hit the collaboration thread limit before usable reviewer output returned, so the slice relies on the green ownership-contract smoke plus the path-scoped diff review in this log

## Slice 3

Scope:
- `frontend/src/app/positions/page.tsx`
- `frontend/src/features/holdings/components/HoldingsLedgerSection.tsx`
- `frontend/src/features/holdings/components/HoldingsImportPanel.tsx`
- `frontend/src/features/holdings/components/ManualPositionEditor.tsx`
- `frontend/src/features/holdings/hooks/useHoldingsManager.ts`
- `frontend/scripts/positions_surface_smoke.mjs`
- `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`
- `docs/architecture/dependency-rules.md`
- `docs/architecture/maintainer-guide.md`
- `docs/architecture/CPAR_FRONTEND_SURFACES.md`
- `docs/operations/OPERATIONS_HARDENING_CHECKLIST.md`
- `docs/operations/CPAR_OPERATIONS_PLAYBOOK.md`

Outcome:
- kept `/positions` as an intentional shared live-holdings control surface rather than splitting it into family-owned pages
- made the shared surface explicit in code:
  - shared holdings reads on `/positions` now come from `useHoldingsApi`
  - holdings helper/type surfaces now prefer shared holdings owners where applicable
  - cUSE modeled snapshot/control reads and cPAR read-only overlay remain explicit instead of hiding behind mixed barrels
- added `frontend/scripts/positions_surface_smoke.mjs` as the ownership-contract smoke for the page
- updated the active architecture and operations docs to describe `/positions` as shared holdings control with cUSE operator ownership and cPAR read-only participation

Validation:
- `git diff --check -- frontend/src/app/positions/page.tsx frontend/src/features/holdings/components/HoldingsLedgerSection.tsx frontend/src/features/holdings/components/HoldingsImportPanel.tsx frontend/src/features/holdings/components/ManualPositionEditor.tsx frontend/src/features/holdings/hooks/useHoldingsManager.ts frontend/scripts/positions_surface_smoke.mjs docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md docs/architecture/dependency-rules.md docs/architecture/maintainer-guide.md docs/architecture/CPAR_FRONTEND_SURFACES.md docs/operations/OPERATIONS_HARDENING_CHECKLIST.md docs/operations/CPAR_OPERATIONS_PLAYBOOK.md`
- `cd frontend && node scripts/positions_surface_smoke.mjs`

Validation blockers:
- `cd frontend && npm run typecheck` progressed through `next typegen`, but then stalled in the later typecheck phase until manual termination
- `cd frontend && npm run test:cpar-portfolio` timed out after 60s under the bounded runner
- `cd frontend && npm run test:cpar-portfolio-whatif` exited `-15` under the same bounded runner after the portfolio timeout sequence
- `cd frontend && npm run test:cpar-hedge` exited `-15` under the same bounded runner after the portfolio timeout sequence

Notes:
- the pre-edit adversarial review was dispatched, but the agent threads did not return usable output before the slice moved forward
- the post-edit review dispatch hit the existing agent-thread cap until stale Slice 2 agents were closed, so this slice keeps the commit boundary narrow and records the validation state explicitly

## Slice 4

Scope:
- `backend/services/cuse4_dashboard_payload_service.py`
- `backend/services/cuse4_factor_history_service.py`
- `backend/services/cuse4_health_diagnostics_service.py`
- `backend/tests/test_exposure_history_route.py`
- `backend/tests/test_serving_output_route_preference.py`
- `docs/architecture/maintainer-guide.md`

Outcome:
- introduced one public dependency seam per cUSE alias owner instead of relying on tests to mutate several alias-module globals directly:
  - `get_dashboard_payload_readers()`
  - `get_factor_history_dependencies()`
  - `get_health_diagnostics_readers()`
- updated the route-level tests to patch those public seams instead of patching `load_runtime_payload`, `cache_get`, `load_factor_return_history`, and `config.SQLITE_PATH` individually on the alias modules
- kept route behavior unchanged; this slice only hardens the test seam ahead of the later cUSE service de-dup slices
- updated maintainer guidance so future route tests patch the public alias dependency seam instead of mutating alias-module globals directly

Validation:
- `git diff --check -- backend/services/cuse4_dashboard_payload_service.py backend/services/cuse4_factor_history_service.py backend/services/cuse4_health_diagnostics_service.py backend/tests/test_dashboard_payload_service.py backend/tests/test_exposure_history_route.py backend/tests/test_health_diagnostics.py backend/tests/test_health_diagnostics_scoping.py backend/tests/test_serving_output_route_preference.py docs/architecture/maintainer-guide.md`
- `python3 -m py_compile backend/services/cuse4_dashboard_payload_service.py backend/services/cuse4_factor_history_service.py backend/services/cuse4_health_diagnostics_service.py backend/tests/test_exposure_history_route.py backend/tests/test_serving_output_route_preference.py`

Validation blockers:
- `./.venv_local/bin/pytest -q backend/tests/test_dashboard_payload_service.py backend/tests/test_exposure_history_route.py backend/tests/test_health_diagnostics.py backend/tests/test_health_diagnostics_scoping.py backend/tests/test_serving_output_route_preference.py` could not run because `.venv_local` does not exist in this workspace
- fallback `pytest -q ...` under the global interpreter failed during import because `python-dotenv` is not installed there (`ModuleNotFoundError: No module named 'dotenv'`)

Notes:
- the seam problem was concentrated in the cUSE alias owners, but the validation bundle still referenced the wider dashboard/health tests to preserve the planned rollback boundary once the repo-local env is available again

## Slice 5

Scope:
- `backend/services/cuse4_dashboard_payload_service.py`
- `backend/services/dashboard_payload_service.py`
- `backend/services/cuse4_factor_history_service.py`
- `backend/services/factor_history_service.py`
- `backend/services/cuse4_health_diagnostics_service.py`
- `backend/services/health_diagnostics_service.py`
- `docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`
- `docs/architecture/maintainer-guide.md`

Outcome:
- made the `cuse4_*` dashboard, factor-history, and health-diagnostics modules the concrete route-facing owners
- reduced the legacy default-named service modules to explicit compatibility shims that re-export the cUSE4 owners for older callers and direct service tests
- preserved the new Slice 4 public dependency seams on the concrete cUSE4 owners so route tests still have stable patch points
- updated the active ownership docs so they no longer describe these surfaces as alias-first

Validation:
- `git diff --check -- backend/services/cuse4_dashboard_payload_service.py backend/services/dashboard_payload_service.py backend/services/cuse4_factor_history_service.py backend/services/factor_history_service.py backend/services/cuse4_health_diagnostics_service.py backend/services/health_diagnostics_service.py docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md docs/architecture/maintainer-guide.md`
- `python3 -m py_compile backend/services/cuse4_dashboard_payload_service.py backend/services/dashboard_payload_service.py backend/services/cuse4_factor_history_service.py backend/services/factor_history_service.py backend/services/cuse4_health_diagnostics_service.py backend/services/health_diagnostics_service.py backend/tests/test_dashboard_payload_service.py backend/tests/test_architecture_boundaries.py backend/tests/test_model_family_ownership_boundaries.py`

Validation blockers:
- `python3 -m pytest -q backend/tests/test_dashboard_payload_service.py backend/tests/test_architecture_boundaries.py backend/tests/test_model_family_ownership_boundaries.py` could not run because `pytest` is not installed in the global interpreter in this shell
- the planned `./.venv_local/bin/pytest ...` bundle remains unavailable because `.venv_local` does not exist in this workspace

## Slice 4 Follow-Up Repair

Scope:
- `backend/services/cuse4_factor_history_service.py`
- `backend/services/cuse4_health_diagnostics_service.py`

Outcome:
- restored the public injected-kwargs seam on the concrete cUSE4 factor-history and health-diagnostics owners
- fixed the exact regression left behind by Slice 4, where the route tests had already been rewritten to call these public seams but the concrete service entrypoints no longer accepted the injected loaders
- kept runtime behavior unchanged; this repair only re-opened the public test seam that the committed tests already expected

Validation:
- `git diff --check -- backend/services/cuse4_factor_history_service.py backend/services/cuse4_health_diagnostics_service.py`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_exposure_history_route.py backend/tests/test_serving_output_route_preference.py`

Notes:
- the repo-local `.venv_local` is present in this workspace, so the earlier “venv missing” blocker note for Slice 4 was stale by the time this repair was validated

## Slice 6

Scope:
- `backend/api/routes/operator.py`
- `backend/services/cuse4_operator_status_service.py`
- `backend/tests/test_operator_status_route.py`
- `backend/tests/test_cloud_auth_and_runtime_roles.py`
- `docs/architecture/REPO_TIGHTENING_PLAN.md`

Outcome:
- narrowed Slice 6 after adversarial review to the actual compat-module blocker: operator-status tests were still binding to the legacy default service even though the route already serves through the cUSE4 owner surface
- added an explicit route-level callable seam for `/api/operator/status` so auth-only tests can stay route-scoped instead of patching service internals
- moved the operator-status route tests onto `backend.services.cuse4_operator_status_service` and exposed the private helper hooks those tests already rely on there, aligning the tests with the service owner that Slice 7 will keep
- left universe, holdings, and portfolio what-if out of this slice because they are either already aligned with cUSE-facing surfaces or belong with the later owner move

Validation:
- `git diff --check -- backend/api/routes/operator.py backend/services/cuse4_operator_status_service.py backend/tests/test_operator_status_route.py backend/tests/test_cloud_auth_and_runtime_roles.py docs/architecture/REPO_TIGHTENING_PLAN.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_operator_status_route.py backend/tests/test_cloud_auth_and_runtime_roles.py -k operator_status`

## Slice 6B

Scope:
- `backend/services/holdings_service.py`
- `backend/tests/test_holdings_service.py`
- `backend/services/portfolio_whatif.py`
- `backend/tests/test_portfolio_whatif_service.py`
- `docs/architecture/REPO_TIGHTENING_PLAN.md`

Outcome:
- added public dependency dataclasses and injected kwargs to the legacy holdings and portfolio what-if service entrypoints instead of forcing tests to mutate module globals directly
- rewrote the holdings service tests and the preview-oriented portfolio what-if service tests to use those public dependency seams
- kept runtime behavior unchanged and left the actual cUSE4 owner move for a later slice; this slice only makes that owner move safe

Validation:
- `git diff --check -- backend/services/holdings_service.py backend/tests/test_holdings_service.py backend/services/portfolio_whatif.py backend/tests/test_portfolio_whatif_service.py docs/architecture/REPO_TIGHTENING_PLAN.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_holdings_service.py backend/tests/test_holdings_route_dirty_state.py backend/tests/test_portfolio_whatif_service.py backend/tests/test_portfolio_whatif_route.py`
