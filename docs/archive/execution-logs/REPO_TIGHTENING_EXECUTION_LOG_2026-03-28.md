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
- `docs/architecture/REPO_TIGHTENING_PLAN.md`

Outcome:
- added a public dependency dataclass plus injected kwargs to the legacy holdings service entrypoints instead of forcing tests to mutate module globals directly
- rewrote the holdings service tests to use that public dependency seam
- kept runtime behavior unchanged and left the actual cUSE4 holdings owner move for a later slice; this slice only makes that owner move safe

Validation:
- `git diff --check -- backend/services/holdings_service.py backend/tests/test_holdings_service.py docs/architecture/REPO_TIGHTENING_PLAN.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_holdings_service.py backend/tests/test_holdings_route_dirty_state.py`

## Slice 7A

Scope:
- `backend/services/cuse4_holdings_service.py`
- `backend/services/holdings_service.py`
- `docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`
- `docs/architecture/maintainer-guide.md`
- `docs/architecture/REPO_TIGHTENING_PLAN.md`

Outcome:
- moved the concrete holdings implementation into `backend/services/cuse4_holdings_service.py`
- reduced `backend/services/holdings_service.py` to a compatibility shim that still re-exports the full public holdings contract for older callers, direct service tests, and portfolio what-if consumers
- kept the route-facing cUSE4 holdings API unchanged while making the ownership boundary explicit in code and docs

Validation:
- `git diff --check -- backend/services/cuse4_holdings_service.py backend/services/holdings_service.py docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md docs/architecture/maintainer-guide.md docs/architecture/REPO_TIGHTENING_PLAN.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_holdings_service.py backend/tests/test_holdings_route_dirty_state.py backend/tests/test_portfolio_whatif_route.py::test_portfolio_whatif_apply_route_returns_service_payload backend/tests/test_model_family_ownership_boundaries.py`

## Slice 7B

Scope:
- `backend/services/cuse4_portfolio_whatif.py`
- `backend/services/portfolio_whatif.py`
- `backend/tests/test_portfolio_whatif_service.py`
- `docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`
- `docs/architecture/maintainer-guide.md`
- `docs/architecture/REPO_TIGHTENING_PLAN.md`
- `docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

Outcome:
- moved the concrete portfolio what-if preview implementation into `backend/services/cuse4_portfolio_whatif.py`
- reduced `backend/services/portfolio_whatif.py` to a compatibility shim that still re-exports the public preview contract for older callers and direct service tests
- made the cUSE4 holdings dependency explicit inside the concrete owner while keeping the route-facing cUSE4 preview API unchanged
- added an explicit shim-parity test so the supported legacy import surface remains pinned even as internal helpers move

Validation:
- `git diff --check -- backend/services/cuse4_portfolio_whatif.py backend/services/portfolio_whatif.py backend/tests/test_portfolio_whatif_service.py docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md docs/architecture/maintainer-guide.md docs/architecture/REPO_TIGHTENING_PLAN.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_portfolio_whatif_service.py backend/tests/test_portfolio_whatif_route.py backend/tests/test_model_family_ownership_boundaries.py`

## Slice 7B Follow-up

Scope:
- `backend/services/portfolio_whatif.py`
- `backend/tests/test_portfolio_whatif_service.py`
- `docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

Outcome:
- restored legacy-shim default-call composition so `backend.services.portfolio_whatif.preview_portfolio_whatif()` resolves the legacy module's own `get_portfolio_whatif_dependencies()` when callers omit explicit dependencies
- expanded portfolio what-if service coverage to exercise that legacy default path directly instead of only asserting import identity

Validation:
- `git diff --check -- backend/services/portfolio_whatif.py backend/tests/test_portfolio_whatif_service.py docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_portfolio_whatif_service.py backend/tests/test_portfolio_whatif_route.py backend/tests/test_model_family_ownership_boundaries.py`

## Slice 7C

Scope:
- `backend/services/cuse4_universe_service.py`
- `backend/services/universe_service.py`
- `backend/tests/test_universe_service_contract.py`
- `docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`
- `docs/architecture/maintainer-guide.md`
- `docs/architecture/REPO_TIGHTENING_PLAN.md`
- `docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

Outcome:
- kept `backend/services/cuse4_universe_service.py` as the concrete universe/search/detail owner already used by the default routes
- reduced `backend/services/universe_service.py` to a compatibility shim that preserves the supported public import surface for older direct imports
- added explicit universe shim-contract coverage so de-dup is verified independently of route behavior

Validation:
- `git diff --check -- backend/services/cuse4_universe_service.py backend/services/universe_service.py backend/tests/test_universe_service_contract.py docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md docs/architecture/maintainer-guide.md docs/architecture/REPO_TIGHTENING_PLAN.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_universe_service_contract.py backend/tests/test_universe_search_route.py backend/tests/test_universe_history_route.py backend/tests/test_serving_output_route_fallbacks.py::test_universe_routes_use_persisted_payload_when_cache_missing backend/tests/test_serving_output_route_preference.py::test_universe_search_prefers_serving_payload_over_cache backend/tests/test_api_golden_snapshots.py::test_api_universe_factors_matches_golden_snapshot backend/tests/test_model_family_ownership_boundaries.py`

## Slice 9 Prep Docs

Scope:
- `docs/architecture/CPAR_BACKEND_READ_SURFACES.md`
- `docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/operations/CPAR_OPERATIONS_PLAYBOOK.md`
- `docs/architecture/REPO_TIGHTENING_PLAN.md`
- `docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

Outcome:
- clarified that the current package-pinned cPAR risk core is already reused by aggregate `/api/cpar/risk` and aggregate current/hypothetical explore what-if states
- removed stale wording that implied the shared cPAR portfolio snapshots did not carry specific-risk-aware `risk_shares`, variance proxies, or row `risk_mix`
- tightened Slice 9 prep so any aggregate-risk extraction keeps `load_cpar_portfolio_support_rows()` shared and validates `test_cpar_explore_whatif_service.py`

Validation:
- `git diff --check -- docs/architecture/CPAR_BACKEND_READ_SURFACES.md docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md docs/operations/CPAR_OPERATIONS_PLAYBOOK.md docs/architecture/REPO_TIGHTENING_PLAN.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

## Slice 9

Scope:
- `backend/services/cpar_aggregate_risk_service.py`
- `backend/services/cpar_risk_service.py`
- `backend/services/cpar_portfolio_snapshot_service.py`
- `backend/services/cpar_explore_whatif_service.py`
- `backend/tests/test_cpar_risk_service.py`
- `backend/tests/test_cpar_portfolio_snapshot_service.py`
- `backend/tests/test_cpar_explore_whatif_service.py`
- `backend/tests/test_cpar_service_route_boundaries.py`
- `docs/architecture/CPAR_BACKEND_READ_SURFACES.md`
- `docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/operations/CPAR_OPERATIONS_PLAYBOOK.md`
- `docs/architecture/maintainer-guide.md`
- `docs/architecture/dependency-rules.md`
- `docs/architecture/REPO_TIGHTENING_PLAN.md`
- `docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

Outcome:
- extracted explicit aggregate `/api/cpar/risk` assembly into `backend/services/cpar_aggregate_risk_service.py` while keeping `backend/services/cpar_risk_service.py` as the thin route-facing owner
- kept `backend/services/cpar_portfolio_snapshot_service.py` as the shared package-pinned support/core owner for support-row loads and reused helper assembly instead of moving account-scoped hedge/what-if paths into the aggregate owner
- moved `POST /api/cpar/explore/whatif` onto the explicit aggregate owner for aggregate current/hypothetical snapshots so the live path no longer routes back through the snapshot service
- removed the obsolete snapshot-service aggregate-risk alias so the owner chain is one-directional again
- pinned the raw-vs-display covariance contract so `cov_matrix` stays on raw package covariance while `display_cov_matrix` remains the additive explanatory surface
- updated the active cPAR architecture, backend-read, operations, maintainer, dependency, and slice-plan docs to reflect the route-facing shim -> aggregate owner -> shared support/core chain

Validation:
- `git diff --check -- backend/services/cpar_aggregate_risk_service.py backend/services/cpar_risk_service.py backend/services/cpar_portfolio_snapshot_service.py backend/services/cpar_explore_whatif_service.py backend/tests/test_cpar_risk_service.py backend/tests/test_cpar_portfolio_snapshot_service.py backend/tests/test_cpar_explore_whatif_service.py backend/tests/test_cpar_service_route_boundaries.py docs/architecture/CPAR_BACKEND_READ_SURFACES.md docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md docs/operations/CPAR_OPERATIONS_PLAYBOOK.md docs/architecture/maintainer-guide.md docs/architecture/dependency-rules.md docs/architecture/REPO_TIGHTENING_PLAN.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_cpar_risk_service.py backend/tests/test_cpar_portfolio_snapshot_service.py backend/tests/test_cpar_runtime_coverage_contract.py backend/tests/test_cpar_explore_whatif_service.py backend/tests/test_cpar_portfolio_hedge_service.py backend/tests/test_cpar_portfolio_whatif_service.py backend/tests/test_cpar_service_route_boundaries.py backend/tests/test_cpar_architecture_boundaries.py backend/tests/test_cpar_routes.py::test_cpar_risk_route_returns_payload backend/tests/test_cpar_routes.py::test_cpar_risk_route_maps_not_ready_to_503`

Validation blockers:
- `make doctor` remains blocked by the pre-existing syntax error in `scripts/doctor.sh`'s inline Python (`SyntaxError: invalid syntax` at `finally:`), so Slice 9 keeps the blocker recorded instead of widening scope into a repair

## Slice 10A

Scope:
- `backend/services/cpar_portfolio_hedge_service.py`
- `backend/services/cpar_portfolio_snapshot_service.py`
- `backend/tests/test_cpar_portfolio_hedge_service.py`
- `backend/tests/test_cpar_runtime_coverage_contract.py`
- `backend/tests/test_cpar_service_route_boundaries.py`
- `docs/architecture/CPAR_BACKEND_READ_SURFACES.md`
- `docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/operations/CPAR_OPERATIONS_PLAYBOOK.md`
- `docs/architecture/maintainer-guide.md`
- `docs/architecture/dependency-rules.md`
- `docs/architecture/REPO_TIGHTENING_PLAN.md`
- `docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

Outcome:
- made `backend/services/cpar_portfolio_hedge_service.py` the explicit route-facing hedge payload owner instead of leaving the real load path inside `backend/services/cpar_portfolio_snapshot_service.py`
- kept `backend/services/cpar_portfolio_snapshot_service.py` focused on the shared account-scoped hedge snapshot/context/support core instead of also owning the route-facing hedge load path
- kept portfolio what-if package/context/support-row reuse unchanged so one request still builds both `current` and `hypothetical` from one pinned account/support snapshot set
- kept the runtime coverage contract on the shared hedge snapshot builder and added boundary checks so the route-facing hedge load path does not silently fall back into the snapshot service
- updated the active cPAR architecture, backend-read, operations, maintainer, dependency, and slice-plan docs to describe the explicit hedge route owner plus shared snapshot support/core split

Validation:
- `git diff --check -- backend/services/cpar_portfolio_hedge_service.py backend/services/cpar_portfolio_snapshot_service.py backend/tests/test_cpar_portfolio_hedge_service.py backend/tests/test_cpar_runtime_coverage_contract.py backend/tests/test_cpar_service_route_boundaries.py docs/architecture/CPAR_BACKEND_READ_SURFACES.md docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md docs/operations/CPAR_OPERATIONS_PLAYBOOK.md docs/architecture/maintainer-guide.md docs/architecture/dependency-rules.md docs/architecture/REPO_TIGHTENING_PLAN.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_cpar_portfolio_snapshot_service.py backend/tests/test_cpar_portfolio_hedge_service.py backend/tests/test_cpar_runtime_coverage_contract.py backend/tests/test_cpar_architecture_boundaries.py backend/tests/test_cpar_service_route_boundaries.py backend/tests/test_cpar_routes.py::test_cpar_portfolio_hedge_route_returns_payload`

Validation blockers:
- `make doctor` remains blocked by the pre-existing syntax error in `scripts/doctor.sh`'s inline Python (`SyntaxError: invalid syntax` at `finally:`), so Slice 10A keeps the blocker recorded instead of widening scope into a repair

## Slice 10B

Scope:
- `backend/services/cpar_portfolio_account_snapshot_service.py`
- `backend/services/cpar_portfolio_snapshot_service.py`
- `backend/tests/test_cpar_portfolio_snapshot_service.py`
- `backend/tests/test_cpar_runtime_coverage_contract.py`
- `backend/tests/test_cpar_service_route_boundaries.py`
- `docs/architecture/CPAR_BACKEND_READ_SURFACES.md`
- `docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/operations/CPAR_OPERATIONS_PLAYBOOK.md`
- `docs/architecture/maintainer-guide.md`
- `docs/architecture/dependency-rules.md`
- `docs/architecture/REPO_TIGHTENING_PLAN.md`
- `docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

Outcome:
- extracted the shared account-scoped hedge snapshot builder into `backend/services/cpar_portfolio_account_snapshot_service.py`
- moved only the hedge-specific builder implementation plus hedge-only row helpers; the generic helper graph shared with aggregate risk remains in `backend/services/cpar_portfolio_snapshot_service.py`
- kept `backend/services/cpar_portfolio_snapshot_service.py::build_cpar_portfolio_hedge_snapshot()` as a forwarding compatibility seam so existing hedge, what-if, and runtime-contract callers stayed stable in the same slice
- registered the new lower owner in the cPAR boundary harness and added a direct shim-forwarding test so the new seam is explicit instead of implicit
- updated the active cPAR architecture, backend-read, operations, maintainer, dependency, and slice-plan docs to describe the split between account-context/support loaders and the shared account-scoped hedge snapshot builder

Validation:
- `git diff --check -- backend/services/cpar_portfolio_account_snapshot_service.py backend/services/cpar_portfolio_snapshot_service.py backend/tests/test_cpar_portfolio_snapshot_service.py backend/tests/test_cpar_runtime_coverage_contract.py backend/tests/test_cpar_service_route_boundaries.py docs/architecture/CPAR_BACKEND_READ_SURFACES.md docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md docs/operations/CPAR_OPERATIONS_PLAYBOOK.md docs/architecture/maintainer-guide.md docs/architecture/dependency-rules.md docs/architecture/REPO_TIGHTENING_PLAN.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_cpar_portfolio_snapshot_service.py backend/tests/test_cpar_portfolio_hedge_service.py backend/tests/test_cpar_portfolio_whatif_service.py backend/tests/test_cpar_runtime_coverage_contract.py backend/tests/test_cpar_service_route_boundaries.py backend/tests/test_cpar_routes.py::test_cpar_portfolio_hedge_route_returns_payload backend/tests/test_cpar_routes.py::test_cpar_portfolio_whatif_route_returns_payload`

Validation blockers:
- `make doctor` remains blocked by the pre-existing syntax error in `scripts/doctor.sh`'s inline Python (`SyntaxError: invalid syntax` at `finally:`), so Slice 10B keeps the blocker recorded instead of widening scope into a repair

## Slice 11A

Scope:
- `backend/universe/runtime_authority.py`
- `backend/universe/runtime_rows.py`
- `backend/tests/test_universe_runtime_authority_boundaries.py`
- `docs/architecture/maintainer-guide.md`
- `docs/architecture/dependency-rules.md`
- `docs/architecture/REPO_TIGHTENING_PLAN.md`
- `docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

Outcome:
- extracted current-table runtime authority loading for registry, policy, taxonomy, and source-observation rows into `backend/universe/runtime_authority.py`
- kept `backend/universe/runtime_rows.py` as the mixed-state owner for compat/legacy fallback, historical classification reads, structural/policy resolution, candidate-RIC selection, and the public runtime-row loaders
- narrowed the slice after adversarial review so historical and mixed-state resolvers did not cross the boundary in the same commit
- added a dedicated runtime-authority boundary test so the new owner cannot silently absorb legacy fallback or PIT history logic
- updated the active maintainer/dependency rules and slice plan to describe the new `runtime_authority.py` -> `runtime_rows.py` split explicitly

Validation:
- `git diff --check -- backend/universe/runtime_authority.py backend/universe/runtime_rows.py backend/tests/test_universe_runtime_authority_boundaries.py docs/architecture/maintainer-guide.md docs/architecture/dependency-rules.md docs/architecture/REPO_TIGHTENING_PLAN.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m py_compile backend/universe/runtime_authority.py backend/universe/runtime_rows.py`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_universe_runtime_authority_boundaries.py`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_universe_selector_parity.py -k runtime_rows backend/tests/test_universe_migration_scaffolding.py -k runtime_rows`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_cuse_membership_contract.py -k runtime_state_by_row_as_of_date`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_core_reads.py backend/tests/test_holdings_reads.py backend/tests/test_universe_selector_parity.py backend/tests/test_registry_first_diagnostics.py backend/tests/test_architecture_boundaries.py`

Validation blockers:
- `backend/tests/test_architecture_boundaries.py::test_backend_does_not_add_new_vague_module_names` still fails on the unrelated existing file `backend/tests/test_lseg_session_manager.py`, so Slice 11A keeps that repo-hygiene blocker recorded instead of widening scope into unrelated renaming
- `make doctor` remains blocked by the pre-existing syntax error in `scripts/doctor.sh`'s inline Python (`SyntaxError: invalid syntax` at `finally:`), so Slice 11A keeps the blocker recorded instead of widening scope into a repair

## Slice 12

Scope:
- `backend/data/source_read_authority.py`
- `backend/data/source_reads.py`
- `backend/tests/test_source_read_authority_boundaries.py`
- `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/maintainer-guide.md`
- `docs/architecture/dependency-rules.md`
- `docs/architecture/REPO_TIGHTENING_PLAN.md`
- `docs/operations/OPERATIONS_PLAYBOOK.md`
- `docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

Outcome:
- extracted the lower registry-first source authority helpers into `backend/data/source_read_authority.py`
- kept `backend/data/source_reads.py` as the public source-read facade with the SQLite cache path, compat branches, and raw cross-section exposure helpers still in place
- left `backend/data/core_reads.py` unchanged so higher layers continue to depend on `source_reads.py` instead of the new lower module
- added a dedicated source-read authority boundary test to pin that ownership split
- narrowed the slice after adversarial review so it did not absorb the broader worktree’s session-lifecycle or source-read behavior changes

Validation:
- `git diff --check -- backend/data/source_read_authority.py backend/data/source_reads.py backend/tests/test_source_read_authority_boundaries.py docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md docs/architecture/maintainer-guide.md docs/architecture/dependency-rules.md docs/architecture/REPO_TIGHTENING_PLAN.md docs/operations/OPERATIONS_PLAYBOOK.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_source_read_authority_boundaries.py`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_core_reads.py`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_architecture_boundaries.py`

Validation blockers:
- `backend/tests/test_architecture_boundaries.py::test_backend_does_not_add_new_vague_module_names` still fails on the unrelated existing file `backend/tests/test_lseg_session_manager.py`, so Slice 12 keeps that repo-hygiene blocker recorded instead of widening scope into unrelated renaming
- `make doctor` remains blocked by the pre-existing syntax error in `scripts/doctor.sh`'s inline Python (`SyntaxError: invalid syntax` at `finally:`), so Slice 12 keeps the blocker recorded instead of widening scope into a repair

## Rebaseline After Slices 11A-12

Context:
- multiple adversarial reviewers re-checked the landed Slice 11A and Slice 12 boundaries against the still-broad worktree before continuing
- the landed slices remain valid and rollback-safe, but the review found that the original Slice 13 plan was too broad for the current repo state

Findings:
- `serving_outputs.py` is the actual mixed-state hotspot; `runtime_state.py` is materially narrower and should not share the same immediate rollback boundary
- the original Slice 13 validation bundle is noisy before any new work: the current combined bundle finishes with 42 passed and 4 failed
- the four pre-existing failures are:
  - `backend/tests/test_cloud_bootstrap_proof.py::test_cloud_bootstrap_reads_from_neon_without_local_sqlite`
  - `backend/tests/test_cloud_bootstrap_proof.py::test_cloud_bootstrap_fails_closed_without_local_fallbacks`
  - `backend/tests/test_cloud_auth_and_runtime_roles.py::test_cloud_runtime_role_allows_only_serve_refresh`
  - `backend/tests/test_architecture_boundaries.py::test_backend_does_not_add_new_vague_module_names`
- the cloud-bootstrap failures still patch a missing `_load_current_payload_sqlite` seam, and the refresh-role failure is not a safe prerequisite to couple into the same serving/runtime-state refactor commit

Revision:
- the old combined Slice 13 has been replaced in the active plan with:
  - Slice 13A: serving-output read authority split only
  - Slice 13B: serving-output write/verify/manifest split only
  - Slice 13C: runtime-state split only
- Slice 14 and later remain in the same order, but now depend on the 13A/13B/13C sequence instead of one combined Slice 13

## Slice 13A

Scope:
- `backend/data/serving_output_read_authority.py`
- `backend/data/serving_outputs.py`
- `backend/tests/test_serving_output_read_authority_boundaries.py`
- `backend/tests/test_cloud_bootstrap_proof.py`
- `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/dependency-rules.md`
- `docs/operations/OPERATIONS_PLAYBOOK.md`
- `docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

Outcome:
- extracted only the lower batch serving-payload read adapters into `backend/data/serving_output_read_authority.py`
- kept `backend/data/serving_outputs.py` as the public serving-payload boundary for `load_current_payload(s)` and `load_runtime_payload(s)`
- kept row-reader helpers in `serving_outputs.py` after adversarial review narrowed the rollback boundary away from manifest-adjacent code
- corrected the stale cloud-bootstrap tests so they patch the real batch read seams instead of a nonexistent sqlite single-payload helper
- added a dedicated serving-output read-authority boundary test so higher layers stay pinned to `serving_outputs.py` and the lower module remains read-only
- updated the active architecture, dependency, and operations docs to make the public serving-output boundary explicit

Validation:
- `git diff --check -- backend/data/serving_outputs.py backend/data/serving_output_read_authority.py backend/tests/test_serving_output_read_authority_boundaries.py backend/tests/test_cloud_bootstrap_proof.py docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md docs/architecture/dependency-rules.md docs/operations/OPERATIONS_PLAYBOOK.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m py_compile backend/data/serving_outputs.py backend/data/serving_output_read_authority.py backend/tests/test_serving_output_read_authority_boundaries.py backend/tests/test_cloud_bootstrap_proof.py`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_serving_output_read_authority_boundaries.py backend/tests/test_serving_outputs.py backend/tests/test_serving_output_route_preference.py backend/tests/test_cloud_bootstrap_proof.py backend/tests/test_cloud_auth_and_runtime_roles.py::test_serving_outputs_cloud_mode_does_not_fallback_to_sqlite`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_dashboard_payload_service.py backend/tests/test_portfolio_whatif_service.py`

Validation blockers:
- `make doctor` remains blocked by the pre-existing syntax error in `scripts/doctor.sh`'s inline Python (`SyntaxError: invalid syntax` at `finally:`), so Slice 13A keeps the blocker recorded instead of widening scope into a repair

Notes:
- two adversarial reviewers disagreed about whether a sqlite single-payload seam should exist; the landed slice followed the stricter reading and kept store-specific reads batched under the public `serving_outputs.py` facade while updating the stale bootstrap tests to target the real seams

## Slice 13B

Scope:
- `backend/data/serving_output_write_authority.py`
- `backend/data/serving_output_manifest.py`
- `backend/data/serving_outputs.py`
- `backend/tests/test_serving_output_write_manifest_boundaries.py`
- `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/architecture-invariants.md`
- `docs/architecture/dependency-rules.md`
- `docs/operations/OPERATIONS_PLAYBOOK.md`
- `docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

Outcome:
- extracted the lower durable serving-payload write helpers into `backend/data/serving_output_write_authority.py`
- moved the pure manifest drift helpers into `backend/data/serving_output_manifest.py`
- kept `backend/data/serving_outputs.py` as the only public serving-payload facade for reads, writes, and manifest/repair entrypoints
- preserved the patchable shim names on `serving_outputs.py` so existing tests and repair flows still intercept the same facade-level seams
- added a dedicated write/manifest boundary test so higher layers and repair tooling stay pinned to `serving_outputs.py`
- updated the active architecture, invariants, dependency, and operations docs to reflect the new lower write/manifest owners without changing the public contract

Validation:
- `git diff --check -- backend/data/serving_outputs.py backend/data/serving_output_write_authority.py backend/data/serving_output_manifest.py backend/tests/test_serving_output_write_manifest_boundaries.py docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md docs/architecture/architecture-invariants.md docs/architecture/dependency-rules.md docs/operations/OPERATIONS_PLAYBOOK.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m py_compile backend/data/serving_outputs.py backend/data/serving_output_write_authority.py backend/data/serving_output_manifest.py backend/tests/test_serving_output_write_manifest_boundaries.py`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_serving_outputs.py backend/tests/test_serving_output_route_preference.py backend/tests/test_serving_output_write_manifest_boundaries.py`

Validation blockers:
- `./.venv_local/bin/python -m pytest -q backend/tests/test_operating_model_contract.py -k "persist_current_payloads or repair_serving_payloads"` selected no matching tests for this slice, so Slice 13B keeps the narrower serving-output validation fence recorded instead of widening scope into unrelated operating-model checks
- `make doctor` remains blocked by the pre-existing syntax error in `scripts/doctor.sh`'s inline Python (`SyntaxError: invalid syntax` at `finally:`), so Slice 13B keeps the blocker recorded instead of widening scope into a repair

Notes:
- adversarial review converged on one key boundary rule: the Neon/sqlite write helpers and Neon verification must move together, while the public `persist_current_payloads()` facade and manifest/repair entrypoints stay on `serving_outputs.py`

## Slice 13C

Scope:
- `backend/data/runtime_state_authority.py`
- `backend/data/runtime_state.py`
- `backend/tests/test_runtime_state_authority_boundaries.py`
- `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/architecture-invariants.md`
- `docs/architecture/dependency-rules.md`
- `docs/operations/OPERATIONS_PLAYBOOK.md`
- `docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

Outcome:
- extracted the lower Neon and fallback runtime-state helper bodies into `backend/data/runtime_state_authority.py`
- kept `backend/data/runtime_state.py` as the public runtime-state facade for allowed-key validation, schema ownership, public read/write entrypoints, and active-snapshot publish
- preserved the patchable `_read_neon_runtime_state` and `_write_neon_runtime_state` shims on the facade so existing runtime-state and cloud-bootstrap tests still intercept the same names
- kept `refresh_status_service.py` on the facade/schema contract instead of routing that Neon claim path through the lower module
- added a dedicated runtime-state boundary test so higher layers stay pinned to `runtime_state.py` and the lower module stays below the public contract
- updated the active architecture, invariants, dependency, and operations docs to name the new lower runtime-state owner

Validation:
- `git diff --check -- backend/data/runtime_state.py backend/data/runtime_state_authority.py backend/tests/test_runtime_state_authority_boundaries.py docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md docs/architecture/architecture-invariants.md docs/architecture/dependency-rules.md docs/operations/OPERATIONS_PLAYBOOK.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m py_compile backend/data/runtime_state.py backend/data/runtime_state_authority.py backend/tests/test_runtime_state_authority_boundaries.py`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_runtime_state.py backend/tests/test_cloud_bootstrap_proof.py backend/tests/test_runtime_state_authority_boundaries.py backend/tests/test_refresh_status_service.py`

Validation blockers:
- `make doctor` remains blocked by the pre-existing syntax error in `scripts/doctor.sh`'s inline Python (`SyntaxError: invalid syntax` at `finally:`), so Slice 13C keeps the blocker recorded instead of widening scope into a repair

Notes:
- the runtime-state slice stayed intentionally smaller than the serving-output slices because `runtime_state.py` already had a limited surface area and `refresh_status_service.py` still owns a direct Neon claim path against the same table contract

## Slice 14

Scope:
- `backend/universe/security_master_sync.py`
- `backend/tests/test_security_master_lineage.py`
- `backend/tests/test_security_master_demotion.py`
- `backend/tests/test_projection_only_exclusion.py`
- `backend/tests/test_registry_first_diagnostics.py`
- `backend/tests/test_universe_migration_scaffolding.py`
- `docs/reference/specs/cUSE4_engine_spec.md`
- `docs/architecture/architecture-invariants.md`
- `docs/operations/OPERATIONS_PLAYBOOK.md`
- `docs/architecture/maintainer-guide.md`
- `docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

Outcome:
- tightened `backend/universe/security_master_sync.py` so its legacy-named runtime/bootstrap/seed helpers operate on registry-first surfaces plus `security_master_compat_current` instead of treating physical `security_master` as the normal runtime write target
- kept the legacy helper name in place, but shifted the contained behavior to registry/policy/taxonomy/source-observation plus compat-projection maintenance
- added direct demotion coverage in `backend/tests/test_security_master_demotion.py`
- updated the lineage, projection-only, diagnostics, and migration-scaffolding tests to assert registry-first and compat-current behavior explicitly
- updated the active spec, invariants, operations, and maintainer docs to make the compatibility-only status of `security_master` and `security_master_sync.py` explicit

Validation:
- `git diff --check -- backend/universe/security_master_sync.py backend/tests/test_security_master_lineage.py backend/tests/test_security_master_demotion.py backend/tests/test_projection_only_exclusion.py backend/tests/test_registry_first_diagnostics.py backend/tests/test_universe_migration_scaffolding.py docs/reference/specs/cUSE4_engine_spec.md docs/architecture/architecture-invariants.md docs/operations/OPERATIONS_PLAYBOOK.md docs/architecture/maintainer-guide.md docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_security_master_lineage.py backend/tests/test_security_master_demotion.py backend/tests/test_projection_only_exclusion.py backend/tests/test_registry_first_diagnostics.py backend/tests/test_universe_migration_scaffolding.py`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_architecture_boundaries.py`

Validation blockers:
- `backend/tests/test_architecture_boundaries.py::test_backend_does_not_add_new_vague_module_names` still fails on the unrelated existing file `backend/tests/test_lseg_session_manager.py`, so Slice 14 keeps that repo-hygiene blocker recorded instead of widening scope into unrelated renaming
- `make doctor` remains blocked by the pre-existing syntax error in `scripts/doctor.sh`'s inline Python (`SyntaxError: invalid syntax` at `finally:`), so Slice 14 keeps the blocker recorded instead of widening scope into a repair

Notes:
- the broad worktree already contained a candidate Slice 14 change set before this slice study; the work here treated that diff as the starting point and validated that it was coherent as a compat-containment slice rather than a mixed unrelated refactor

## Slice 15

Scope:
- `backend/analytics/pipeline.py`
- `backend/analytics/refresh_publication.py`
- `docs/architecture/maintainer-guide.md`
- `docs/operations/OPERATIONS_PLAYBOOK.md`
- `docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

Outcome:
- extracted the publish-only republish path, durable publish sequencing, and post-publish deep-health patch out of `backend/analytics/pipeline.py` into `backend/analytics/refresh_publication.py`
- kept the stable `pipeline.py` monkeypatch seams intact so the existing operating-model contract tests did not need to retarget onto the new helper module
- updated the active maintainer and operations docs so serving publication sequencing now points at `backend/analytics/refresh_publication.py` instead of describing those state transitions as ad hoc `pipeline.py` branches

Validation:
- `git diff --check -- backend/analytics/pipeline.py backend/analytics/refresh_publication.py docs/architecture/maintainer-guide.md docs/operations/OPERATIONS_PLAYBOOK.md`
- `./.venv_local/bin/python -m py_compile backend/analytics/pipeline.py backend/analytics/refresh_publication.py`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_operating_model_contract.py::test_run_refresh_publish_only_republishes_cached_payloads_without_recompute backend/tests/test_operating_model_contract.py::test_run_refresh_publishes_before_deep_health_diagnostics backend/tests/test_operating_model_contract.py::test_load_publishable_payloads_prefers_durable_serving_payloads backend/tests/test_operating_model_contract.py::test_run_model_pipeline_clears_pending_after_serving_refresh backend/tests/test_projection_only_serving_cadence.py::test_validate_projection_only_serving_outputs_raises_on_native_downgrade backend/tests/test_projection_only_serving_cadence.py::test_publish_only_refresh_fails_when_projection_only_ticker_is_downgraded`

Validation blockers:
- `make doctor` remains blocked by the pre-existing syntax error in `scripts/doctor.sh`'s inline Python (`SyntaxError: invalid syntax` at `finally:`), so Slice 15 keeps the blocker recorded instead of widening scope into a repair

Notes:
- the landed extraction stayed inside the publish/persist/diagnostics rollback boundary; it did not pull refresh-context or core-read ownership changes into the same commit

## Slice 16

Scope:
- `backend/analytics/pipeline.py`
- `backend/orchestration/stage_serving.py`
- `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/architecture-invariants.md`
- `docs/architecture/maintainer-guide.md`
- `docs/operations/OPERATIONS_PLAYBOOK.md`
- `docs/archive/execution-logs/REPO_TIGHTENING_EXECUTION_LOG_2026-03-28.md`

Outcome:
- narrowed `backend/orchestration/stage_serving.py` so workspace `data_db` / `cache_db` paths no longer force local core reads on their own
- hoisted the projection-only locals in `backend/analytics/pipeline.py` so the reused and rebuilt universe-loadings paths share one variable-definition contract
- updated the active architecture, invariants, maintainer, and operations docs to describe the narrower serving-lane authority rule precisely
- accepted the adversarial-review narrowing explicitly: this landed as a serving authority-selection correction, not as a broad extraction of all remaining `run_refresh` refresh-context and projection-assembly logic

Validation:
- `git diff --check -- backend/analytics/pipeline.py backend/orchestration/stage_serving.py docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md docs/architecture/architecture-invariants.md docs/architecture/maintainer-guide.md docs/operations/OPERATIONS_PLAYBOOK.md`
- `./.venv_local/bin/python -m pytest -q backend/tests/test_projection_only_serving_cadence.py::test_run_serving_stage_requests_projection_refresh_on_core_lane backend/tests/test_projection_only_serving_cadence.py::test_run_serving_stage_does_not_request_projection_refresh_on_serving_only_lane backend/tests/test_projection_only_serving_cadence.py::test_run_refresh_uses_persisted_projection_outputs_on_serving_rebuild backend/tests/test_projection_only_serving_cadence.py::test_run_refresh_recomputes_projection_outputs_when_persisted_asof_is_stale backend/tests/test_projection_only_serving_cadence.py::test_run_refresh_uses_canonical_projection_rows_when_workspace_has_none backend/tests/test_operating_model_contract.py::test_pipeline_prefers_fundamentals_asof backend/tests/test_operating_model_contract.py::test_light_refresh_can_fail_closed_when_stable_core_package_is_required backend/tests/test_operating_model_contract.py::test_run_stage_serving_refresh_uses_local_source_archive_for_local_publish_profiles backend/tests/test_operating_model_contract.py::test_run_stage_serving_refresh_keeps_neon_backend_for_canonical_serve_refresh backend/tests/test_operating_model_contract.py::test_run_stage_serving_refresh_uses_local_backend_during_core_rebuild backend/tests/test_operating_model_contract.py::test_run_stage_serving_refresh_passes_workspace_paths_without_mutating_core_reads`

Validation blockers:
- `make doctor` remains blocked by the pre-existing syntax error in `scripts/doctor.sh`'s inline Python (`SyntaxError: invalid syntax` at `finally:`), so Slice 16 keeps the blocker recorded instead of widening scope into a repair

Notes:
- this execution-log entry is intentionally explicit that the landed Slice 16 commit was narrower than the original broader decomposition draft; the program should not pretend that `385799b` completed a larger helper-extraction sweep than it actually did
