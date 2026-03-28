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
- a follow-up docs-only clarification commit is required because `/cpar/explore` can hand staged deltas into the shared holdings apply surface even though cPAR itself still does not own a mutation route
