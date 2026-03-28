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

Decision:
- `/positions` remains an intentional shared live-holdings control surface

Outcome:
- made the shared/family split explicit on `/positions` and the related holdings helpers:
  - shared holdings reads and mutations now stay on the shared holdings owners
  - cUSE participation remains explicit for modeled snapshot and operator/control semantics
  - cPAR participation remains explicit and read-only for method/coverage overlays
- added stable `data-testid` anchors for the positions surface and holdings ledger
- added `frontend/scripts/positions_surface_smoke.mjs` as a source-contract smoke for the shared-owner decision
- updated the active architecture and operations docs so `/positions` is documented as shared rather than implicitly cUSE-owned

Validation:
- `git diff --check -- frontend/src/app/positions/page.tsx frontend/src/features/holdings/components/HoldingsLedgerSection.tsx frontend/src/features/holdings/components/HoldingsImportPanel.tsx frontend/src/features/holdings/components/ManualPositionEditor.tsx frontend/src/features/holdings/hooks/useHoldingsManager.ts frontend/scripts/positions_surface_smoke.mjs docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md docs/architecture/MODEL_FAMILIES_AND_OWNERSHIP.md docs/architecture/dependency-rules.md docs/architecture/maintainer-guide.md docs/architecture/CPAR_FRONTEND_SURFACES.md docs/operations/OPERATIONS_HARDENING_CHECKLIST.md docs/operations/CPAR_OPERATIONS_PLAYBOOK.md`
- `cd frontend && node scripts/positions_surface_smoke.mjs`

Validation blockers:
- `cd frontend && npm run typecheck` progressed past `next typegen`, then stalled in the later typecheck phase until manual termination
- `cd frontend && npm run test:cpar-portfolio` timed out after 60s under the bounded runner
- `cd frontend && npm run test:cpar-portfolio-whatif` exited `-15` under the same bounded runner after the portfolio timeout sequence
- `cd frontend && npm run test:cpar-hedge` exited `-15` under the same bounded runner after the portfolio timeout sequence

Notes:
- the Slice 3 study confirmed there was no dedicated positions-focused smoke before this change
- `/positions` remains intentionally separate from the cUSE and cPAR page families in navigation; this slice did not split the page back into family-owned surfaces
- a follow-up docs-only clarification commit is required because `/cpar/explore` can hand staged deltas into the shared holdings apply surface even though cPAR itself still does not own a mutation route
