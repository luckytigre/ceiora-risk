# Model Families And Ownership

Date: 2026-03-18
Status: Active architecture note
Owner: Codex

## Purpose

This document explains how the repository currently hosts two model families:

- `cUSE4`: the existing default risk system
- `cPAR`: the new explicitly namespaced parallel system

It exists to remove ambiguity around file organization and to explain why `cPAR` currently appears more visibly namespaced than `cUSE4`.

## Current Rule

The repository is not organized as two perfectly mirrored top-level products.

Instead:

- `cUSE4` remains the incumbent/default system, so many integration surfaces still appear as the repo's baseline app behavior
- `cPAR` is being added later as a parallel model family, so its new surfaces are intentionally namespaced from the start

This asymmetry is acceptable in the current phase of the project.

## cUSE4 Ownership

### Pure model and estimation logic

Primary home:

- `backend/risk_model/*`

Examples:

- factor catalogs and descriptors
- eligibility
- regression-frame preparation
- factor returns
- covariance and specific risk logic
- projection-only returns-based ETF outputs derived from the cUSE4 core package

### cUSE4 integration surfaces

Current cUSE4 integration is still the default app wiring in several places, including:

- `backend/analytics/*`
- `backend/services/*`
- `backend/api/routes/*`
- `frontend/src/app/cuse/*`
- legacy route redirects in `frontend/next.config.js` for:
  - `/exposures`
  - `/explore`
  - `/health`
- shared frontend helpers such as `frontend/src/lib/analyticsTruth.ts`

These surfaces are not "generic factor-model abstractions."
They are the current cUSE4-first application surfaces unless explicitly documented otherwise.

Preferred cUSE4 frontend import surfaces now include:

- `frontend/src/hooks/useCuse4Api.ts`
- `frontend/src/lib/cuse4Api.ts`
- `frontend/src/lib/types/cuse4.ts`
- `frontend/src/lib/cuse4Truth.ts`
- `frontend/src/lib/cuse4Refresh.ts`
- `frontend/src/features/cuse4/components/*` for shared visual components used by the default cUSE4 pages/features

Transitional mixed-family compatibility files still exist:

- `frontend/src/hooks/useApi.ts`
- `frontend/src/lib/api.ts`
- `frontend/src/lib/types.ts`
- `frontend/src/lib/analyticsTruth.ts`
- `frontend/src/lib/refresh.ts`

These compatibility files should not be the default import path for new cUSE4 work.

Current default-named cUSE4 route family includes:

- `/api/exposures`
- `/api/risk`
- `/api/portfolio`
- `/api/universe/*`
- `/api/portfolio/whatif`

Explicit cUSE4 backend alias modules may sit beside these defaults to make ownership clearer without changing the user-facing route family.
Examples:

- `backend/services/cuse4_dashboard_payload_service.py`
- `backend/services/cuse4_universe_service.py`
- `backend/services/cuse4_portfolio_whatif.py`
- `backend/services/cuse4_factor_history_service.py`
- `backend/services/cuse4_health_diagnostics_service.py`
- `backend/services/cuse4_holdings_service.py`
- `backend/services/cuse4_operator_status_service.py`

For dashboard, universe, factor-history, health diagnostics, holdings, and portfolio what-if, those `cuse4_*` modules are now the concrete route-facing owners.
The legacy default-named modules remain compatibility shims for older callers and direct service tests. Those shims preserve the documented public import surface, not arbitrary internal helper or monkeypatch seams.

Current cUSE4 frontend page family now resolves under:

- `/cuse/exposures`
- `/cuse/explore`
- `/cuse/health`
- shared global `/positions`

Current shared holdings surface:

- `/positions` remains an intentional shared live-holdings control surface rather than a cUSE-only or cPAR-only page
- shared holdings reads/writes on that page should prefer `frontend/src/hooks/useHoldingsApi.ts` and `frontend/src/lib/holdingsApi.ts`
- cUSE participation there is explicit and read/write:
  - operator/control chrome
  - holdings publish + refresh flow
  - modeled snapshot and truth summary via `frontend/src/hooks/useCuse4Api.ts`
- cPAR participation there is explicit and read-only:
  - method/coverage overlay sourced through `frontend/src/hooks/useCparApi.ts`
- do not hide that mixed ownership behind the transitional mixed-family barrels

Legacy redirects remain in place from:

- `/exposures`
- `/explore`
- `/health`

Those legacy root redirects are owned in `frontend/next.config.js` only.
The root legacy compatibility entrypoints should remain redirect-only and should not grow separate live page implementations or duplicate App Router pages.

## cPAR Ownership

### Pure model and estimation logic

Primary home:

- `backend/cpar/*`

This package is intentionally limited to pure cPAR domain/model logic.
It must not become a second full backend stack.

### cPAR integration surfaces

Integration stays in the repo's normal layers, but with explicit cPAR naming:

- `backend/data/cpar_*`
- `backend/services/cpar_*`
- `backend/api/routes/cpar.py`
- `backend/orchestration/cpar_*`
- `frontend/src/app/cpar/*`
- `frontend/src/features/cpar/*`
- `frontend/src/hooks/useCparApi.ts`
- `frontend/src/lib/cparApi.ts`
- `frontend/src/lib/cparTruth.ts`
- `frontend/src/lib/types/cpar.ts`

Preferred cPAR frontend import surfaces now include:

- `frontend/src/hooks/useCparApi.ts`
- `frontend/src/lib/cparApi.ts`
- `frontend/src/lib/types/cpar.ts`
- shared `frontend/src/hooks/useHoldingsApi.ts` and `frontend/src/lib/holdingsApi.ts` only where cPAR intentionally reuses shared holdings/account plumbing
- shared `frontend/src/lib/types/holdings.ts` only where cPAR intentionally reuses shared holdings/account plumbing
- `frontend/src/lib/apiTransport.ts` only for neutral low-level fetch/error handling

The mixed-family compatibility barrels:

- `frontend/src/hooks/useApi.ts`
- `frontend/src/lib/api.ts`
- `frontend/src/lib/types.ts`

may remain for compatibility, but they should not be the default import path for cPAR-owned frontend code after the current cleanup slice.
They also should not be the owner of shared holdings/account reuse inside cPAR surfaces; that shared plumbing should stay explicit through the holdings owners above.

## Why The Asymmetry Exists

`cUSE4` predates `cPAR` in this repository and already defines the default operating model, frontend pages, and backend serving surfaces.

`cPAR` is being introduced deliberately beside it, so:

- new cPAR files should be namespaced clearly
- existing cUSE4 files should not be moved only for visual symmetry
- broad cUSE4 file moves should happen only when there is a real ownership or maintenance payoff

Do not run a large cUSE4 namespace migration just to make the tree look more symmetric.

## Practical Guidance

### When adding cUSE4 work

- treat existing default risk pages/services/routes as cUSE4-owned unless a document says otherwise
- keep pure cUSE4 math in `backend/risk_model/*`
- only introduce more explicit `cuse4` naming if you are already touching a surface for substantive reasons

### When adding cPAR work

- keep pure cPAR logic in `backend/cpar/*`
- keep cPAR integration surfaces explicitly namespaced
- do not reuse cUSE4 payload names, runtime-state keys, or factor definitions

## Near-Term Guidance

For the current phase:

- preserve the existing cUSE4 layout
- document ownership clearly
- prefer explicit cUSE4 import surfaces for default frontend/backend integration work
- let cPAR stay explicitly namespaced
- avoid a broad cUSE4 restructuring while cPAR is still stabilizing end to end

If the repo later accumulates substantial parallel cUSE4 integration work, it may become worth introducing clearer `cuse4` integration namespaces at that time.

That is a future cleanup decision, not a current requirement.
