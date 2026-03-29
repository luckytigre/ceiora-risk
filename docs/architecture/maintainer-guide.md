# Maintainer Guide

Date: 2026-03-16
Status: Active maintainer guide
Owner: Codex

## High-Level Shape

The repository is organized around five backend layers:

- `backend/api`: transport entrypoints
- `backend/services`: application-facing payload and mutation surfaces
- `backend/orchestration`: refresh and rebuild workflows
- `backend/analytics`, `backend/risk_model`, `backend/universe`, `backend/portfolio`: reusable domain and compute logic
- `backend/data`: persistence and provider-specific adapters

Frontend pages should read a small number of backend surfaces and rely on shared freshness/truth helpers rather than rebuilding semantics locally.

Model-family ownership is documented separately in `MODEL_FAMILIES_AND_OWNERSHIP.md`.
Use that note when deciding whether a surface is currently cUSE4-owned by default or should be explicitly namespaced for cPAR.

## Local Environment

Use a single root virtualenv for local work:

- bootstrap with `make setup` or `./scripts/setup_local_env.sh`
- activate with `source .venv_local/bin/activate`
- the local app scripts and backend commands assume `.venv_local`
- the repository is standardized on Python `3.14.x`
- `make doctor` verifies `.venv_local`, core backend imports, whether `lseg.data` is importable in that environment, and that the registry-first current-state surfaces plus `security_master_compat_current` are present and structurally sane in the local workspace
- install the real LSEG runtime into `.venv_local` when you need ingest/rebuild lanes; the backend package extra is `.[lseg]`, and `./scripts/setup_local_env.sh` also attempts `pip install lseg-data`

## Where New Code Should Go

### Add to `backend/api`

Only when you are changing transport behavior:
- request parsing
- auth
- response translation

### Add to `backend/services`

When one API/UI surface needs a coherent application-facing payload or mutation flow.

Examples:
- operator truth
- dashboard payload serving
- holdings mutations
- diagnostics payload assembly

### Add to `backend/orchestration`

When the change affects:
- job sequencing
- stage planning
- stage execution
- post-run publication/reporting

### Add to `backend/data`

When the change is about:
- SQLite / Neon persistence
- stable data-product surfaces
- schema maintenance
- provider-specific read/write behavior

## Model Families

Current repo reality:

- `cUSE4` is still the incumbent/default risk system across many integration surfaces
- `cPAR` is the new explicitly namespaced parallel system

Practical rule:

- do not move existing cUSE4 files only to make the tree look symmetric with cPAR
- do make ownership explicit in docs and in new namespaced cPAR surfaces
- when touching pure cUSE4 model logic, keep it in `backend/risk_model/*`
- when touching pure cPAR model logic, keep it in `backend/cpar/*`
- when touching cPAR integration, keep it in normal repo layers with `cpar_*` naming
- when touching aggregate cPAR risk/explore snapshot assembly, keep the split explicit:
  - `backend/services/cpar_risk_service.py` is the thin route-facing owner for `GET /api/cpar/risk`
  - `backend/services/cpar_aggregate_risk_service.py` is the explicit aggregate package-pinned snapshot owner
  - `backend/services/cpar_portfolio_snapshot_service.py` stays the shared support/core owner below those route-facing owners
- when touching account-scoped cPAR hedge/what-if snapshot assembly, keep the split explicit:
  - `backend/services/cpar_portfolio_hedge_service.py` is the explicit route-facing hedge payload owner
  - `backend/services/cpar_portfolio_snapshot_service.py` stays the shared account-context/support/helper-core owner below both the hedge and what-if services
  - `backend/services/cpar_portfolio_account_snapshot_service.py` owns the shared account-scoped hedge snapshot builder below those services
  - `backend/services/cpar_portfolio_snapshot_service.py::build_cpar_portfolio_hedge_snapshot()` is compatibility only while callers migrate; do not grow new logic there
  - `backend/services/cpar_portfolio_whatif_service.py` should keep one package/context/support-row set for `current` and `hypothetical` instead of rereading through the hedge route
- when touching universe runtime assembly, keep the split explicit:
  - `backend/universe/runtime_authority.py` owns current-table authority loading for registry/policy/taxonomy/source-observation rows
  - `backend/universe/runtime_rows.py` still owns compat/legacy fallback, historical classification resolution, mixed-state policy/structural resolution, candidate-RIC selection, and the public runtime-row loaders
- when touching default cUSE4 frontend imports, prefer the explicit cUSE4 surfaces:
  - `frontend/src/hooks/useCuse4Api.ts`
  - `frontend/src/lib/cuse4Api.ts`
  - `frontend/src/lib/types/cuse4.ts`
  - `frontend/src/lib/cuse4Truth.ts`
  - `frontend/src/lib/cuse4Refresh.ts`
  - `frontend/src/features/cuse4/components/*` for shared cUSE4 visual components
- when touching intentionally shared holdings/account frontend plumbing, prefer:
  - `frontend/src/hooks/useHoldingsApi.ts`
  - `frontend/src/lib/holdingsApi.ts`
  - `frontend/src/lib/types/holdings.ts`
  - `frontend/src/lib/apiTransport.ts` only for neutral low-level transport/error handling
- when touching the shared `/positions` surface, keep the split explicit:
  - holdings reads/writes from the shared holdings owners above
  - cUSE-only modeled snapshot/control reads from `frontend/src/hooks/useCuse4Api.ts`
  - cPAR read-only method overlays from `frontend/src/hooks/useCparApi.ts`
- when touching the default cUSE dashboard/universe/factor-history/health/holdings/portfolio-whatif service surfaces, prefer:
  - `backend/services/cuse4_dashboard_payload_service.py`
  - `backend/services/cuse4_universe_service.py`
  - `backend/services/cuse4_factor_history_service.py`
  - `backend/services/cuse4_health_diagnostics_service.py`
  - `backend/services/cuse4_holdings_service.py`
  - `backend/services/cuse4_portfolio_whatif.py`
  - treat the legacy default-named modules as compatibility shims unless a cleanup slice is explicitly removing them
  - do not depend on undocumented legacy helper globals or monkeypatch seams surviving those shims

## Where New Code Should NOT Go

- do not put cross-store truth assembly in routes
- do not put UI semantics in `backend/data`
- do not put workflow-policy branching in domain math modules
- do not add `shared.py`, `common.py`, or vague `*manager.py` files
- do not move code between packages just for visual tidiness

## Workflow Structure

### Refresh / rebuild

Use:
- `refresh_control_service` for the application-facing refresh control path used by routes and control clients
- `refresh_manager` for process-local refresh lifecycle inside the local compatibility surface
- `refresh_status_service` for read-only persisted refresh-status reads from serve-facing/operator-facing surfaces that do not own the worker
- `refresh_dispatcher` for runtime-aware “request serve-refresh” behavior when a mutation flow may or may not be allowed to start refresh locally
- `backend/ops/cloud_run_jobs.py` for provider-specific Cloud Run Jobs dispatch
- `run_model_pipeline` and `backend/orchestration/*` for staged rebuild workflows

Do not:
- mutate module globals to retarget one run
- hide stage behavior in unrelated helper modules
- let serving-only paths synthesize or advance core artifacts when the stable core package is stale or missing
- let a serve-only process reconcile shared refresh state as though it owned the control-plane worker

### App surfaces

Current approved entrypoints:

- `backend.main:app`
  - full local/all-in-one compatibility surface
- `backend.serve_main:app`
  - stateless serving surface
- `backend.control_main:app`
  - operator/control surface

Frontend split-origin proxy ownership lives in:
- `frontend/src/app/api/_backend.ts`
- the operator/control App Router proxy handlers under `frontend/src/app/api/*`

Pages/components should not choose backend origins directly.

### Serving

Prefer:
- route -> service -> serving/runtime/data surface

Avoid:
- route -> many data adapters directly
- serving-time writes into canonical historical source tables

### Core Cadence

Treat these as different timelines:

- weekly stable core package
  - factor returns
  - covariance
  - specific risk
  - estimation basis metadata
- daily serving/projection
  - holdings
  - prices used for serving
  - current loadings
  - portfolio outputs
- PIT source timeline
  - fundamentals
  - classifications

Rule:
- `serve-refresh` may read and project against the stable core package, but it may not compute, persist, or advance that package.
- canonical historical price writes belong only to approved ingest/history paths, not serving-time logic.
- projection-only ETF outputs are derived from the stable core package, but they are not native core artifacts. Refresh them on core lanes, persist them, and let serving read them as a durable surface.
- `projection_asof` should track the active `core_state_through_date`, not an incidental overlap date.
- `serving_refresh` progress should be observable at the substage level. Keep publish/persist milestones, diagnostics-section heartbeats, and finished-stage timing summaries intact rather than collapsing them into one terminal message.
- Universe-loadings reuse keys must be based on the current serving snapshot's source dates. Do not let stale eligibility metadata advance `exposures_latest_available_asof` and accidentally force rebuilds.
- Current v1 projection-only estimation is intentionally plain OLS with residual-variance-based projected specific risk. Do not introduce intercept/EWLS/outlier changes unless there is concrete evidence the current method is materially wrong.
- detailed operating semantics for refresh lanes, health surfaces, and retention live in `../operations/OPERATIONS_PLAYBOOK.md`.

## Common Drift Mistakes

1. Putting convenience SQL or cache reads back into routes.
2. Adding one more helper branch to a facade instead of putting it in the extracted helper module that already owns that concern.
3. Pulling orchestration metadata from a full job module when a narrower source already exists.
4. Creating a generic new module because the correct owner feels slightly inconvenient.

## Safe Extension Rule

When extending the system:
- start at the owning surface
- preserve stable facades when they already exist
- add only the smallest new helper module that creates a clearer boundary
- update the architecture docs when ownership materially changes

## Cleanup Execution Protocol

During repo-tightening work:

- keep one rollback-safe cleanup slice per commit
- treat the dirty worktree as hostile context and do not absorb unrelated files into a slice
- write a slice note before editing with exact in-scope files, explicit out-of-scope files, required doc updates, validation commands, and the rollback boundary
- run at least two adversarial pre-edit reviews for every slice; add a post-edit review before commit when the slice touches runtime authority, serving behavior, or orchestration
- use the smallest meaningful validation bundle for the touched surface and split the slice again if narrow validation is not possible
- for docs-only or hygiene-only slices, keep validation to `git diff --check -- <touched paths>` plus directly relevant static checks; do not run backend/runtime gates unless executable behavior changed
- keep repo-hygiene ignore rules root-anchored and concrete so the cleanup does not hide legitimate source artifacts
- when a backend route test only needs to override one service call, prefer a route-level callable seam or public injected service kwargs over monkeypatching alias-module globals
- when route tests need to isolate cUSE alias owners, patch the alias module's public dependency seam function rather than mutating several alias-module globals directly

## Semantic Contract Rules

Canonical contract names and compatibility-alias rules are defined in `architecture-invariants.md`.

Use those canonical names in new code, docs, and UI labels.
Compatibility aliases may remain only as fallback readers and should not drive new semantics.
