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
- when touching default cUSE4 frontend imports, prefer the explicit cUSE4 surfaces:
  - `frontend/src/hooks/useCuse4Api.ts`
  - `frontend/src/lib/cuse4Api.ts`
  - `frontend/src/lib/types/cuse4.ts`
  - `frontend/src/lib/cuse4Truth.ts`
  - `frontend/src/lib/cuse4Refresh.ts`
  - `frontend/src/features/cuse4/components/*` for shared cUSE4 visual components

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

## Semantic Contract Rules

Canonical contract names and compatibility-alias rules are defined in `architecture-invariants.md`.

Use those canonical names in new code, docs, and UI labels.
Compatibility aliases may remain only as fallback readers and should not drive new semantics.
