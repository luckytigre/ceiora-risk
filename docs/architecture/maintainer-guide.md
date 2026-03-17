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

## Where New Code Should NOT Go

- do not put cross-store truth assembly in routes
- do not put UI semantics in `backend/data`
- do not put workflow-policy branching in domain math modules
- do not add `shared.py`, `common.py`, or vague `*manager.py` files
- do not move code between packages just for visual tidiness

## Workflow Structure

### Refresh / rebuild

Use:
- `refresh_manager` for process-local refresh lifecycle
- `run_model_pipeline` and `backend/orchestration/*` for staged rebuild workflows

Do not:
- mutate module globals to retarget one run
- hide stage behavior in unrelated helper modules
- let serving-only paths synthesize or advance core artifacts when the stable core package is stale or missing

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
- top-level Health summary metrics should come from persisted core metadata; if a core metric like `latest_r2` is missing, render it as unavailable rather than coercing it to `0`.
- deep Health regression diagnostics are still a deferred legacy path; if their R² series is empty, label that state explicitly instead of implying a real zero-fit signal.

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
