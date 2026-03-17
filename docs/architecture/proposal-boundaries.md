# Proposal A: Boundary Architect

Date: 2026-03-16
Perspective: Boundary Architect
Goal: stable module boundaries and obvious dependency direction

## Core Opinion

The repository does not need a new grand framework. It needs sharper package boundaries and stricter dependency direction.

The main failure mode today is cross-layer leakage:
- routes sometimes assemble truth directly
- services import orchestration details
- data modules carry policy
- orchestration knows too much implementation detail

The recommended architecture is a small set of explicit layers:

1. Entrypoints
   - FastAPI routes
   - CLI scripts
   - local-app scripts

2. Application Services
   - route-facing composed payload/services
   - refresh manager
   - holdings mutation service
   - operator/data/dashboard service surfaces

3. Workflows / Jobs
   - orchestration profiles
   - stage planning
   - stage execution
   - post-run publication and reporting

4. Domain / Compute
   - risk-model math
   - universe logic
   - portfolio calculations
   - reusable analytics builders

5. Infrastructure / Adapters
   - Neon and SQLite stores
   - serving outputs
   - runtime state
   - job runs
   - sync/mirror adapters

## Recommended Dependency Direction

- entrypoints may depend on application services and workflow entry APIs
- application services may depend on domain/compute and infrastructure adapters
- workflows may depend on domain/compute and infrastructure adapters
- domain/compute may depend only on narrow adapters, contracts, and internal math utilities
- infrastructure must not depend on routes or frontend-driven concerns

In practice:
- `backend/api/routes/*` should not import deep analytics and data modules directly when a service exists
- `backend/services/*` should not need to import the full orchestrator merely to inspect profile metadata
- `backend/orchestration/*` should not depend on route code or UI-facing semantics

## Package Recommendations

### Keep

- `backend/api`
- `backend/analytics`
- `backend/data`
- `backend/orchestration`
- `backend/risk_model`
- `backend/services`
- `backend/universe`
- `backend/portfolio`

### Restructure Within Those Packages

#### `backend/api`

Own only:
- HTTP auth
- route parameter validation
- delegation to services
- small presenter helpers

Do not own:
- durable-truth assembly
- readiness rules beyond thin route-level translation
- domain normalization that other surfaces need

#### `backend/services`

Treat `services/` as the application-service layer.

Subdivide conceptually into:
- operator and diagnostics surfaces
- dashboard-serving surfaces
- holdings/use-case surfaces
- refresh control surfaces

Avoid turning this into a generic “shared” layer.

#### `backend/orchestration`

Split into:
- `profiles.py`
- `stage_planning.py`
- `stage_runner.py`
- `post_run_publish.py`
- thin `run_model_pipeline.py`

The orchestrator should become a workflow shell, not the only place where workflow concepts exist.

#### `backend/data`

Split by store surface, not by technology:
- source reads
- model outputs
- serving outputs
- runtime state
- job runs
- neon/sqlite connection helpers

`core_reads.py` should be split because it is both an adapter selector and a domain query library.

## Public vs Internal Modules

Define public surfaces by convention:

- public route-facing services:
  - `operator_status_service`
  - `data_diagnostics_service`
  - future `dashboard_payload_service`
- public workflow entrypoints:
  - `run_model_pipeline`
  - refresh-manager start functions
- public durable stores:
  - `serving_outputs`
  - `runtime_state`
  - `job_runs`

Everything else should be treated as internal package implementation unless explicitly documented.

## Main Advantages

- easier reasoning about dependency direction
- better test seams
- lower chance of route-driven drift
- fewer accidental imports into the wrong layer

## Tradeoff

This approach adds a little more module surface area. That is acceptable only if each new module has sharp ownership and reduces coupling immediately.

## First Changes This Perspective Would Prioritize

1. Extract orchestration profiles out of `run_model_pipeline.py`
2. Introduce a dashboard-serving service so routes stop owning serving-truth assembly
3. Split `core_reads.py` into backend-selection plumbing and domain query helpers
