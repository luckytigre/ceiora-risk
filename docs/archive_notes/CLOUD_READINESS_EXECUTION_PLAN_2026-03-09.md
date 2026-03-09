# Cloud Readiness Execution Plan

Date: 2026-03-08
Owner: Codex
Status: Active working plan

## Objective

Prepare the project for a cheap, low-traffic, scale-to-zero cloud deployment where:
- LSEG ingest remains local-only on Shaun's machine
- local SQLite remains the full historical authority
- Neon becomes the persistent cloud serving database
- the cloud backend becomes a lightweight read/write and orchestration surface
- the frontend remains highly observable and operator-friendly

This document converts the recent audit into an execution plan.

## Non-Goals

- Do not move LSEG pulls into the cloud.
- Do not expand the statistical model beyond the current agreed cUSE4 simplifications.
- Do not automate `universe-add` beyond operator/Codex workflow for now.

## Desired End State

### Local machine
- Runs LSEG ingest and any heavy historical backfills.
- Stores the full long-history SQLite database.
- Pushes bounded, audited outputs to Neon after approved runs.

### Neon
- Stores pruned canonical source tables.
- Stores pruned model output tables needed for serving.
- Stores all runtime holdings tables.
- Stores persistent serving-output tables or materialized payload tables needed by the cloud backend.

### Cloud backend
- Does not depend on local SQLite cache blobs.
- Reads persistent serving data from Neon.
- Accepts authenticated holdings edits and authenticated operator actions.
- Can sleep when idle and wake without requiring a fresh local rebuild.

### Frontend
- Shows trustworthy operator status.
- Polls sparingly.
- Uses on-demand refreshes and explicit operator actions instead of constant background chatter.

## Audit-Derived Priorities

### P0: Persistent serving independence
Problem:
- Some serving APIs still depend on local cache publication.

Required outcome:
- Portfolio, risk, exposures, universe, and related serving endpoints must be satisfiable from durable relational state, not transient local cache keys.

### P1: Auth and control-surface hardening
Problem:
- Refresh, holdings writes, and heavy diagnostics are too open for public hosting.

Required outcome:
- All mutating or expensive operator endpoints require explicit authentication and authorization policy.

### P2: Scale-to-zero request discipline
Problem:
- Frontend polling and status recomputation are too chatty.

Required outcome:
- Backend/operator state should be cheap to read and frontend polling should be sparse, conditional, and mostly manual.

### P3: Runtime role separation
Problem:
- Local ingest and cloud serving are still separated mostly by env toggles.

Required outcome:
- The system must have an explicit runtime-role model: `local-ingest` vs `cloud-serve`.

### P4: Codebase simplification before cutover
Problem:
- A few critical modules and pages are too broad and mix responsibilities.

Required outcome:
- File/module boundaries should match deployment responsibilities and be easier to maintain during cutover.

## Execution Phases

## Phase 1: Persistent serving outputs in Neon

Goal:
- Remove cloud-serving dependence on local cache blobs.

Implementation:
1. Define canonical serving tables in Neon for frontend payloads or their relational equivalents.
2. Persist the outputs currently served from SQLite cache into those tables during refresh publish.
3. Refactor serving endpoints to read from Neon-backed persisted outputs first.
4. Preserve local cache only as a local optimization, not as the serving authority.
5. Add readiness checks proving that cloud-serving endpoints can boot correctly from Neon alone.

Current implementation note:
- Durable serving payload persistence is implemented in `serving_payload_current`.
- Refresh publish now writes the active serving payload set into both SQLite and Neon.
- In `cloud-serve` mode, serving reads are effectively durable/Neon-first even if `SERVING_OUTPUTS_PRIMARY_READS` is unset.
- `SERVING_OUTPUTS_PRIMARY_READS=true` remains useful for staged local rehearsal.

Candidate tables/views:
- `serving_portfolio_snapshot`
- `serving_risk_snapshot`
- `serving_exposures_snapshot`
- `serving_universe_snapshot`
- `serving_operator_snapshot`

Acceptance criteria:
- Cloud backend can restart cold and still serve portfolio/risk/exposures/universe without `not_ready` from missing local cache.
- Data page shows active serving snapshot id and source timestamps from persistent state.

## Phase 2: Auth and operator hardening

Goal:
- Make cloud writes and expensive controls safe.

Implementation:
1. Require an auth layer for all mutating endpoints:
   - holdings edit/import/remove
   - refresh trigger
   - profile trigger
   - heavy diagnostics actions
2. Require auth for expensive operator-read endpoints if needed.
3. Fail closed by default:
   - if auth config is missing in cloud mode, mutating endpoints reject requests.
4. Split permissions into at least two roles:
   - operator: refresh, diagnostics, destructive actions
   - editor: holdings edits/imports
5. Add explicit UI behavior for unauthorized states.
6. Add audit logging fields to mutating actions:
   - actor
   - action
   - account_id when relevant
   - request timestamp

Recommended near-term policy for your use case:
- simple shared secret or session gate is sufficient initially
- do not build a full enterprise auth system yet
- but do not leave endpoints open on the public internet

Current implementation note:
- Cloud-mode backend routes already fail closed for:
  - refresh trigger / refresh status
  - holdings import/edit/remove
  - expensive diagnostics
- Current shared-token roles are:
  - `OPERATOR_API_TOKEN`
  - `EDITOR_API_TOKEN`
  - legacy `REFRESH_API_TOKEN` as operator fallback
- Next server routes proxy these actions and inject the backend token server-side.

Acceptance criteria:
- Public unauthenticated requests cannot edit holdings or trigger refresh.
- Cloud deployment fails safely if required auth config is absent.

## Phase 3: Request-discipline and observability refinement

Goal:
- Lower idle cost and reduce unnecessary Neon/backend traffic.

Implementation:
1. Replace duplicate status surfaces with one canonical operator-status path.
2. Publish operator snapshots at refresh completion instead of recomputing everything per request.
3. Reduce global polling:
   - header health signal should poll slowly or on visibility/focus only
   - fast polling should happen only during an active run
4. Debounce search/typeahead requests.
5. Collapse redundant frontend revalidation after holdings edits.
6. Make Data page the primary operator surface; simplify or downgrade duplicate operator cards elsewhere.

Current implementation note:
- `TabNav` and the operator control deck now share the canonical operator-status path.
- Idle polling is slower and only accelerates during active refresh execution.
- Deep diagnostics remain operator-triggered instead of part of the steady-state polling loop.

Acceptance criteria:
- Idle browsing produces minimal backend traffic.
- Operator status reads are cheap enough to support intermittent scale-to-zero use.

## Phase 4: Runtime-role separation

Goal:
- Make local-ingest and cloud-serve explicit operating modes.

Implementation:
1. Introduce a runtime role setting:
   - `APP_RUNTIME_ROLE=local-ingest`
   - `APP_RUNTIME_ROLE=cloud-serve`
2. In `local-ingest` role:
   - LSEG ingest allowed
   - local SQLite authority active
   - publish/mirror allowed
3. In `cloud-serve` role:
   - LSEG ingest disabled regardless of other flags
   - serving endpoints and holdings writes enabled
   - refresh lanes limited to safe cloud-side operations
4. Reflect runtime role in `/api/operator/status` and the Data page.
5. Add tests proving ingest cannot run in cloud-serve mode.

Current implementation note:
- `APP_RUNTIME_ROLE` is live with:
  - `local-ingest`
  - `cloud-serve`
- `cloud-serve` now:
  - hard-blocks LSEG ingest
  - restricts refresh lanes to `serve-refresh`
  - treats durable serving payloads as the serving authority
  - skips broad Neon mirror/parity/prune sweeps meant for local publish runs
- `local-ingest` remains the broad source/model publisher into Neon.

Acceptance criteria:
- A cloud deployment cannot accidentally run local LSEG ingest.
- Local operator machine can still run full workflows.

## Phase 5: Codebase boundary cleanup

Goal:
- Reduce maintenance risk before full cutover.

Implementation:
1. Split `backend/data/postgres.py` into clearer modules, for example:
   - `backend/data/core_reads.py`
   - `backend/data/neon_reads.py`
   - `backend/data/sqlite_reads.py`
2. Refactor large frontend pages into feature modules/hooks/components:
   - positions
   - explore
   - health/data operator surfaces
3. Separate serving publication code from core analytics computation.
4. Keep route handlers thin and move business logic into service modules.
5. Align module names with actual responsibility.

Acceptance criteria:
- Critical cloud-cutover code paths are easier to reason about and test.

## Phase 6: Cloud cutover rehearsal

Goal:
- Prove the cloud serving path works before real dependency on it.

Implementation:
1. Run local ingest as normal.
2. Publish bounded outputs to Neon.
3. Start backend in cloud-serve mode against Neon only.
4. Validate:
   - frontend loads after cold start
   - holdings edits work
   - operator status works
   - refresh semantics are safe and expected
5. Record one operator runbook for:
   - local daily source update
   - local weekly core update
   - cloud holdings edit + recalc
   - cloud diagnostics check

Acceptance criteria:
- End-to-end cold-start rehearsal works without local SQLite cache dependency.

## Recommended Control Model

### Local-only actions
- LSEG ingest
- broad historical backfills
- universe-add execution
- deep source repairs
- cold-core after structural data change

### Cloud-safe actions
- holdings edits/imports/removes
- serve-refresh
- safe operator checks
- read-only diagnostics summaries
- viewing risk/exposures/explore/dashboard outputs

### Cloud-conditional actions
- `core-weekly` only if you intentionally decide cloud compute should own it later
- until then, prefer local execution with Neon publish afterward

## Frontend Observability Requirements

The Data page should remain the primary operator cockpit and must show:
- runtime role (`local-ingest` vs `cloud-serve`)
- latest successful source update
- latest successful core update
- latest successful serving refresh
- active serving snapshot id
- Neon mirror/parity status
- holdings dirty state
- pending recalculation indicator
- auth/config warning state
- plain-English descriptions for each lane/action

## Documentation Updates Required During Execution

The following docs should stay synchronized with the implementation:
- `docs/OPERATING_MODEL_PLAN.md`
- `docs/OPERATIONS_PLAYBOOK.md`
- `docs/specs/cUSE4_engine_spec.md`
- `docs/migrations/neon/NEON_MIGRATION_EXECUTION_PLAN.md`
- `docs/migrations/neon/NEON_STAGE1_OPERATOR_RUNBOOK.md`

## Suggested Order of Work

1. Phase 1: persistent Neon-backed serving outputs
2. Phase 2: auth and operator hardening
3. Phase 3: request-discipline and operator snapshot publication
4. Phase 4: runtime-role separation
5. Phase 5: codebase boundary cleanup
6. Phase 6: cloud cutover rehearsal

## Definition of Ready for Full Cloud Use

The project is ready for full cloud serving when all of the following are true:
- cloud backend can cold-start and serve from Neon without local cache rebuild
- all mutating/expensive endpoints are authenticated and fail closed
- frontend idle traffic is low enough for scale-to-zero economics
- runtime role prevents accidental LSEG/cloud mixing
- Data page accurately reflects operator reality
- local-to-Neon publish flow is documented and repeatable
