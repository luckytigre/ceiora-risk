# Neon-Authoritative Rebuild Plan

Date: 2026-03-15
Owner: Codex
Status: Active migration plan

Execution companion:
- `docs/NEON_STANDALONE_EXECUTION_PLAN.md` is the concrete implementation checklist, checkpoint plan, and review protocol for executing this migration end to end.

## Objective

Make Neon the authoritative rebuild and runtime database for the standalone tool, while keeping local SQLite as the private LSEG ingest/archive reservoir.

Plain English:
- local SQLite is the private loading dock and deep warehouse
- Neon is the public warehouse and the real operating system of the app
- LSEG lands locally first because only the local machine has access
- after ingest, the authoritative working dataset must be pushed into Neon
- all normal rebuilds and all serving reads should then use Neon
- local SQLite remains useful only for ingest, archival optionality, and deliberate retention expansion

This is a different contract than the current codebase, which still treats local SQLite as the authority for some core rebuild paths.

## Target Operating Contract

### 1. Storage roles

`local SQLite`
- purpose: local-only LSEG ingest landing zone and deep archive
- authority for: raw upstream acquisition and optional history extension
- not the normal runtime serving authority
- not the normal core rebuild authority once Neon is current

`Neon`
- purpose: authoritative standalone runtime and rebuild database
- authority for: source tables after publish, retained raw history, retained factor returns, covariance, specific risk, holdings, serving payloads
- all normal rebuilds should use Neon
- all dashboard/runtime reads should use Neon-backed truth surfaces

### 2. Rebuild rule

All non-ingest rebuilds should run from Neon, not from local SQLite.

Implication:
- before a core rebuild can start, Neon must already be current enough for that rebuild window
- widening the rebuild horizon means widening Neon retention first, then rebuilding
- local SQLite is no longer part of the ordinary rebuild truth path

### 3. Publish rule

Any successful source ingest or core rebuild must publish the retained working window into Neon and update parity evidence.

### 4. Failure rule

If a mode depends on Neon-authoritative rebuild data and Neon is missing required rows, columns, or retention depth, the mode should fail closed with a clear operator error.

## Current State Summary

Today the repo already has important pieces:
- Neon-primary serving payload reads are in place
- local SQLite remains the full archive
- Neon mirror/parity/prune infrastructure exists
- runtime/operator surfaces already expose Neon sync health
- core read routing already has a `core_read_backend()` override seam

But the profile contract is still mixed:
- `source-daily` is the ingest-capable profile
- `core-weekly` still does not prepend direct LSEG ingest by itself
- `cold-core` still does not prepend direct LSEG ingest by itself
- local ingest/core publish flows still deliberately force some reads to local SQLite
- `ORCHESTRATOR_ENABLE_INGEST` can currently disable real upstream ingest entirely

That means the current system is close to Neon-authoritative serving, and now has the first real Neon-authoritative rebuild slice in place:
- `source_sync` now publishes source tables into Neon before Neon-backed source/core lanes proceed
- `neon_readiness` now validates Neon depth/coverage and builds a scratch SQLite workspace from Neon for the existing core math
- Neon-authoritative core runs now fail closed if the local archive is older than Neon or if Neon retention is too shallow
- rebuilt derived outputs are mirrored back into Neon and then copied back into the local private mirror for congruence

Remaining work is still required before the migration is fully complete, but the contract is no longer docs-only.

Current remaining gap, in plain English:
- Neon is now the enforced authority for rebuild inputs when the migration flag is on
- but the core math still executes inside a Neon-backed scratch SQLite workspace rather than directly on Postgres
- and `core-weekly` / `cold-core` still rely on operator sequencing or a prior ingest-capable lane to ensure the latest LSEG data has landed locally before source-sync runs

## Design Principles For The Refactor

### A. Separate concerns cleanly

Keep these concerns independent and composable:
- ingest from LSEG into local SQLite
- publish/sync retained source and model data into Neon
- rebuild core state from Neon
- rebuild serving payloads from Neon

Avoid profile logic that mixes these concerns implicitly in route code or one-off fallbacks.

### B. Keep one explicit authority per stage

For each stage, there should be one obvious source of truth:
- ingest stage: local SQLite
- source/runtime publish stage: Neon becomes authoritative after successful sync
- core rebuild stage: Neon
- serving refresh stage: Neon

### C. Make dependency boundaries visible

Profiles should say:
- whether they ingest
- whether they require Neon sync first
- whether they rebuild core
- whether they rebuild serving
- whether they widen or rely on existing Neon retention

### D. Avoid hidden fallback behavior

If Neon-authoritative rebuilds are the contract, rebuild code should not silently fall back to local SQLite unless the profile explicitly says so.

### E. Preserve modular seams

Prefer explicit orchestration seams over route-local branching:
- profile metadata
- ingest/publish prerequisites
- read-backend policy
- Neon retention policy
- Neon readiness checks

## Proposed Mode Semantics

### `serve-refresh`

Purpose:
- cheap holdings/runtime serving refresh only

Reads:
- Neon

Writes:
- Neon serving payloads

Should not:
- run LSEG ingest
- run broad source sync
- rebuild core

### `publish-only`

Purpose:
- republish already-current serving payloads

Reads:
- current persisted serving payloads

Writes:
- Neon serving payloads

Should not:
- ingest
- rebuild core

### `source-daily`

Purpose:
- pull new upstream source data locally, push the retained operating window into Neon, refresh serving

Reads:
- LSEG upstream, then local SQLite during publish

Writes:
- local SQLite source tables
- Neon retained source/runtime data
- Neon serving payloads

Core behavior:
- no core rebuild

### `core-weekly`

Purpose:
- refresh source freshness first, then run the weekly core rebuild from Neon, then republish serving

Reads:
- LSEG upstream locally first
- Neon for the actual rebuild

Writes:
- local SQLite source tables
- Neon retained source tables
- Neon core model tables
- Neon serving payloads

### `cold-core`

Purpose:
- refresh source freshness first, then run a structural rebuild from Neon, then republish

Reads:
- LSEG upstream locally first
- Neon for actual structural rebuild

Writes:
- local SQLite source tables
- Neon retained source tables
- Neon raw history/core tables
- Neon serving payloads

### `universe-add`

Purpose:
- targeted onboarding and post-backfill publish

Rule:
- should explicitly declare whether it needs ingest, retention widening, or only serving rebuild

## Required Architectural Changes

### 1. Introduce an explicit stage authority contract

Add a single place that answers:
- which backend is authoritative for source reads during ingest
- which backend is authoritative for rebuild reads during core/cold-core
- which backend is authoritative for serving reads

Likely seam:
- extend profile metadata with an explicit read-authority policy rather than relying on scattered `core_read_backend("local")` overrides

### 2. Split local ingest from rebuild authority

Today "local ingest" and "local rebuild" are partially tied together. They need to be separated:
- ingest still runs locally
- publish moves authority into Neon
- rebuild then runs on Neon

### 3. Add Neon readiness gates for rebuild profiles

Before `core-weekly` or `cold-core`:
- verify required Neon tables exist
- verify required columns exist
- verify required retention depth exists for the requested rebuild window
- fail with a clear operator error if Neon is not ready

### 4. Make profile prerequisites explicit

Profiles should state:
- `requires_local_ingest`
- `requires_neon_sync`
- `rebuild_backend`
- `serving_backend`
- `retention_requirement`

This should live in orchestrator metadata rather than implicit comments or one-off conditionals.

### 5. Rework `source-daily`/`core-weekly`/`cold-core` sequencing

The intended phase order is:
1. ingest locally
2. publish/sync to Neon
3. verify Neon readiness
4. rebuild from Neon if profile requires it
5. publish serving payloads to Neon
6. run parity/prune/audit

### 6. Remove accidental local-authority fallbacks from core rebuild paths

Any remaining local overrides that were designed to keep rebuilds local should be narrowed to the ingest/publish stage only, not left active for Neon-authoritative rebuild profiles.

### 7. Tighten operator/runtime language

Docs and operator responses should clearly distinguish:
- local source freshness
- Neon retained source freshness
- Neon rebuild readiness
- currently served snapshot freshness

### 8. Separate scratch compute from durable authority

The current stack still uses local `cache.db` as a scratch layer during parts of the risk-model build.

That is acceptable as an implementation detail during migration, but the contract must be explicit:
- scratch is not durable truth
- rebuild inputs come from Neon
- rebuild success is not declared until durable outputs are written back to Neon
- a fresh process should be able to operate from Neon durable state without depending on old local scratch artifacts

## Refactoring Seams

### Seam A: Profile metadata

Primary files:
- `backend/orchestration/run_model_pipeline.py`
- `backend/services/refresh_manager.py`

Refactor goal:
- turn profiles into explicit contracts, not just stage lists

### Seam B: Read routing

Primary files:
- `backend/data/core_reads.py`
- `backend/analytics/pipeline.py`
- `backend/orchestration/run_model_pipeline.py`

Refactor goal:
- make rebuild-backend choice profile-driven and centralized

### Seam C: Neon sync/readiness

Primary files:
- `backend/services/neon_mirror.py`
- `backend/services/neon_stage2.py`
- `backend/config.py`

Refactor goal:
- make "Neon is ready for rebuild" a first-class checked condition

### Seam D: Runtime/operator messaging

Primary files:
- `backend/api/routes/operator.py`
- `.env.example`
- `docs/ARCHITECTURE_AND_OPERATING_MODEL.md`

Refactor goal:
- align system language with Neon-authoritative rebuild semantics

### Seam E: Tests

Primary files:
- `backend/tests/test_operating_model_contract.py`
- `backend/tests/test_refresh_profiles.py`
- `backend/tests/test_neon_mirror_integration.py`
- `backend/tests/test_operator_status_route.py`
- route preference/fallback tests

Refactor goal:
- make the new contract executable and hard to regress

### Seam F: Scratch Compute Layer

Primary files:
- `backend/analytics/pipeline.py`
- `backend/risk_model/daily_factor_returns.py`
- `backend/risk_model/specific_risk.py`
- cache/data persistence helpers

Refactor goal:
- keep local scratch explicit and isolated so it does not masquerade as the authoritative database contract

## Implementation Phases

### Phase 1. Document the target contract

Deliverables:
- this plan doc
- architecture doc update summarizing the new authority model
- env doc update describing local-ingest as upstream feeder and Neon as rebuild/runtime authority

Success criteria:
- no ambiguity about which database is authoritative at each stage

### Phase 2. Encode profile prerequisites

Deliverables:
- richer profile config fields
- validation helpers for profile prerequisites
- refresh manager/runtime logic aligned to the new profile semantics

Success criteria:
- profile behavior can be reasoned about from metadata alone
- profile metadata distinguishes ingest prerequisites, Neon sync prerequisites, rebuild authority, and scratch-vs-durable expectations

### Phase 3. Separate ingest/publish from rebuild authority

Deliverables:
- explicit stage boundaries between local ingest, Neon sync, Neon rebuild, serving publish
- localized use of local SQLite only in ingest/publish steps

Success criteria:
- `core-weekly` and `cold-core` no longer rely on local SQLite as rebuild authority
- the code clearly distinguishes local ingest/staging, Neon durable authority, and optional local scratch

### Phase 4. Add Neon rebuild readiness checks

Deliverables:
- readiness validation helpers
- operator-visible failure messages when Neon depth/columns/parity are insufficient

Success criteria:
- rebuild profiles fail early and truthfully when Neon cannot support them

### Phase 4.5. Add a controlled migration gate

Deliverables:
- one explicit feature flag or profile-level contract switch for Neon-authoritative rebuilds
- transitional tests proving legacy and new behavior where needed during rollout
- operator-visible warnings when runtime behavior is still on legacy local-authoritative rebuild semantics

Success criteria:
- the architecture can move in controlled steps instead of as a brittle big-bang cutover

### Phase 5. Rewire profile sequencing

Deliverables:
- `source-daily` clearly becomes ingest + publish + serving refresh
- `core-weekly` becomes ingest + publish + Neon rebuild + publish
- `cold-core` becomes ingest + publish + Neon structural rebuild + publish

Success criteria:
- operator no longer has to remember to run `source-daily` before core profiles
- the broad source-sync-to-Neon prerequisite is explicit rather than hidden as a side effect of parity work

### Phase 6. Clean up docs, envs, and operator surfaces

Deliverables:
- doc updates
- env default review
- operator/runtime wording updates

Success criteria:
- UI and docs describe the same operating model as the code

### Phase 7. Inventory and freeze the remaining SQLite runtime/rebuild contract

Purpose:
- identify every remaining place where ordinary rebuild/runtime work still depends on SQLite semantics, file paths, or cache tables
- turn that inventory into a bounded migration backlog instead of a vague "port the engine later" promise

Current SQLite-shaped rebuild/runtime seams:
- `backend/risk_model/raw_cross_section_history.py`
  - owns raw-history rebuild table DDL and reads source/history directly with `sqlite3`
- `backend/risk_model/daily_factor_returns.py`
  - reads prices/exposures from SQLite, writes factor returns and residual caches into SQLite tables
- `backend/risk_model/eligibility.py`
  - computes structural eligibility with direct SQLite queries
- `backend/universe/estu.py`
  - computes ESTU membership with direct SQLite reads and schema probes
- `backend/analytics/pipeline.py`
  - still treats local cache and model-output helpers as SQLite-native
- `backend/data/model_outputs.py`
  - persists durable model tables only through SQLite DDL/upserts
- `backend/data/sqlite.py`
  - is the runtime cache authority abstraction today, but it is SQLite-only
- `backend/analytics/services/cache_publisher.py`
  - expects cache-backed runtime state from `backend.data.sqlite`
- `backend/data/history_queries.py`
  - mixes Neon and SQLite query paths instead of exposing one authoritative runtime/rebuild store interface
- `backend/analytics/health.py`
  - still reads diagnostics inputs directly from SQLite-backed data/cache files
- `backend/analytics/services/universe_loadings.py`
  - still computes some runtime loadings from SQLite-backed cache state
- `backend/analytics/refresh_policy.py`
  - still determines factor-return freshness from SQLite cache tables

Deliverables:
- a written inventory of SQLite-only seams, grouped by rebuild stage and runtime surface
- one owner module per seam for the Neon-native replacement
- a migration matrix that distinguishes:
  - scratch-only state
  - durable model outputs
  - runtime-serving payloads
  - local-ingest/archive-only data

Success criteria:
- every remaining SQLite dependency for normal runtime/rebuild work is explicitly named
- no module is allowed to hide "just one more" SQLite responsibility outside the plan

### Phase 8. Introduce backend-neutral storage interfaces

Purpose:
- stop letting model code open database files directly
- create a small number of storage seams that can be implemented by Neon first and SQLite only where the local ingest/archive still needs it

Required interfaces:
- `SourceStore`
  - authoritative reads for prices, fundamentals, classifications, security master, exposure history inputs
- `ModelStore`
  - durable writes/reads for factor returns, covariance, specific risk, run metadata, raw history
- `RuntimeStore`
  - durable serving payloads, refresh metadata, operator/runtime status surfaces
- `ScratchStore`
  - ephemeral intermediate state only, if a stage still benefits from temp persistence during migration

Rules:
- ordinary rebuild/runtime code may depend only on these interfaces, not on `sqlite3.connect(...)`
- local SQLite implementations remain valid only for:
  - `SourceStore` in the LSEG landing/archive role
  - temporary `ScratchStore` during transition
- Neon/Postgres becomes the default implementation for `SourceStore`, `ModelStore`, and `RuntimeStore` in ordinary operation

Initial landing points:
- add repository/service modules under `backend/data/` or `backend/services/`
- move direct SQL/DDL ownership out of analytics/risk modules and into store implementations
- keep the public function signatures of the math-heavy modules as stable as possible while swapping how data is supplied

Success criteria:
- new code stops importing `sqlite3` in runtime/rebuild modules unless it is explicitly scratch-only or local-ingest-only
- authority selection is expressed in one place, not spread across orchestrator and model modules

### Phase 9. Move durable runtime state off SQLite

Purpose:
- eliminate SQLite as the normal authority for runtime payloads and refresh-state metadata first, because that is the safest high-leverage cut

Scope:
- replace `backend/data/sqlite.py` usage in ordinary runtime work with a Neon-backed runtime cache/state service
- keep snapshot semantics, versioned payload publishing, and fallback rules explicit

Main code moves:
- migrate cache/state keys currently written through `sqlite.cache_set(...)`
  - `risk_engine_cov`
  - `risk_engine_specific_risk`
  - `risk_engine_meta`
  - `neon_sync_health`
  - refresh snapshots and active snapshot pointers
- shift `backend/analytics/services/cache_publisher.py` to read from `RuntimeStore`
- shift `backend/analytics/health.py`, `backend/analytics/services/universe_loadings.py`, and `backend/analytics/refresh_policy.py` to use runtime/model stores rather than raw SQLite cache tables
- shrink `backend/data/sqlite.py` to one of:
  - local-ingest/archive utility only
  - scratch-only compatibility layer during migration

Important constraint:
- once this phase lands, a fresh cloud worker should be able to recover ordinary runtime state from Neon without preexisting local cache files

Success criteria:
- Health, exposures, positions, and operator surfaces no longer depend on local SQLite cache state for normal reads
- runtime publish/refresh metadata is durable in Neon
- local cache fallback is either removed or clearly marked emergency-only

### Phase 10. Move durable model outputs off SQLite

Purpose:
- make Neon the native home for derived model tables instead of persisting to SQLite first and mirroring later

Scope:
- port `backend/data/model_outputs.py` from SQLite-native DDL/upsert logic to Neon-native persistence
- treat SQLite copies, if still retained locally, as secondary mirrors rather than primary writes

Primary tables:
- `model_factor_returns_daily`
- `model_factor_covariance_daily`
- `model_specific_risk_daily`
- `model_run_metadata`

Design requirements:
- use Postgres-native upsert/delete semantics directly
- keep row-replacement logic explicit for rolling windows and rebuild rewrites
- preserve operator/debuggability with clear run metadata and row counts

Success criteria:
- successful core runs write durable model outputs to Neon first
- restart/recovery of runtime and downstream views depends on Neon model tables, not local SQLite copies

### Phase 11. Port raw-history, eligibility, and ESTU reads to Neon-native stores

Purpose:
- remove the first large tranche of scratch-SQLite rebuild input dependency by moving the historical source/model read layer to Neon

Modules in scope:
- `backend/risk_model/raw_cross_section_history.py`
- `backend/risk_model/eligibility.py`
- `backend/universe/estu.py`
- any shared schema helpers they still call purely for SQLite table inspection

Approach:
- separate SQL/data-loading logic from math/transformation logic
- keep pandas/numpy computation where useful
- replace SQLite-only schema probes (`sqlite_master`, `PRAGMA table_info`) with store-backed capability checks or Postgres equivalents
- where large extracts are unavoidable, make them explicit bounded window pulls from Neon rather than copying whole tables into scratch SQLite

Success criteria:
- these modules can run directly from Neon-backed source/model stores
- scratch SQLite is no longer required just to assemble raw-history, eligibility, or ESTU inputs

### Phase 12. Port factor-return and risk-engine stages to Neon-native persistence

Purpose:
- remove the most SQLite-bound stage from the rebuild path

Modules in scope:
- `backend/risk_model/daily_factor_returns.py`
- risk cache/state writers in `backend/orchestration/run_model_pipeline.py`
- downstream covariance/specific-risk builders that still assume SQLite cache tables

Approach:
- split the stage into:
  - data load
  - regression/math
  - durable writeback
- route data load from Neon-backed stores
- write residuals, eligibility summaries, and factor-return metadata into Neon-backed durable tables or explicit scratch tables, depending on whether they are durable products or temporary stage artifacts
- remove direct dependency on `daily_factor_returns`, `daily_factor_returns_meta`, and `daily_specific_residuals` as SQLite-only tables

Success criteria:
- `factor_returns` and `risk_model` no longer require a SQLite cache database to complete
- the only remaining local persistence during rebuilds is explicit temp scratch, if any

### Phase 13. Eliminate the Neon-backed scratch SQLite workspace

Purpose:
- finish the migration by removing the compatibility adapter introduced during the first Neon-authoritative slice

Scope:
- remove `prepare_neon_rebuild_workspace(...)` and the temporary workspace path-swapping from the ordinary rebuild path
- convert `neon_readiness` from "validate and materialize workspace" to "validate and grant Neon-native execution"

Required outcomes:
- `core-weekly` and `cold-core` execute directly against Neon-backed stores
- no rebuild stage requires copying large retained datasets from Neon into local SQLite first
- local SQLite remains only:
  - LSEG ingest landing zone
  - optional deep archive
  - optional manual retention-expansion source

Success criteria:
- ordinary rebuilds can run on a cloud worker with Neon access and no local SQLite warehouse present
- local LSEG machine remains necessary only to land new upstream data or deliberately widen retained history

### Phase 14. Cloud-ready cutover and simplification

Purpose:
- make the standalone-tool contract operationally true, not just architecturally possible

Deliverables:
- default config favors Neon-native rebuild/runtime paths
- legacy SQLite runtime/rebuild flags are removed or tightly quarantined
- docs and operator surfaces describe the cloud-ready contract without caveats about ordinary SQLite compute

Success criteria:
- a clean environment with Neon credentials can run serve-refresh, core-weekly, and cold-core after upstream ingest has been synchronized
- only source ingest remains tied to the local LSEG machine

## Post-SQLite Sequencing Recommendation

Recommended order, in plain English:
1. Move runtime state to Neon first.
2. Move durable model outputs to Neon second.
3. Port read-heavy preparation stages next.
4. Port factor-return and risk-model persistence after the stores are stable.
5. Delete the scratch SQLite workspace only after the previous phases are proven in real runs.

Why this order:
- runtime state and model outputs are lower-risk storage seams than the regression engine itself
- once those stores are stable, the remaining SQLite work is mostly about stage-local reads and scratch, not durable authority
- deleting the workspace too early would turn the migration into a brittle big-bang rewrite

## Refactoring Boundaries To Preserve

Keep these boundaries clean during the migration:

`orchestrator`
- decides profile sequencing, prerequisites, and authority
- should not own raw SQL for individual model tables

`stores`
- own SQL/DDL and database-specific behavior
- should present small explicit methods, not "run arbitrary SQL" escape hatches

`math/stage modules`
- should receive data frames, typed payloads, or store interfaces
- should not know whether the backing store is Neon or SQLite

`local ingest/archive`
- remains the only place where the app is allowed to assume direct SQLite warehouse access as part of normal operation

## Hobby-User Failure Modes To Design For

### Case 1. Local ingest succeeded, but Neon sync failed

Desired behavior:
- operator status shows local archive newer than Neon
- Neon-authoritative rebuilds fail closed
- docs tell the user to rerun a source-syncing lane, not a cold-core guess

### Case 2. Neon retention is too shallow for cold-core

Desired behavior:
- `cold-core` fails before compute begins
- the error says to widen Neon retention first
- the system does not silently read deeper local SQLite history to "help"

### Case 3. Cloud worker restarts with no local cache files

Desired behavior:
- serve-refresh and runtime reads recover from Neon durable state
- no invisible dependency on old `cache.db` files remains

### Case 4. Local SQLite archive becomes unavailable after publish

Desired behavior:
- ordinary runtime work continues from Neon
- only new LSEG ingest or deliberate retention expansion is blocked

### Case 5. A partial migration leaves one stage Neon-native and the next stage SQLite-only

Desired behavior:
- interfaces and stage contracts make the remaining scratch dependency explicit
- tests catch mixed-authority regressions before they reach operators

## Acceptance Criteria For "SQLite No Longer Powers Ordinary Runtime/Rebuild Work"

The migration should not be called complete until all of the following are true:
- runtime payload reads come from Neon durable state without requiring local SQLite cache files
- model outputs are durably written to Neon first
- `core-weekly` and `cold-core` can execute without materializing a full scratch SQLite rebuild workspace
- no ordinary rebuild/runtime module imports `sqlite3` except in local-ingest/archive-only code or narrowly scoped temp scratch helpers
- a cloud worker with Neon access can execute rebuild/runtime profiles after data has already been ingested and synchronized from the LSEG machine

## Test And Audit Strategy

### After each implementation phase

Run:
- focused unit tests for the changed seam
- operator/runtime contract tests
- a manual sanity check against live runtime where relevant

Review pass:
- confirm no new fallback path reintroduces local rebuild authority
- confirm no profile can accidentally skip Neon sync before a Neon rebuild

### Final verification

Required checks:
- successful real source-refresh path
- successful real core rebuild path
- successful real cold-core path, if environment/runtime budget permits
- operator status shows Neon-authoritative rebuild/runtime semantics
- serving payloads and relevant core tables are current in Neon
- parity artifacts are clean

## Risks

### Risk 1. Hidden local fallback paths

Mitigation:
- centralize authority selection
- add contract tests

### Risk 2. Neon retention too shallow for intended rebuilds

Mitigation:
- explicit rebuild readiness gates
- explicit operator error messages

### Risk 3. Rebuild semantics become too coupled to one profile

Mitigation:
- encode prerequisites and stage contracts in metadata
- keep orchestration functions small and composable

### Risk 4. Runtime/operator wording lags behind code

Mitigation:
- update docs and operator surfaces in the same change series

### Risk 5. Local scratch survives as an accidental hidden authority

Mitigation:
- name scratch explicitly in code and docs
- require durable Neon writes before declaring rebuild success
- add tests proving a fresh process can recover from Neon durable state

## Review Checklist

Any reviewer of this plan should ask:
- Is there exactly one authority per stage?
- Can a core rebuild start without Neon being current enough?
- Does any profile still rely on local SQLite for ordinary rebuilds?
- Are profile semantics obvious from metadata rather than hidden conditionals?
- Are docs, env defaults, operator messages, and tests aligned?
- Does the system still preserve the local-only LSEG constraint without making local SQLite the normal runtime authority?
- Does the plan distinguish ephemeral local scratch from durable authoritative state?
- Is there a safe migration path, or is the plan accidentally a big-bang rewrite?

## Recommended Final State

The desired final state is:
- LSEG ingest happens only on the local machine
- local SQLite keeps the deep archive for optionality
- Neon is the authoritative operational database
- all normal rebuilds use Neon
- all serving reads use Neon
- widening rebuild history means widening Neon retention first
- source/core/serving profiles are explicit, composable, and fail closed when prerequisites are not met
