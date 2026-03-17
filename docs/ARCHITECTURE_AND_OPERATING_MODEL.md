# Architecture And Operating Model

Date: 2026-03-08
Owner: Codex
Status: Canonical reference document

Related planning document:
- `docs/architecture/` is the active repository-structure package. `docs/architecture/restructure-plan.md` is the live master tracker for architectural cleanup and follow-up structure work.
- `docs/NEON_AUTHORITATIVE_REBUILD_PLAN.md`, `docs/NEON_STANDALONE_EXECUTION_PLAN.md`, and `docs/NEON_MAIN_PLATFORM_PLAN.md` are still the relevant focused Neon migration plans.
- `docs/NEON_LEAN_CONSOLIDATION_PLAN.md`, `docs/HEALTH_DIAGNOSTICS_REFRESH_PLAN.md`, and `docs/PROJECT_HARDENING_ORGANIZATION_PLAN.md` are completed or subordinate execution notes kept for context rather than as competing master plans.

## Current Implementation Status

Implemented now:
- canonical orchestrator lane names are live in `run_model_pipeline`
- Neon-authoritative core lanes now insert explicit `source_sync` and `neon_readiness` stages when `NEON_AUTHORITATIVE_REBUILDS=true`
- Neon-authoritative core work now runs from a scratch SQLite workspace materialized from Neon, then mirrors derived outputs back into Neon and the local private mirror
- source-sync now fails closed if the local archive is older than Neon for the source tables it is about to publish
- `/api/operator/status` exposes lane status, source recency, core-due state, refresh state, and Neon parity health
- `/api/operator/status` now distinguishes authoritative operating source dates from the local SQLite ingest/archive dates on the LSEG machine
- `/api/operator/status` also carries backend-authoritative holdings dirty state and runtime warnings
- operator-status and data-diagnostics payload assembly now live in dedicated backend services instead of route-local construction blocks
- exposures, risk, and portfolio serving-payload assembly now also live in a dedicated backend service rather than route-local load/normalize blocks
- refresh-context policy, universe-loadings reuse checks, publish-only payload stamping, and durable refresh persistence now live in dedicated analytics modules rather than inside one monolithic `analytics/pipeline.py`
- serving-source-date assembly, eligibility-summary loading, model-sanity reporting, and health-diagnostics carry-forward now also live in dedicated analytics helpers rather than one oversized `cache_publisher.py`
- canonical source reads now use a thin `core_reads.py` facade over explicit transport/source-date/source-query modules
- durable model-output persistence now uses a thin `model_outputs.py` facade over explicit schema/state/payload/writer helpers
- Health page now acts as the live operator control deck and freshness/model-quality surface
- Data page now acts as the source-table/cache diagnostics surface
- Health page is now split into a shell plus lazily mounted diagnostics sections so heavy chart bundles load only after explicit user intent
- Exposures and Positions now share one frontend truth-summary helper for snapshot id, served loadings date, latest available loadings date, and core-model date instead of recomputing that banner separately per page
- frontend API contracts are now split by domain behind a stable `src/lib/types.ts` barrel so feature ownership is clearer without changing import paths
- Health page now treats Operator Status as the primary source-recency/control-room surface and uses served risk payloads only for served-model facts like current factor-return fit
- header refresh controls are now reduced to one context-aware quick action (`SYNC` / `RECALC`) so the same `serve-refresh` action is not exposed twice with different labels
- source recency now explicitly tracks prices, fundamentals, classification, and raw cross-section dates
- `make operator-check` / `scripts/operator_check.sh` provide one-command backend/operator validation
- holdings serving reads now prefer Neon whenever a Neon DSN is configured; in-code mock positions are bootstrap fallback only
- refresh-driven `RECALC needed` state is backend-persisted and only clears after a successful serving refresh
- serving payloads now persist into durable `serving_payload_current` rows so the cloud-serving path can read dashboard outputs without depending solely on local cache blobs
- `cloud-serve` runtime now treats durable serving payloads as the effective serving authority
- `/api/health/diagnostics` now follows the same durable-serving-first discipline as the main dashboard truth surfaces
- `cloud-serve` runtime now restricts refresh lanes to `serve-refresh` and blocks LSEG ingest
- a bare cloud `POST /api/refresh` now resolves safely to `serve-refresh`; deeper lanes still require explicit local/operator intent
- broad Neon mirror/parity/prune remain a `local-ingest` publish responsibility rather than a cloud-serving behavior
- holdings mutations now flow through a dedicated backend service layer instead of route-local business logic
- the Positions page now composes feature-level holdings modules (`features/holdings`) rather than owning CSV parsing, mutation orchestration, and tables inline
- holdings projection now loads one holdings snapshot per refresh build instead of re-querying holdings metadata repeatedly
- Neon holdings read failures now stop serving-refresh projection work instead of silently degrading to an empty successful portfolio
- Deep `health_diagnostics` are now published durably alongside the rest of the serving payload set, but ordinary quick refreshes carry them forward instead of recomputing them
- factor-return persistence now replaces stale history slices in durable SQLite and Neon instead of only appending from the latest durable date
- durable covariance persistence now prunes retired factor names so removed factors do not linger in historical covariance rows
- Neon factor-return parity now checks sampled row values and inference-field coverage, not only row counts and date windows
- durable model outputs now write to Neon first when Neon is configured; local SQLite acts as a secondary mirror during migration
- durable serving payloads now write to Neon first when Neon serving authority is required; local SQLite remains a secondary mirror and local diagnostic surface
- operator/health runtime truth keys now have a Neon-backed `runtime_state_current` surface with local SQLite fallback retained only for transitional local-ingest recovery
- the runtime-state surface is intentionally narrow: `risk_engine_meta`, `neon_sync_health`, and the active snapshot pointer are the only durable runtime-state keys in Neon for this phase
- `/api/health` and `/api/operator/status` now expose runtime-state status and source metadata so missing or degraded runtime truth is visible instead of silently reading as healthy
- post-run Neon sync health publication now preserves a local fallback health signal even if the Neon runtime-state write fails, so operator observability does not disappear on the ingest machine during a Neon incident
- the active model now carries 45 factors in total, including 14 style factors; there is no standalone `Value` factor in the live style set

Cold-core lessons now incorporated:
- serving refresh must read live risk-engine cache keys, not only the active published snapshot
- staged snapshots can be correct while the live pointer is still stale, so operator observability must show the active snapshot id
- heavy diagnostics and operator-state surfaces should be separated so the operator view remains useful even when diagnostics are stale
- during local/workspace rebuilds, `serving_refresh` must read from the same local/workspace source tables that produced the new raw history; otherwise it can publish stale Neon loadings beside fresh local core-model outputs
- during light `serve-refresh`, weekly core-state should resolve from the latest durable `model_run_metadata` before runtime-state fallback; otherwise a stale runtime key can overwrite the current core model while republishing fresh loadings
- during ordinary `serve-refresh`, deep model-health diagnostics should be reused or explicitly deferred; they should not be recomputed on the quick path
- frontend operator/freshness surfaces should stay narrow: one compact banner on user-facing pages, one control-room page for runtime truth, and one deeper diagnostics page for maintenance

## Known Limitations Still Open

- Neon-authoritative rebuilds still rely on a Neon-backed scratch SQLite workspace because the core math has not yet been ported to run directly on Postgres
- runtime-state migration is only partially complete; operator/health keys now mirror into Neon, but broader cache-backed analytics state and rebuild-stage state still depend heavily on SQLite
- regression inference currently ships as HC1 robust SE / t-stat; HC2 or HC3 evaluation and explicit leverage diagnostics remain follow-up work for sparse or high-leverage buckets
- winsorization policy is configurable and improved, but its governance and diagnostic instrumentation are still lighter than ideal
- durable-serving publish and cache-snapshot publish still use separate stores, so there is no single atomic cross-store commit boundary
- refresh locking is still process-local rather than cross-process or distributed
- route contract enforcement is much tighter than before, but still not fully unified across every route surface

## Objective

Define one explicit operating model for:
- universe maintenance,
- source-of-truth ingest and backfill,
- local SQLite vs Neon retention,
- holdings-driven refreshes,
- cUSE4 core model recompute depths,
- serving/cache-only refreshes,
- frontend observability.

This plan is intentionally operational rather than theoretical. It maps directly to the current codebase and identifies the cleanup needed before full cloud cutover.

## Design Principles

- Local SQLite remains the only direct LSEG ingest landing zone and the optional deep archive.
- Neon is the authoritative operating database for the standalone tool once source sync has published the retained working set.
- During migration, `NEON_AUTHORITATIVE_REBUILDS` controls whether core/cold-core still rebuild from local SQLite or from Neon.
- The active Barra model-history window is defined by retained `barra_raw_cross_section_history`, not by the deepest source archive.
- `security_master` is the only universe authority.
- The committed universe artifact is `data/reference/security_master_seed.csv`.
- The three canonical source-of-truth tables are:
  - `security_prices_eod`
  - `security_fundamentals_pit`
  - `security_classification_pit`
- cUSE4 core model state is slower-moving than source data.
- Frontend-facing caches are cheap projections and should refresh more often than the core model.
- Served holdings, prices, and factor loadings may move ahead of the core risk engine between weekly rebuilds; that is intentional and should not be treated as drift by itself.
- The stable core risk package (factor returns, covariance, specific risk, and estimation-basis metadata) advances only on core rebuild lanes and is frozen between rebuilds.
- `serve-refresh` is a serving/projection lane only; it must not compute, persist, or advance core artifacts.
- Holdings changes, price updates, source-data refreshes, and core model recalculations must be treated as different operational events.
- A factor-set change is a core-model change. `serve-refresh` may reuse risk-engine artifacts only when the live cache is both present and current for the active method version.
- Serving-time prices, if introduced, are read-only serving inputs and must never write into canonical historical model-estimation tables such as `security_prices_eod`.

## Four-Layer Operating Model

### 1) Universe Layer

Purpose:
- Define which securities the platform cares about.

Authority:
- `security_master`

Key actions:
- add new tickers/RICs
- update registry identifiers
- mark names in or out of the eligible universe

Rule:
- Universe maintenance is explicit and file-driven.
- The committed `security_master_seed.csv` is a registry/bootstrap input only; LSEG enrichment is the authority for live identifiers and the source for derived eligibility flags.
- No separate universe-builder artifacts should be needed at runtime.
- After approved universe changes, regenerate and commit `data/reference/security_master_seed.csv`.

### 2) Source Data Layer

Purpose:
- Maintain canonical historical prices, PIT fundamentals, and PIT classification.

Authority:
- local `data.db` for direct LSEG landing and deep archive
- Neon for the retained operating window after publish/sync

Key actions:
- daily latest-session updates
- targeted subset repairs
- historical backfills for new names

Rule:
- Source tables can be current to the latest completed XNYS session.
- They are not constrained by the cUSE4 lag policy.
- Local source archives may intentionally extend beyond the active Barra model window.
- The app should treat Neon as the authoritative trimmed operating copy after a successful publish.
- Neon receives a pruned rolling publish window from this layer:
  - source tables: 10 years
  - analytics tables: 5 years
- Fundamentals and classification PIT backfills run monthly by default.
- `source-daily` enforces closed-month PIT anchors only; open-month fundamentals/classification rows are purged and missing prior month anchors are backfilled automatically.
- `source-daily` also repairs missing daily price sessions between the previous local price date and the latest completed session.
- Only `local-ingest` should publish broad source/model updates into Neon.

### 3) Core cUSE4 Model Layer

Purpose:
- Build the slower-moving model state:
  - ESTU
  - raw cross-section history
  - daily factor returns
  - covariance
  - specific risk

Key rule:
- Core estimation uses lagged exposures only.
- Current policy remains `CROSS_SECTION_MIN_AGE_DAYS=7`.
- The active Barra model-history horizon is defined by retained `barra_raw_cross_section_history`.
- Ordinary `core-weekly` recomputes should ignore deeper source/archive history outside that retained model window.
- The intended rebuild authority is Neon so the tool can run standalone after local LSEG ingest publishes forward.
- While `NEON_AUTHORITATIVE_REBUILDS=false`, local SQLite still remains the actual rebuild authority for core/cold-core.
- When rebuild lanes use a workspace/local override, the orchestrator now passes explicit `data_db` / `cache_db` targets through execution instead of mutating process-wide runtime paths.
- The risk-model math window is narrower than retained model history:
  - covariance / specific risk use `LOOKBACK_DAYS` (currently ~2 trading years)
  - factor-return / raw cross-section history may be retained for longer (for example ~5 years)

Interpretation:
- New source data can arrive daily.
- Core model coefficients do not need to move daily.
- This is the correct separation for cost control and stability.

### 4) Serving / UI Layer

Purpose:
- Build cheap outputs for the frontend using the latest available holdings plus current source data plus frozen core model state.

Examples:
- portfolio projection
- exposures page
- risk page

### Dashboard Output Wiring

The dashboard should stay thin. Each page should read one of a small number of serving surfaces rather than rebuilding logic in the browser.

Canonical page-to-backend wiring:
- `Risk` (`/exposures`)
  - reads: `/api/exposures`, `/api/risk`, `/api/portfolio`
  - purpose: factor-level portfolio views plus portfolio risk split and per-position drilldown
- `Explore`
  - reads: `/api/universe/search`, `/api/universe/ticker/{ticker}`, `/api/universe/ticker/{ticker}/history`, `/api/universe/factors`, `/api/portfolio`, `/api/portfolio/whatif`
  - purpose: single-name inspection plus account-aware what-if preview against the current live holdings ledger, with optional apply + `serve-refresh` once a scenario is accepted
- `Positions`
  - reads: `/api/holdings/*`, `/api/portfolio`, `/api/universe/search`
  - purpose: holdings editing/import and current model portfolio view
- `Data`
  - reads: `/api/data/diagnostics`
  - purpose: source-table lineage, coverage, cache surfaces, and integrity diagnostics
- `Health`
  - reads: `/api/operator/status`, `/api/risk`, `/api/health/diagnostics`
  - purpose: live operator control-room status plus top-level model quality and deeper model-diagnostics study, loaded on demand because it is the heaviest dashboard page

Efficiency rules now in force:
- operator state is fetched on demand plus fast-polled only while a refresh is actively running; pages should not each invent their own background loop
- header sync/recalc actions now use canonical profile semantics (`serve-refresh`) instead of legacy mode-based refresh calls
- ticker/RIC typeahead is debounced before hitting `/api/universe/search`
- Health diagnostics are no longer fetched automatically on page load, and heavy sections mount only as the user scrolls
- user-facing dashboard pages should consume durable serving outputs first rather than piecing together raw source tables in the browser
- the Explore what-if preview is intentionally ephemeral until explicit apply; staged trade deltas live in browser state and are posted once to `/api/portfolio/whatif` for in-memory comparison only, then can be written only through explicit `Apply + RECALC`
- in `local-ingest`, old local cache blobs remain bootstrap fallback only when a serving payload snapshot does not yet exist
- in `cloud-serve`, serving routes fail closed instead of falling back to local cache/SQLite state
- universe explore/search outputs
- health/diagnostic payloads

Key rule:
- Serving refreshes should be cheap and frequent.
- They should not trigger full cUSE4 recompute unless explicitly requested.
- Their job is to publish the latest holdings, prices, and factor-loadings projection against the currently accepted core risk-engine state.
- If the current stable core package is missing or stale, `serve-refresh` should fail closed and direct the operator to a core rebuild lane instead of recomputing core artifacts on the serving path.
- Deep model-health diagnostics belong to `core-weekly`, `cold-core`, or another explicit diagnostics-producing lane rather than the ordinary quick refresh path.
- The currently active serving payload set should be durable and mirrorable (`serving_payload_current`), not only present in the local cache layer.

## Canonical Event Types

### A) Holdings-only change

Examples:
- manual position edit
- CSV import
- account-level rebalance

Should do:
- write holdings to Neon
- mark serving cache dirty in backend state
- allow manual `RECALC`
- refresh portfolio/exposure/risk projection outputs

Should not do:
- LSEG ingest
- source-table rewrite
- cUSE4 core recompute

### B) Daily source update

Examples:
- new trading day prices
- latest fundamentals/classification refresh

Should do:
- pull latest eligible-universe source data into local SQLite
- publish/sync the retained operating window into Neon
- rebuild serving outputs

Should usually not do:
- full raw-history rebuild
- full core recompute

Optional:
- if weekly cadence says core is due, run core recompute afterward
- under the target contract, that core recompute should run from Neon after the publish step succeeds

### C) Weekly core update

Examples:
- scheduled weekly cUSE4 refresh
- manual high-confidence model refresh

Should do:
- ensure current source-of-truth history has been published into Neon
- recompute factor returns, covariance, specific risk
- rebuild serving outputs
- mirror/prune/parity to Neon

Should not do by default:
- full historical raw-history rebuild

Migration note:
- while `NEON_AUTHORITATIVE_REBUILDS=false`, operators should still run a source-syncing lane before `core-weekly` because the rebuild path remains local-SQLite-first
- once `NEON_AUTHORITATIVE_REBUILDS=true`, `core-weekly` should be treated as a Neon-authoritative rebuild lane with local ingest as its prerequisite

### D) Structural data change

Examples:
- new ticker(s) added to the universe
- broad price history rewrite
- fundamentals/classification schema change
- methodology change

Should do:
- update `security_master`
- targeted or full source backfill as needed
- rebuild raw-history over affected window
- run `cold-core` if the change affects historical model inputs materially

## Recommended Named Profiles

These are the profiles/processes the system should expose clearly, even if some map to existing code paths under the hood.

### 1) `serve-refresh`

Purpose:
- rebuild frontend-facing caches only

Does:
- portfolio / risk / exposures / universe serving payloads
- health payloads
- durable serving-payload write (`serving_payload_current`)
- staged `health_diagnostics` computation against the same refresh-run inputs that are about to be published
- holdings-triggered refreshes may reuse the current published `universe_loadings` payload when source dates and the risk-engine fingerprint still match
- on that reuse path, cached `eligibility` and `cov_matrix` may also be reused when present
- on that reuse path, durable relational `model_outputs` persistence is intentionally skipped because the underlying factor/covariance/specific-risk state has not changed

Does not:
- pull LSEG
- recompute cUSE4 core
- run broad Neon mirror/parity/prune in `cloud-serve`

Primary trigger:
- holdings edits
- manual frontend refresh

Current implemented path:
- `serve-refresh`
- In `cloud-serve`, this is the only allowed refresh lane.
- manual `serve-refresh` keeps the existing full serving-refresh behavior; only holdings-triggered refreshes set the explicit `holdings_only` reuse hint

### 2) `source-daily`

Purpose:
- update source-of-truth tables for latest available session without touching core model

Does:
- LSEG ingest for the latest completed session
- contiguous daily price repair for any missing sessions up to that session
- closed-month PIT maintenance for fundamentals + classification, including automatic backfill of missed month-end anchors
- serving refresh
- Neon mirror/parity/prune

Does not:
- recompute daily factor returns / covariance / specific risk

Primary trigger:
- daily market data update

Current implemented path:
- `source-daily`
- actual live ingest still depends on `ORCHESTRATOR_ENABLE_INGEST=true`
- orchestrator-driven live ingest is a single full-universe pass; manual sharded runs belong on the direct LSEG ingest script, not the orchestrator lane
- prices are ingested for the latest completed XNYS session, then missing daily sessions are backfilled from the prior local price date
- fundamentals/classification are restricted to closed-month anchors only; if open-month PIT rows exist, `source-daily` deletes them before syncing to Neon
- missing closed-month anchors are backfilled automatically so skipped months do not accumulate silently
- This lane is intentionally unavailable in `cloud-serve`.

### 3) `source-daily-plus-core-if-due`

Purpose:
- default daily scheduled maintenance run

Does:
- daily source update
- serving refresh
- run core recompute only if cadence/version says due

Primary trigger:
- once-daily operator run

Current implemented path:
- `source-daily-plus-core-if-due`
- local ingest still depends on `ORCHESTRATOR_ENABLE_INGEST=true`
- while `NEON_AUTHORITATIVE_REBUILDS=false`, any due core work still rebuilds from local SQLite
- once `NEON_AUTHORITATIVE_REBUILDS=true`, this lane now inserts `source_sync` before serving/core work and `neon_readiness` before any due core rebuild
- the Neon-authoritative core path materializes a scratch SQLite workspace from Neon, runs the existing core math there, then mirrors derived outputs back into Neon and the local private mirror

### 4) `core-weekly`

Purpose:
- force a weekly cUSE4 recompute without cold historical rebuild

Does:
- factor returns
- covariance
- specific risk
- serving refresh
- Neon mirror/parity/prune

Does not:
- historical raw rebuild unless separately requested

Current implemented path:
- `core-weekly`
- operators should still keep local ingest current, but when `NEON_AUTHORITATIVE_REBUILDS=true` the lane now inserts `source_sync` and `neon_readiness` automatically
- rebuild authority is local SQLite until `NEON_AUTHORITATIVE_REBUILDS=true`, then the rebuild inputs come from Neon via the scratch workspace
- daily factor-return recompute now resolves uncached dates before loading prices and only materializes the required price window plus the immediately prior session needed for return calculation
- factor-return cache invalidation now also keys off the configured minimum exposure-snapshot age (`CROSS_SECTION_MIN_AGE_DAYS`)

### 5) `cold-core`

Purpose:
- structural rebuild path

Does:
- raw cross-section history rebuild over retained history
- core cache reset
- factor returns recompute
- covariance/specific risk recompute
- serving refresh
- Neon mirror/parity/prune

Use only when:
- historical source data changed materially
- methodology changed
- large universe expansion happened

Current path:
- `cold-core`
- today this still rebuilds from local SQLite unless `NEON_AUTHORITATIVE_REBUILDS=true`
- on the Neon-authoritative path, `cold-core` fails closed if Neon retention is too shallow for the rebuild horizon instead of silently widening from the local archive
- under the target contract, operators widen Neon retention first when they want a deeper standalone rebuild window

### 6) `universe-add`

Purpose:
- operational workflow for adding new names

Does:
- merge/validate new names into `security_master`
- targeted backfill for:
  - full price history
  - monthly or quarterly fundamentals PIT
  - monthly or quarterly classification PIT
- coverage audit for the added names
- then either:
  - `serve-refresh` for small adds, or
  - `cold-core` for material additions

Current state:
- named lane exists for observability/finalization
- onboarding and targeted backfill still remain operator-driven via runbook/commands with Codex

## Universe-Add Standard Procedure

For every new ticker batch:

1. Merge identifiers into the committed security registry
- required: `ric`
- recommended: `ticker`
- preferred metadata: `isin`, `exchange_name`
- bootstrap-sync the registry into `security_master`
- do not set `classification_ok` or `is_equity_eligible` manually; they are populated by canonical LSEG enrichment and derived classification logic

2. Validate the merge
- duplicate RIC check
- blank RIC check
- registry rows present in `security_master`
- new rows pending until LSEG enrichment/backfill runs

3. Backfill source-of-truth tables for only the new RICs
- prices: full retained local history
- fundamentals: monthly PIT from retained local start to current
- classification: monthly PIT from retained local start to current
- explicit subset backfills may target pending names directly; those runs are also responsible for populating live `security_master` identifiers/flags from LSEG

4. Run targeted coverage checks
- prices coverage by date
- fundamentals field coverage
- classification/TRBC coverage

5. Choose refresh depth
- small batch / recent adds: `serve-refresh` or `core-weekly`
- large batch or deep history rewrite: `cold-core`

## Local vs Neon Policy

### Local SQLite

Role:
- full ingest authority
- full historical archive
- LSEG-connected machine only

Policy:
- keep long/full history locally
- local source archives may extend beyond Neon's publish window when extra historical context is useful
- active Barra recomputes should still respect the retained `barra_raw_cross_section_history` floor rather than the deepest local source date
- do not prune unless deliberately doing local retention work

### Neon

Role:
- serving store
- holdings store
- API read target

Policy:
- prune automatically after mirror
- keep only bounded windows:
  - source tables: 10 years
  - analytics tables: 5 years
- Neon retention is a publish/serving policy, not the definition of the active Barra model horizon
- parity checks must compare Neon only against the same bounded windows
- if a Neon DSN is configured, holdings-serving reads should resolve from Neon rather than static mock positions

This is already the implemented direction and should remain the rule.

## Frontend Observability Model

The frontend should expose backend/runtime truth clearly, without pretending to offer deeper operator controls than it actually does.

### Header-level signals

- backend-authoritative holdings dirty / `RECALC` needed
- refresh running / idle / failed
- Neon sync health

### Health-page operator cards

Should show:
- compact status for each canonical lane (`serve-refresh`, `source-daily`, `source-daily-plus-core-if-due`, `core-weekly`, `cold-core`, `universe-add`)
- latest run state per lane
- in-flight run status without pretending to be a full stage-inspector
- latest source dates:
  - prices
  - fundamentals
  - classification
- current loadings / cross-section
- core risk-state dates, kept separate from raw source recency:
  - core state through
  - core rebuilt
  - estimation exposure anchor when available
- core due status:
  - due / not due
  - reason
- Neon mirror status
- runtime warnings when the backend is not operating in the standard Neon-first profile
- fast diagnostics vs deep diagnostics explicitly labeled, so omitted expensive checks are not mistaken for live truth
- lane-specific control actions beyond the generic refresh prompt should remain operator/API driven until the frontend intentionally reintroduces them

### Recommended color model

- green: healthy
- yellow: stale / due / waiting for recalc
- red: failed / mismatch / missing required data

## What “Done” Looks Like

The operating model is considered ready for full cloud usage when:
- every update type maps to one named workflow
- `security_master` update and source backfill procedure for new names is explicit
- holdings edits can refresh serving outputs without touching core model
- daily source updates are clearly separated from weekly cUSE4 recompute
- Neon pruning/parity is automatic and observable
- the frontend shows operational state clearly enough that you do not need the terminal for routine checks
