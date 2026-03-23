# Architecture And Operating Model

Date: 2026-03-08
Owner: Codex
Status: Canonical reference document

Related planning document:
- `docs/architecture/` is the active architecture package. The active maintenance surface is:
  - `docs/architecture/architecture-invariants.md`
  - `docs/architecture/dependency-rules.md`
  - `docs/architecture/maintainer-guide.md`
- historical architecture audits, restructure/remediation trackers, and investigation notes live under `docs/architecture/archive/`
- completed root-level plans and execution notes live under `docs/archive/legacy-plans/`

## Scope

This document is the canonical description of the operating model and active architectural boundaries.

Use the following docs for more specific views instead of repeating that detail here:
- `../operations/OPERATIONS_PLAYBOOK.md` for named refresh lanes, operational commands, and retention rules
- `../operations/CLOUD_NATIVE_RUNBOOK.md` for the serve-app vs control-app process split and split-origin frontend proxy contract
- `CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md` for the current cPAR system surfaces
- `../operations/CPAR_OPERATIONS_PLAYBOOK.md` for current cPAR runtime-role and authority behavior
- `../reference/protocols/UNIVERSE_ADD_RUNBOOK.md` for the approved universe-add workflow
- `MODEL_FAMILIES_AND_OWNERSHIP.md` for current cUSE4 vs cPAR ownership boundaries and file-placement guidance
- `archive/current-state.md` for the last deep architecture diagnosis snapshot
- `archive/target-architecture.md` for the last target-shape snapshot
- `archive/module-inventory.md` for the last module-by-module ownership snapshot

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

## System Identity

This repository currently hosts two model families:

- `cUSE4`: the incumbent/default risk system
- `cPAR`: the new explicitly namespaced parallel system

The current app and many current integration surfaces remain cUSE4-first by default.
That does not make them generic shared model surfaces.

`cUSE4` is inspired by Barra USE4 methodology but is not a direct implementation.

`Barra` references in this codebase represent lineage only, not system identity.

Pure model-family ownership is:

- `backend/risk_model/*` for pure cUSE4 model logic
- `backend/cpar/*` for pure cPAR model logic

Integration-layer ownership remains in the repo's normal layers and is documented in `MODEL_FAMILIES_AND_OWNERSHIP.md`.

## Design Principles

- Local SQLite remains the only direct LSEG ingest landing zone and the optional deep archive.
- Neon is the authoritative operating database for the standalone tool once source sync has published the retained working set.
- `NEON_AUTHORITATIVE_REBUILDS` now defaults on when Neon is the active data backend and a Neon DSN is configured; set it to `false` only to force a rollback to local-SQLite rebuild authority.
- In `cloud-serve`, a fresh machine should be able to serve cUSE/cPAR runtime surfaces from Neon without a preexisting large local `data.db`; local SQLite remains only for ingest, archive, explicit local diagnostics, and scratch/workspace files.
- Cloud-native prep now assumes two app surfaces:
  - a stateless serve app for public/editor-facing reads and holdings mutations
  - a control app for refresh execution and operator/control diagnostics
- In that split model, the serve app must not own refresh execution or reconcile process-local refresh-worker state.
- The active cUSE model-history window is defined by retained `barra_raw_cross_section_history`, not by the deepest source archive.
- `security_master` is the only universe authority.
- The committed universe artifact is `data/reference/security_master_seed.csv`.
- The three canonical source-of-truth tables are:
  - `security_prices_eod`
  - `security_fundamentals_pit`
  - `security_classification_pit`
- cUSE4 core model state is slower-moving than source data.
- Frontend-facing caches are cheap projections and should refresh more often than the core model.
- Served holdings, prices, and native factor loadings may move ahead of the core risk engine between weekly rebuilds; that is intentional and should not be treated as drift by itself.
- Projection-only returns-based outputs are an exception: they are derived from the stable core package and remain frozen with that package until the next core lane refreshes them.
- The stable core risk package (factor returns, covariance, specific risk, and estimation-basis metadata) advances only on core rebuild lanes and is frozen between rebuilds.
- `serve-refresh` is a serving/projection lane only; it must not compute, persist, or advance core artifacts.
- Holdings changes, price updates, source-data refreshes, and core model recalculations must be treated as different operational events.
- A factor-set change is a core-model change. `serve-refresh` may reuse risk-engine artifacts only when the live cache is both present and current for the active method version.
- Serving-time prices, if introduced, are read-only serving inputs and must never write into canonical historical model-estimation tables such as `security_prices_eod`.
- Canonical timing and contract names are defined in `architecture-invariants.md`.
  Compatibility aliases may remain only for fallback decoding and must not drive new UI or documentation semantics.

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
- When `DATA_BACKEND=neon`, Neon `security_master` is the operating source of truth for app/runtime reads; local SQLite remains the ingest/archive/mirror surface that feeds or repairs Neon.
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
- Local source archives may intentionally extend beyond the active cUSE model window.
- The app should treat Neon as the authoritative trimmed operating copy after a successful publish.
- Neon receives a pruned rolling publish window from this layer:
  - source tables: 10 years
  - analytics tables: 5 years
- Identifier-based historical source tables such as `security_prices_eod`, `security_fundamentals_pit`, and `security_classification_pit` must sync into Neon with identifier-aware semantics:
  - identifiers already fully initialized in Neon may use the normal incremental overlap reload
  - identifiers that are absent in Neon, or only partially initialized there, must receive full retained history for that identifier up to Neon's retained-history floor
- This rule exists so "add ticker + local backfill" converges correctly into the Neon-primary app without manual repair steps.
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
- The active cUSE model-history horizon is defined by retained `barra_raw_cross_section_history`.
- Ordinary `core-weekly` recomputes should ignore deeper source/archive history outside that retained model window.
- Neon rebuild authority is now the default operating path whenever the app is running against Neon with a configured DSN, so the tool can run standalone after local LSEG ingest publishes forward.
- Set `NEON_AUTHORITATIVE_REBUILDS=false` only when you intentionally need to pin core/cold-core rebuilds back to local SQLite for rollback or local-only troubleshooting.
- Durable `model_outputs` readers are contract-split:
  - rebuild-authority readers follow the currently configured rebuild authority
  - local diagnostic readers inspect only the local SQLite archive and must not be treated as app-serving truth
- When rebuild lanes use a workspace/local override, the orchestrator now passes explicit `data_db` / `cache_db` targets through execution instead of mutating process-wide runtime paths.
- Neon-authoritative rebuild rehearsal must fail closed if `neon_readiness` cannot produce a valid scratch workspace or if the derived local mirror sync from that workspace fails.
- The risk-model math window is narrower than retained model history:
  - covariance / specific risk use `LOOKBACK_DAYS` (currently ~2 trading years)
  - factor-return / raw cross-section history may be retained for longer (for example ~5 years)

Interpretation:
- New source data can arrive daily.
- Core model coefficients do not need to move daily.
- This is the correct separation for cost control and stability.

### Projection-Only Derived Outputs

Purpose:
- Support instruments such as SPY and sector ETFs without letting them enter native cUSE estimation.

Rules:
- Served exposure methodology is explicit:
  - `Core` = `model_status = core_estimated` with `exposure_origin = native`
  - `Fundamental Projection` = `model_status = projected_only` with `exposure_origin = projected_fundamental` for single-name equities carried by descriptor/fundamental scoring outside the US core ESTU
  - `Returns Projection` = `model_status = projected_only` with `exposure_origin = projected_returns` for ETFs/ETPs projected from returns regression onto core factor returns
- Projection-only instruments remain outside native factor-return, covariance, and specific-risk estimation.
- Their projected outputs are derived from durable `model_factor_returns_daily`, not cache-era factor-return tables.
- They refresh only on core lanes, persist once per active `core_state_through_date`, and are then read by serving as a durable surface.
- Ordinary `serve-refresh` must not recompute them opportunistically. If the active core package has no persisted projected output for a projection-only instrument, serving surfaces explicit degraded/unavailable state for that instrument instead.
- Current v1 projection math remains intentionally simple: plain OLS on factor returns plus residual-variance-based projected specific risk. Intercepts, EWLS/ridge, and outlier handling remain deferred until there is evidence that the current outputs are materially wrong.

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
- `Risk` (`/cuse/exposures`)
  - reads: `/api/exposures`, `/api/risk`, `/api/portfolio`
  - purpose: factor-level portfolio views plus portfolio risk split and per-position drilldown
- `Explore` (`/cuse/explore`)
  - reads: `/api/universe/search`, `/api/universe/ticker/{ticker}`, `/api/universe/ticker/{ticker}/history`, `/api/universe/factors`, `/api/portfolio`, `/api/portfolio/whatif`
  - purpose: single-name inspection plus account-aware what-if preview against the current live holdings ledger, with optional apply + `serve-refresh` once a scenario is accepted
- `Positions` (`/positions`)
  - reads: `/api/holdings/*`, `/api/portfolio`, `/api/universe/search`
  - purpose: holdings editing/import and current model portfolio view
- `Data` (`/data`)
  - reads: `/api/data/diagnostics`
  - purpose: source-table lineage, coverage, cache surfaces, and integrity diagnostics
- `Health` (`/cuse/health`)
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
- The durable serving publish boundary comes before deep diagnostics. Once payload persistence plus active snapshot publish completes, the run should emit a publish milestone and clients may revalidate app-facing surfaces without waiting for diagnostics tail completion.
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
- `core-weekly` should now be treated as a Neon-authoritative rebuild lane with local ingest as its prerequisite whenever the app is running against Neon with a configured DSN
- if you set `NEON_AUTHORITATIVE_REBUILDS=false`, operators should also treat `core-weekly` and `cold-core` as local-SQLite rebuild lanes again until that rollback is removed

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

## Named Refresh Lanes

Named lanes and their exact operational behavior are defined in `../operations/OPERATIONS_PLAYBOOK.md`.

This document treats them conceptually:
- `serve-refresh`: serving/projection only
- `source-daily`: canonical source ingest and serving refresh
- `source-daily-plus-core-if-due`: daily maintenance with conditional core advancement
- `core-weekly`: stable core-package rebuild
- `cold-core`: structural rebuild path
- `universe-add`: operator workflow finalized through the universe-add runbook

## Universe-Add Workflow

The approved universe-add procedure now lives in `../reference/protocols/UNIVERSE_ADD_RUNBOOK.md`.
This document treats universe maintenance only as an operating-model concept, not as the step-by-step runbook.

## Local vs Neon Policy

### Local SQLite

Role:
- full ingest authority
- full historical archive
- LSEG-connected machine only

Policy:
- keep long/full history locally
- local source archives may extend beyond Neon's publish window when extra historical context is useful
- active cUSE recomputes should still respect the retained `barra_raw_cross_section_history` floor rather than the deepest local source date
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
- Neon retention is a publish/serving policy, not the definition of the active cUSE model horizon
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
