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
- `security_registry` plus `security_policy_current` are the authoritative universe-maintenance surfaces.
- `security_master` remains a compatibility mirror for legacy consumers and diagnostics.
- The committed primary universe artifact is `data/reference/security_registry_seed.csv`.
- `data/reference/security_master_seed.csv` remains only as a compatibility export artifact while legacy workflows still require it.
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
- `security_registry` for identity and tracking state
- `security_policy_current` for current ingest and model-path policy
- `security_master` compatibility mirror only

Key actions:
- add new tickers/RICs
- update registry identifiers and tracking state
- update ingest/model policy for the active universe

Rule:
- Universe maintenance is explicit and file-driven.
- The committed `security_registry_seed.csv` is the primary registry/bootstrap input. `security_master_seed.csv` is a compatibility export only.
- LSEG enrichment remains the authority for live identifiers and the source for derived readiness/taxonomy state.
- When `DATA_BACKEND=neon`, Neon registry/policy/taxonomy/compat surfaces are the operating source of truth for app/runtime reads; local SQLite remains the ingest/archive/mirror surface that feeds or repairs Neon.
- No separate universe-builder artifacts should be needed at runtime.
- After approved universe changes, regenerate and commit `data/reference/security_registry_seed.csv`.
- Regenerate `data/reference/security_master_seed.csv` only while compatibility workflows or tests still require it.

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
- `backend/data/source_reads.py` stays the public source-read facade, with lower registry-first authority helpers isolated in `backend/data/source_read_authority.py`.
- `backend/services/neon_source_sync_cycle.py` now owns the source-only Neon publish cycle used by `source_sync`; `backend/services/neon_stage2.py` stays the public lower source-sync/parity facade, with metadata/status lifecycle helpers isolated in `backend/services/neon_source_sync_metadata.py` and per-table overlap/backfill transfer helpers isolated in `backend/services/neon_source_sync_transfer.py`.
- `backend/services/neon_mirror.py` remains the public broad mirror/parity/prune owner and still defines the `sync + factor_returns_sync + prune + parity` envelope plus parity audit execution.
- `backend/services/neon_mirror_reporting.py` now owns mirror artifact persistence, runtime-health publication, and offline parity-repair helpers built on top of that mirror/parity contract.

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
- Those scratch workspaces are not meant to accumulate indefinitely. Keep only a small recent set (default `NEON_REBUILD_WORKSPACE_RETENTION=2`) and prune older `job_*` workspaces automatically after runs.
- The risk-model math window is narrower than retained model history:
  - covariance / specific risk use `LOOKBACK_DAYS` (currently ~2 trading years)
  - factor-return / raw cross-section history may be retained for longer (for example ~5 years)

Interpretation:
- New source data can arrive daily.
- Core model coefficients do not need to move daily.
- This is the correct separation for cost control and stability.

### Projection-Only Derived Outputs

Purpose:
- Support instruments such as SPY, QQQ, broad-market ETFs, and sector ETFs without letting them enter native cUSE estimation.

Rules:
- Served exposure methodology is explicit:
  - `Core` = `model_status = core_estimated` with `exposure_origin = native`
  - `Projected` = `model_status = projected_only` with `exposure_origin = projected_fundamental` or `projected_returns`
- Projection methodology can still differ internally:
  - single-name equities may arrive through descriptor/fundamental-style projection outside the US core ESTU
  - ETFs/ETPs and similar vehicles may arrive through returns-based projection onto core factor returns
- Those methodology distinctions stay visible in served compatibility fields and must not be collapsed back to generic `projected` or `native` when a name is still a projection candidate.
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
  - reads: `/api/cuse/risk-page` for summary-first render, `/api/cuse/risk-page/exposure-mode?mode=...` for non-raw tabs on demand, `/api/cuse/risk-page/covariance` only when the heatmap scrolls into view, then `/api/exposures/history` for factor drilldown history on demand
  - purpose: factor-level portfolio views plus portfolio risk split and per-position drilldown
- `Explore` (`/cuse/explore`)
  - reads: `/api/universe/search`, `/api/universe/ticker/{ticker}`, `/api/universe/ticker/{ticker}/history`, `/api/universe/factors`, `/api/cuse/explore/context`, `/api/portfolio/whatif`
  - purpose: single-name inspection plus account-aware what-if preview against the current live holdings ledger, with optional apply + `serve-refresh` once a scenario is accepted
- `Positions` (`/positions`)
  - reads: `/api/holdings/*`, `/api/cuse/risk-page`, `/api/cpar/risk`, `/api/universe/search`
  - purpose: shared live-holdings editing/import plus a dual-family modeled coverage check, with cUSE as the operator/control owner and cPAR as a read-only method overlay
- `Data` (`/data`)
  - reads: `/api/data/diagnostics`
  - purpose: source-table lineage, coverage, cache surfaces, and integrity diagnostics
- `Health` (`/cuse/health`)
  - reads: `/api/operator/status`, `/api/risk`, `/api/health/diagnostics`
  - purpose: live operator control-room status plus top-level model quality and deeper model-diagnostics study, loaded on demand because it is the heaviest dashboard page
- Public entry and auth:
  - `/` is now the lightweight public landing placeholder
  - `/login` establishes the shared frontend session
  - `/home` is the authenticated app home that replaced the old public dashboard landing role
- Privileged settings:
  - `/settings` is an authenticated privileged page for the primary account
  - it remains the temporary maintenance surface for browser-held backend tokens during the transition to frontend-server trust

Efficiency rules now in force:
- the frontend now owns the app auth boundary through a shared signed-cookie session and middleware-gated `/api/*` routes; page-level token-presence checks are UI suppression only, not auth
- protected pages should hydrate the shared shell from middleware-validated session/context bootstrap and treat `/api/auth/session` as a background refresh path rather than a first-paint gate
- operator state is fetched on demand plus fast-polled only while a refresh is actively running; pages should not each invent their own background loop
- anonymous visits must not trigger shared-shell control-plane reads; operator chrome belongs behind an authenticated operator surface rather than ambient app-shell fetches
- browser `/api/*` traffic should flow only through owned App Router route handlers; ambient catch-all backend rewrites are no longer part of the contract
- header sync/recalc actions now use canonical profile semantics (`serve-refresh`) instead of legacy mode-based refresh calls
- ticker/RIC typeahead is debounced before hitting `/api/universe/search`
- Health diagnostics are no longer fetched automatically on page load, and heavy sections mount only as the user scrolls
- user-facing dashboard pages should consume durable serving outputs first rather than piecing together raw source tables in the browser
- the Explore what-if preview is intentionally ephemeral until explicit apply; staged trade deltas live in browser state and are posted once to `/api/portfolio/whatif` for in-memory comparison only, then can be written only through explicit `Apply + RECALC`
- `/api/cuse/explore/context` is the cUSE-owned held-position lookup for Explore first render; it may derive scoped positions from live holdings plus the current served cUSE loadings surface, but it must not grow into a second generic portfolio contract or duplicate builder-owned holdings account/ledger reads
- in `local-ingest`, old local cache blobs remain bootstrap fallback only when a serving payload snapshot does not yet exist
- in `cloud-serve`, serving routes fail closed instead of falling back to local cache/SQLite state
- universe explore/search outputs
- health/diagnostic payloads

Key rule:
- Serving refreshes should be cheap and frequent.
- They should not trigger full cUSE4 recompute unless explicitly requested.
- Their job is to publish the latest holdings, prices, and factor-loadings projection against the currently accepted core risk-engine state.
- If the current stable core package is missing or stale, `serve-refresh` should fail closed and direct the operator to a core rebuild lane instead of recomputing core artifacts on the serving path.
- Workspace scratch paths handed to `serve-refresh` do not by themselves flip core source reads to local SQLite. Workspace paths alone should not override the serving lane's existing backend-selection decision.
- Projection-only registry rows and persisted projected-loadings fallback remain the narrow exception: those helpers may consult explicit workspace/canonical SQLite paths inside the refresh context without broadening the entire serving lane into local authority mode.
- The durable serving publish boundary comes before deep diagnostics. Once payload persistence plus active snapshot publish completes, the run should emit a publish milestone and clients may revalidate app-facing surfaces without waiting for diagnostics tail completion.
- Deep model-health diagnostics belong to `core-weekly`, `cold-core`, or another explicit diagnostics-producing lane rather than the ordinary quick refresh path.
- The currently active serving payload set should be durable and mirrorable (`serving_payload_current`), not only present in the local cache layer.
- public serving-payload reads go through `backend/data/serving_outputs.py` (`load_current_payload(s)` / `load_runtime_payload(s)`), while lower Neon/SQLite read helpers stay isolated in `backend/data/serving_output_read_authority.py`
- durable serving-payload writes, Neon verification, and manifest drift helpers also stay behind `backend/data/serving_outputs.py`, with lower write/manifest helpers isolated from higher layers
- public operator/runtime-state reads and writes go through `backend/data/runtime_state.py`, while lower Neon/fallback authority helpers stay isolated in `backend/data/runtime_state_authority.py`
- Full serving promotion now means the canonical payload set, not an arbitrary subset:
  - `eligibility`
  - `exposures`
  - `health_diagnostics`
  - `model_sanity`
  - `portfolio`
  - `refresh_meta`
  - `risk`
  - `risk_engine_cov`
  - `risk_engine_specific_risk`
  - `universe_factors`
  - `universe_loadings`
- Canonical serving promotion must be atomic as a set.
  - `replace_all=true` is reserved for that canonical set only.
  - targeted metadata patches such as `health_diagnostics` or `refresh_meta` remain explicit partial writes and must not masquerade as a full publish.
- Projection-only serving rows are protected at publish time.
  - If persisted projected loadings exist for a ticker at the active core date, the live serving payloads must publish that ticker as `projected_only` with `exposure_origin=projected_returns` or `projected_fundamental`, or the publish fails.
- During refresh persistence, the current run's `cuse_security_membership_daily` truth is overlaid onto `universe_loadings`, `portfolio`, and exposure drilldowns before the canonical serving payload set is written, so the just-computed run does not lag one publish behind.
- `universe_loadings` and the app-facing universe search/detail surfaces are runtime-admitted only; raw source names without current registry/runtime identity must not appear there just because prices, fundamentals, or classifications exist.
- Explore/what-if quote search and detail may augment the live `universe_loadings` payload with registry/runtime-admitted rows when a ticker has no current published cUSE loadings row, but those fallback rows must be clearly labeled as registry/runtime coverage rather than live factor payload coverage.
- Held names that already carry a published modeled surface inside the `portfolio` payload must remain preview-eligible even if they are temporarily absent from `universe_loadings`; universe search/detail and cUSE what-if preview should treat that portfolio overlay as part of the active published surface rather than downgrading the name to registry-only coverage.

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
- update the committed registry artifact and the resulting registry/policy surfaces
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
- registry-first universe update and source backfill procedure for new names is explicit
- holdings edits can refresh serving outputs without touching core model
- daily source updates are clearly separated from weekly cUSE4 recompute
- Neon pruning/parity is automatic and observable
- the frontend shows operational state clearly enough that you do not need the terminal for routine checks
