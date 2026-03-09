# Architecture And Operating Model

Date: 2026-03-08
Owner: Codex
Status: Canonical reference document

## Current Implementation Status

Implemented now:
- canonical orchestrator lane names are live in `run_model_pipeline`
- legacy profile aliases still resolve for compatibility
- `/api/operator/status` exposes lane status, source recency, core-due state, refresh state, and Neon parity health
- `/api/operator/status` also carries backend-authoritative holdings dirty state, runtime warnings, and per-lane recent-run history
- Data page now acts as the operator control deck with lane controls, plain-English popovers, recent-run history strips, stage detail, and fast/deep diagnostics
- Health page also surfaces operator cards for redundancy
- source recency now explicitly tracks prices, fundamentals, classification, and raw cross-section dates
- `make operator-check` / `scripts/operator_check.sh` provide one-command backend/operator validation
- holdings serving reads now prefer Neon whenever a Neon DSN is configured; in-code mock positions are bootstrap fallback only
- refresh-driven `RECALC needed` state is backend-persisted and only clears after a successful serving refresh
- serving payloads now persist into durable `serving_payload_current` rows so the cloud-serving path can read dashboard outputs without depending solely on local cache blobs
- `cloud-serve` runtime now treats durable serving payloads as the effective serving authority
- `cloud-serve` runtime now restricts refresh lanes to `serve-refresh` and blocks LSEG ingest
- broad Neon mirror/parity/prune remain a `local-ingest` publish responsibility rather than a cloud-serving behavior

Cold-core lessons now incorporated:
- serving refresh must read live risk-engine cache keys, not only the active published snapshot
- staged snapshots can be correct while the live pointer is still stale, so operator observability must show the active snapshot id
- heavy diagnostics and operator-state surfaces should be separated so the operator view remains useful even when diagnostics are stale

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

- Local SQLite remains the full historical ingest authority.
- Neon remains the pruned serving database and runtime holdings store.
- `security_master` is the only universe authority.
- The committed universe artifact is `data/reference/security_master_seed.csv`.
- The three canonical source-of-truth tables are:
  - `security_prices_eod`
  - `security_fundamentals_pit`
  - `security_classification_pit`
- cUSE4 core model state is slower-moving than source data.
- Frontend-facing caches are cheap projections and should refresh more often than the core model.
- Holdings changes, price updates, source-data refreshes, and core model recalculations must be treated as different operational events.

## Four-Layer Operating Model

### 1) Universe Layer

Purpose:
- Define which securities the platform cares about.

Authority:
- `security_master`

Key actions:
- add new tickers/RICs
- update metadata / eligibility flags
- mark names in or out of the eligible universe

Rule:
- Universe maintenance is explicit and file-driven.
- No separate universe-builder artifacts should be needed at runtime.
- After approved universe changes, regenerate and commit `data/reference/security_master_seed.csv`.

### 2) Source Data Layer

Purpose:
- Maintain canonical historical prices, PIT fundamentals, and PIT classification.

Authority:
- local `data.db` first
- mirrored and pruned into Neon after refresh

Key actions:
- daily latest-session updates
- targeted subset repairs
- historical backfills for new names

Rule:
- Source tables can be current to latest available session.
- They are not constrained by the cUSE4 lag policy.
- Fundamentals and classification PIT backfills run monthly by default.
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
- `Overview`
  - reads: `/api/portfolio`, `/api/risk`
  - purpose: high-level portfolio, risk split, top holdings
- `Risk` (`/exposures`)
  - reads: `/api/exposures`, `/api/risk`, `/api/portfolio`
  - purpose: factor-level portfolio views plus per-position drilldown
- `Explore`
  - reads: `/api/universe/search`, `/api/universe/ticker/{ticker}`, `/api/universe/ticker/{ticker}/history`, `/api/universe/factors`, `/api/portfolio`
  - purpose: single-name inspection against the cached universe
- `Positions`
  - reads: `/api/holdings/*`, `/api/portfolio`, `/api/universe/search`
  - purpose: holdings editing/import and current model portfolio view
- `Data`
  - reads: `/api/operator/status`, `/api/data/diagnostics`
  - purpose: operator control deck and source/serving diagnostics
- `Health`
  - reads: `/api/operator/status`, `/api/health/diagnostics`
  - purpose: deeper model-diagnostics study, loaded on demand because it is the heaviest dashboard page

Efficiency rules now in force:
- operator state is polled centrally and slowly; pages should not each invent their own background loop
- ticker/RIC typeahead is debounced before hitting `/api/universe/search`
- Health diagnostics are no longer fetched automatically on page load
- serving pages should consume durable serving outputs rather than piecing together raw source tables in the browser
- universe explore/search outputs
- health/diagnostic payloads

Key rule:
- Serving refreshes should be cheap and frequent.
- They should not trigger full cUSE4 recompute unless explicitly requested.
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
- mirror to Neon
- rebuild serving outputs

Should usually not do:
- full raw-history rebuild
- full core recompute

Optional:
- if weekly cadence says core is due, run core recompute afterward

### C) Weekly core update

Examples:
- scheduled weekly cUSE4 refresh
- manual high-confidence model refresh

Should do:
- use current source-of-truth history
- recompute factor returns, covariance, specific risk
- rebuild serving outputs
- mirror/prune/parity to Neon

Should not do by default:
- full historical raw-history rebuild

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

### 2) `source-daily`

Purpose:
- update source-of-truth tables for latest available session without touching core model

Does:
- LSEG ingest for prices + fundamentals + classification
- serving refresh
- Neon mirror/parity/prune

Does not:
- recompute daily factor returns / covariance / specific risk

Primary trigger:
- daily market data update

Current implemented path:
- `source-daily`
- actual live ingest still depends on `ORCHESTRATOR_ENABLE_INGEST=true`
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

1. Merge identifiers into `security_master`
- required: `ric`, `ticker`
- preferred metadata: `isin`, `exchange_name`, optional `sid`/`permid`
- set `classification_ok` and `is_equity_eligible` deliberately

2. Validate the merge
- duplicate RIC check
- blank ticker / blank RIC check
- eligibility count delta

3. Backfill source-of-truth tables for only the new RICs
- prices: full retained local history
- fundamentals: monthly PIT from retained local start to current
- classification: monthly PIT from retained local start to current

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
- parity checks must compare Neon only against the same bounded windows
- if a Neon DSN is configured, holdings-serving reads should resolve from Neon rather than static mock positions

This is already the implemented direction and should remain the rule.

## Frontend Observability Model

The frontend should expose operations by lane, not as one vague status.

### Header-level signals

- backend-authoritative holdings dirty / `RECALC` needed
- refresh running / idle / failed
- Neon sync health

### Data or Health page operator cards

Should show:
- last `serve-refresh`
- last `source-daily`
- last `core-weekly`
- last `cold-core`
- last `universe-add`
- recent run history per lane
- latest stage detail per lane
- latest source dates:
  - prices
  - fundamentals
  - classification
  - core factor returns
- core due status:
  - due / not due
  - reason
- Neon mirror status
- Neon parity status
- latest parity artifact link/path
- runtime warnings when the backend is not operating in the standard Neon-first profile
- fast diagnostics vs deep diagnostics explicitly labeled, so omitted expensive checks are not mistaken for live truth

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
