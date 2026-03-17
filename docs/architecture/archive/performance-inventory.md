# Performance Inventory

Date: 2026-03-17
Status: Active inventory for focused runtime-efficiency work
Owner: Codex

## Scope

This inventory targets runtime efficiency problems that can be improved without changing:

- the current architecture
- source-of-truth rules
- stable operating mode
- authoritative lane behavior

It focuses on repeated work, repeated DB access, unnecessary payload loading, and avoidable
serialization costs.

## Expensive Or Broad Runtime Surfaces

### API-facing services and routes

- `backend/services/portfolio_whatif.py`
  - loads multiple serving payloads separately
  - rebuilds current and hypothetical analytics in one request
  - likely one of the heavier interactive request paths

- `backend/services/dashboard_payload_service.py`
  - cheap per payload, but some routes still load multiple durable payloads separately in one request

- `backend/services/factor_history_service.py`
  - repeated serving-payload catalog lookups
  - fallback path can scan historical factor names from storage

- `backend/services/data_diagnostics_service.py`
  - intentionally diagnostic/deep, but still performs many table-inspection queries and cache scans

- `backend/services/operator_status_service.py`
  - multiple truth-surface reads and source-date reads
  - acceptable for operator use, but still a non-trivial status assembly path

### Serving / refresh path

- `backend/analytics/pipeline.py`
  - already reuses some artifacts, but still performs a large amount of assembly when full refresh runs
  - repeated cache/runtime reads around risk-engine metadata and payload staging remain important to watch

- `backend/analytics/services/universe_loadings.py`
  - expensive full-universe builder
  - heavy dataframe transforms and several Python loops
  - not a safe first-pass optimization target without more correctness review

### Deferred large hotspots

These are clearly expensive, but intentionally deferred for this pass:

- `backend/analytics/health.py`
- `backend/risk_model/daily_factor_returns.py`
- `backend/risk_model/raw_cross_section_history.py`
- `backend/services/neon_mirror.py`

## Repeated DB Access / Round-Trip Patterns

### 1. One connection per payload read in serving outputs

Owner:
- `backend/data/serving_outputs.py`

Current behavior:
- `load_current_payload(...)` opens a separate SQLite or Neon connection per payload name
- routes/services that need multiple payloads pay repeated connection/query overhead

Where this shows up:
- `dashboard_payload_service.load_risk_response()` loads `risk` and `model_sanity` separately
- `portfolio_whatif.preview_portfolio_whatif()` loads `portfolio`, `universe_loadings`, `risk_engine_cov`, and `risk_engine_specific_risk` separately

Impact:
- repeated DB round-trips on request paths that are otherwise just payload assembly

### 2. Repeated catalog payload loading in factor-history route

Owner:
- `backend/services/factor_history_service.py`

Current behavior:
- `_resolve_from_payload_catalog(...)` checks multiple serving payloads serially
- the catalogs are effectively duplicates of the same factor set
- one candidate payload, `universe_loadings`, is much larger than the others

Impact:
- repeated reads of redundant payloads on a route that only needs factor identity metadata

### 3. Diagnostics table stats perform separate min/max queries

Owner:
- `backend/services/data_diagnostics_sqlite.py`

Current behavior:
- `table_stats(...)` issues separate ascending and descending date queries
- `build_data_diagnostics_payload(...)` applies this across multiple tables

Impact:
- avoidable extra queries on a diagnostics surface that is already intentionally query-heavy

## Repeated Transformations / Data Shaping

### Factor-history fallback factor-name resolution

Owner:
- `backend/data/history_queries.py`

Current behavior:
- fallback path can load all distinct factor names from history storage in order to map a token to a stored factor name

Impact:
- acceptable for rare use, but wasteful compared with direct-match resolution when payload catalogs already contain the same mapping

### Risk route normalization and extra payload read

Owner:
- `backend/services/dashboard_payload_service.py`

Current behavior:
- risk payload is normalized every request
- `model_sanity` is loaded as a separate payload

Impact:
- not huge on its own, but a clean batch-load target

## Repeated Work In Pipeline / Serving Assembly

### Full-universe loadings rebuild

Owner:
- `backend/analytics/pipeline.py`
- `backend/analytics/services/universe_loadings.py`

Current behavior:
- when reuse fails, refresh loads full prices, fundamentals, and exposure snapshots, then rebuilds the entire ticker universe

Impact:
- this is expected and correct, but it remains one of the main expensive serving computations

Status for this pass:
- observe and document
- do not aggressively optimize without profiling because correctness risk is higher here

### What-if recomputes current and hypothetical analytics separately

Owner:
- `backend/services/portfolio_whatif.py`

Current behavior:
- current and hypothetical portfolios each run through risk decomposition, risk-mix, and exposure-mode assembly

Impact:
- substantial CPU work per request

Status for this pass:
- keep the math intact
- reduce repeated IO around it first

## Serialization / Payload Size Inefficiencies

### Large payloads loaded only to read small metadata subsets

Examples:
- `factor_history_service` loading multiple serving payloads just to inspect `factor_catalog`
- `portfolio_whatif` loading multiple payloads individually when a single serving-output query could fetch them together

Impact:
- unnecessary JSON decode and connection churn

## High-Confidence Optimization Themes

1. Batch serving-payload reads where one request needs multiple current payloads.
2. Prefer smaller metadata-bearing payloads before larger payloads for catalog lookups.
3. Collapse duplicate diagnostics queries when the same result can be obtained in one SQL round-trip.

## Areas To Leave Alone Unless A Dedicated Profiling Pass Is Requested

- deep `health.py` diagnostics internals
- core model math in `daily_factor_returns.py`
- raw cross-section build logic
- Neon parity/mirror flows

Those are real runtime hotspots, but changing them safely is no longer a low/medium-risk efficiency pass.
