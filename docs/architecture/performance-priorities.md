# Performance Priorities

Date: 2026-03-17
Status: Ranked implementation priorities
Owner: Codex

## Ranking Criteria

Each candidate is ranked by:

- likely impact
- implementation risk
- complexity
- confidence

## Top Priority

### 1. Batch serving-payload reads for multi-payload request paths

Targets:
- `backend/data/serving_outputs.py`
- `backend/services/portfolio_whatif.py`
- `backend/services/dashboard_payload_service.py`

Why:
- repeated connection setup and repeated single-row payload queries are pure overhead
- the underlying data already lives in one durable payload table
- this improves request paths without changing source-of-truth behavior

Likely impact:
- High on interactive request efficiency

Risk:
- Low-Medium

Confidence:
- High

Validation needed:
- serving output tests
- what-if tests
- dashboard payload/risk route tests

## Second Priority

### 2. Reduce redundant payload reads in factor-history resolution

Targets:
- `backend/services/factor_history_service.py`

Why:
- route only needs factor identity metadata
- current code may walk several duplicate payload catalogs
- smaller payloads can satisfy the lookup first

Likely impact:
- Medium

Risk:
- Low

Confidence:
- High

Validation needed:
- exposure-history route tests
- factor-history fallback tests

## Third Priority

### 3. Collapse duplicate diagnostics queries in table stats

Targets:
- `backend/services/data_diagnostics_sqlite.py`

Why:
- diagnostics intentionally query a lot, but min/max date lookups do not need two separate queries

Likely impact:
- Medium on diagnostics route

Risk:
- Low

Confidence:
- High

Validation needed:
- diagnostics route tests

## Lower Priority / Observe Only

### 4. Reduce fallback factor-name scans in `history_queries.py`

Why lower:
- less common route
- some edge cases, especially punctuated industry names, still rely on broader matching behavior

Likely impact:
- Medium-Low

Risk:
- Medium

Confidence:
- Medium

Action:
- review after the first optimization pass

### 5. Full-universe loadings build optimization

Why lower:
- high-impact path, but higher correctness risk
- likely requires dataframe and eligibility-path review rather than a small code change

Likely impact:
- High

Risk:
- Medium-High

Confidence:
- Medium-Low

Action:
- document as hotspot, defer unless profiling proves it is the main bottleneck

### 6. Deep health diagnostics optimization

Why lower:
- deferred legacy area
- not a stable request path to optimize opportunistically

Likely impact:
- Medium

Risk:
- High

Confidence:
- High to defer

## Planned Execution Order

1. Batch serving-payload reads where one request needs several current payloads.
2. Reduce redundant factor-history payload reads.
3. Collapse duplicate diagnostics min/max queries.
4. Re-review request paths and decide whether a second tier of batch-loading or targeted query reduction is justified.
