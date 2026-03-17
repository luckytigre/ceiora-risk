# Data Health Check Plan

Date: 2026-03-16
Mode: Read-only investigation

## Scope

Run an ad hoc, non-destructive health check across the local source archive, current model outputs, and current serving/runtime truth surfaces.

No database writes.
No production-data modification.
Only read-only queries, API inspection, and in-memory / local scratch comparison work.

## Key Tables / Surfaces

### Security master / source tables
- `security_master`
- `security_prices_eod`
- `security_fundamentals_pit`
- `security_classification_pit`

### Universe / eligibility / exposures
- `barra_raw_cross_section_history`
- `estu_membership_daily`
- `universe_cross_section_snapshot`
- served `universe_loadings` payload in `serving_payload_current`

### Model outputs
- `model_factor_returns_daily`
- `model_factor_covariance_daily`
- `model_specific_risk_daily`
- `model_run_metadata`

### Pipeline / snapshot metadata
- `job_run_status`
- `serving_payload_current`
- `runtime_state_current`

## Planned Checks

1. Coverage / freshness
- latest dates, row counts, near-full coverage dates
- stale or empty latest snapshots
- sudden count drops

2. Join / key integrity
- orphaned `ric` rows
- current raw-cross-section rows missing security-master joins
- eligible / projectable names missing served exposures

3. Numerical sanity
- all-zero / all-null factor series
- implausible risk-share dominance
- covariance / specific-risk completeness
- suspicious flatlines or repeated identical values

4. Trend / anomaly scan
- row-count history by date
- eligible-universe / regression-member trends
- factor coverage trends

5. LSEG spot checks
- at least 20 securities
- reproducible random seed
- multiple dates across the available range
- compare DB vs fresh LSEG pulls for price, market cap, exchange / country / classification-relevant fields

6. Targeted known-issue tracing
- valid tickers marked ineligible
- factor views zero except Beta
- risk attribution implausibility
- Data Pipeline cards showing `-`

## Output Files

- `docs/architecture/data-health-check-lseg-spot-checks.md`
- `docs/architecture/data-health-check-findings.md`
- `docs/architecture/data-health-check-summary.md`
