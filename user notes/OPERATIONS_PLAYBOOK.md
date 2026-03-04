# Barra Dashboard Operations Playbook

## Core Policy
- Risk engine recompute cadence: weekly (`RISK_RECOMPUTE_INTERVAL_DAYS=7` by default).
- Cross-section recency guard: regressions only use exposure snapshots at least 7 calendar days old (`CROSS_SECTION_MIN_AGE_DAYS=7`).
- Loadings/UI cache refresh: can run daily; it reuses latest weekly risk-engine state unless recompute is due.
- Execution model: one orchestrator framework with profile-specific cadence:
  - `daily-fast`
  - `daily-with-core-if-due`
  - `weekly-core`

## Key Commands
- Orchestrated refresh via API (default profile from `mode=full` mapping):
  - `curl -X POST "http://localhost:8000/api/refresh"`
- API refresh explicit daily-fast profile:
  - `curl -X POST "http://localhost:8000/api/refresh?profile=daily-fast"`
- API refresh explicit weekly core recompute:
  - `curl -X POST "http://localhost:8000/api/refresh?profile=weekly-core&force_core=true"`
- API refresh partial stage run:
  - `curl -X POST "http://localhost:8000/api/refresh?profile=daily-fast&from_stage=ingest&to_stage=estu_audit"`
- Orchestrated refresh via CLI module:
  - `PYTHONPATH=backend python3 -m jobs.run_model_pipeline --profile daily-fast`
- Orchestrated refresh via script wrapper:
  - `python3 backend/scripts/run_model_pipeline.py --profile daily-with-core-if-due`
- Resume a previous run id:
  - `python3 backend/scripts/run_model_pipeline.py --profile daily-with-core-if-due --resume-run-id <run_id>`
- Refresh data from LSEG:
  - `python3 backend/scripts/download_data_lseg.py --db-path backend/data.db`
- Bootstrap cUSE4 canonical source tables:
  - `python3 backend/scripts/bootstrap_cuse4_source_tables.py --db-path backend/data.db`
- Build cUSE4 ESTU audit snapshot:
  - `python3 backend/scripts/build_cuse4_estu_membership.py --db-path backend/data.db`

## What Gets Cached
- `risk_engine_meta`: recompute metadata (method version, last recompute date, latest factor-return date, settings).
- `risk_engine_cov`: serialized factor covariance matrix (weekly cache).
- `risk_engine_specific_risk`: stock-level specific risk map (weekly cache).
- `cuse4_foundation`: bootstrap + latest ESTU audit summary for cUSE4 transition layer.
- `portfolio`, `risk`, `exposures`, `universe_loadings`, `universe_factors`, `health_diagnostics`, `eligibility`, `refresh_meta`: refreshed on each `/api/refresh` call.
- `model_outputs_write`: latest relational model-output persistence status.
- `refresh_status`: background orchestrator state snapshot.

## Validation Checklist
- Verify refresh status + orchestrator state:
  - `curl -s "http://localhost:8000/api/refresh/status" | jq`
- Verify latest refresh metadata:
  - `curl -s "http://localhost:8000/api/data/status" | jq '.cache_outputs[] | select(.key==\"refresh_meta\")'`
- Verify risk payload includes engine metadata:
  - `curl -s "http://localhost:8000/api/risk" | jq '.risk_engine'`
- Verify latest usable eligibility summary (regression members > 0 preferred):
  - `sqlite3 backend/cache.db "SELECT date,exp_date,regression_member_n,structural_coverage,regression_coverage FROM daily_universe_eligibility_summary ORDER BY date DESC LIMIT 10;"`
- Verify no compatibility views remain:
  - `sqlite3 backend/data.db "SELECT COUNT(*) FROM sqlite_master WHERE type='view';"`
- Verify no `sid` column remains in canonical time-series tables:
  - `sqlite3 backend/data.db "SELECT 'security_prices_eod', SUM(CASE WHEN name='sid' THEN 1 ELSE 0 END) FROM pragma_table_info('security_prices_eod') UNION ALL SELECT 'security_fundamentals_pit', SUM(CASE WHEN name='sid' THEN 1 ELSE 0 END) FROM pragma_table_info('security_fundamentals_pit') UNION ALL SELECT 'security_classification_pit', SUM(CASE WHEN name='sid' THEN 1 ELSE 0 END) FROM pragma_table_info('security_classification_pit') UNION ALL SELECT 'estu_membership_daily', SUM(CASE WHEN name='sid' THEN 1 ELSE 0 END) FROM pragma_table_info('estu_membership_daily');"`

## Rollback
- Create checkpoint before major changes:
  - `git tag checkpoint-<name>-<yyyymmdd>`
  - `git push origin --tags`
- Revert a bad commit on `main`:
  - `git revert <commit_sha>`
- Restore from checkpoint tag:
  - `git switch -c rollback-<name> <tag_name>`

## Audit-Fix Operations (2026-03-04)
- Orchestrator ingest behavior:
  - Stage `ingest` now always runs bootstrap checks.
  - Live ingest remains opt-in via `ORCHESTRATOR_ENABLE_INGEST=true`.
- Security master key policy:
  - `security_master` is physically keyed by `ric`.
  - `sid`/`permid` are optional metadata only (not required for eligibility).
- Price schema policy:
  - `security_prices_eod` canonical columns are `open/high/low/close/adj_close/volume/currency` (no `exchange` field).
- Risk API readiness gate:
  - `/api/risk` now returns not-ready if covariance/specific-risk payload is incomplete.
- Relational output quality gate:
  - Refresh fails if any required relational model-output table write is empty.

### Maintenance Commands
- Compact DB files:
  - `python3 backend/scripts/compact_sqlite_databases.py backend/data.db backend/cache.db`
- Rebuild raw cross-section history:
  - `python3 backend/scripts/build_barra_raw_cross_section_history.py --db-path backend/data.db --frequency weekly`
- Force clean core recompute:
  - `sqlite3 backend/cache.db "DELETE FROM daily_factor_returns; DELETE FROM daily_specific_residuals; DELETE FROM daily_universe_eligibility_summary;"`
  - `python3 backend/scripts/run_model_pipeline.py --profile weekly-core --from-stage factor_returns --to-stage risk_model --force-core`

### Quick Health Checks
- Style-score completeness (recent):
  - `sqlite3 backend/data.db "SELECT as_of_date, ROUND(100.0*AVG(CASE WHEN beta_score IS NOT NULL AND momentum_score IS NOT NULL AND size_score IS NOT NULL AND value_score IS NOT NULL THEN 1 ELSE 0 END),2) FROM barra_raw_cross_section_history GROUP BY as_of_date ORDER BY as_of_date DESC LIMIT 10;"`
- Eligibility coverage (recent):
  - `sqlite3 backend/cache.db "SELECT date, structural_eligible_n, regression_member_n, ROUND(100.0*regression_coverage,2) FROM daily_universe_eligibility_summary ORDER BY date DESC LIMIT 10;"`
