# Barra Dashboard Operations Playbook

## Core Policy
- Risk engine recompute cadence: weekly (`RISK_RECOMPUTE_INTERVAL_DAYS=7` by default).
- Cross-section recency guard: regressions only use exposure snapshots at least 7 calendar days old (`CROSS_SECTION_MIN_AGE_DAYS=7`).
- Loadings/UI cache refresh: can run daily; it reuses latest weekly risk-engine state unless recompute is due.

## Key Commands
- Refresh analytics (normal):
  - `curl -X POST "http://localhost:8000/api/refresh"`
- Force a risk-engine recompute now:
  - `curl -X POST "http://localhost:8000/api/refresh?force_risk_recompute=true"`
- Refresh data from LSEG:
  - `python backend/scripts/download_data_lseg.py --db-path backend/data.db`
- Build canonical universe eligibility (PermID keyed):
  - `python backend/scripts/build_universe_eligibility_lseg.py --db-path backend/data.db`
- Bootstrap cUSE4 canonical source tables:
  - `python backend/scripts/bootstrap_cuse4_source_tables.py --db-path backend/data.db`
- Build cUSE4 ESTU audit snapshot:
  - `python backend/scripts/build_cuse4_estu_membership.py --db-path backend/data.db`
- Reset universe + source tables and rebuild current source-of-truth snapshot:
  - `python backend/scripts/reset_and_rebuild_source_of_truth.py --db-path backend/data.db --current-chain-ric .dMIUS000I0PUS --historical-index-ric .dMIUS000I0PUS --historical-date 2019-03-02`
- TRBC historical backfill in 4 shards:
  - `python backend/scripts/backfill_trbc_history_lseg.py --db-path backend/data.db --shard-count 4 --shard-index 0 --skip-sync`
  - `python backend/scripts/backfill_trbc_history_lseg.py --db-path backend/data.db --shard-count 4 --shard-index 1 --skip-sync`
  - `python backend/scripts/backfill_trbc_history_lseg.py --db-path backend/data.db --shard-count 4 --shard-index 2 --skip-sync`
  - `python backend/scripts/backfill_trbc_history_lseg.py --db-path backend/data.db --shard-count 4 --shard-index 3`

## What Gets Cached
- `risk_engine_meta`: recompute metadata (method version, last recompute date, latest factor-return date, settings).
- `risk_engine_cov`: serialized factor covariance matrix (weekly cache).
- `risk_engine_specific_risk`: stock-level specific risk map (weekly cache).
- `cuse4_foundation`: bootstrap + latest ESTU audit summary for cUSE4 transition layer.
- `portfolio`, `risk`, `exposures`, `universe_loadings`, `universe_factors`, `health_diagnostics`, `eligibility`, `refresh_meta`: refreshed on each `/api/refresh` call.

## Validation Checklist
- Verify refresh status + engine state:
  - `curl -s "http://localhost:8000/api/refresh" | jq`
- Verify risk payload includes engine metadata:
  - `curl -s "http://localhost:8000/api/risk" | jq '.risk_engine'`
- Verify latest usable eligibility summary (regression members > 0 preferred):
  - `sqlite3 backend/cache.db "SELECT date,exp_date,regression_member_n,structural_coverage,regression_coverage FROM daily_universe_eligibility_summary ORDER BY date DESC LIMIT 10;"`
- Verify no unresolved TRBC placeholders in strict model path:
  - `sqlite3 backend/data.db "SELECT COUNT(*) FROM trbc_industry_history WHERE trbc_industry_group IN ('Unmapped','unmapped');"`

## Rollback
- Create checkpoint before major changes:
  - `git tag checkpoint-<name>-<yyyymmdd>`
  - `git push origin --tags`
- Revert a bad commit on `main`:
  - `git revert <commit_sha>`
- Restore from checkpoint tag:
  - `git switch -c rollback-<name> <tag_name>`
