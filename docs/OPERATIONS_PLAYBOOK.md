# Barra Dashboard Operations Playbook

## Core Policy
- Holdings writes and holdings-serving reads are both Neon-authoritative when a Neon DSN is configured.
- `RECALC`/holdings-dirty state is backend-persisted, not browser-local.
- Risk engine recompute cadence: weekly (`RISK_RECOMPUTE_INTERVAL_DAYS=7` by default).
- Cross-section recency guard: regressions only use exposure snapshots at least 7 calendar days old (`CROSS_SECTION_MIN_AGE_DAYS=7`).
- Loadings/UI cache refresh: can run daily; it reuses latest weekly risk-engine state unless recompute is due.
- Execution model: one orchestrator framework with profile-specific cadence:
  - `serve-refresh`
  - `source-daily`
  - `source-daily-plus-core-if-due`
  - `core-weekly`
  - `cold-core` (full historical rebuild path)
  - `universe-add` (post-onboarding finalization lane)

## Hobby Launch Profile (Low Cost, 1-2 Users)
- Run a single backend process/worker only.
- Keep SQLite local and persistent on disk (no shared multi-node writes).
- Runtime DB location defaults to `backend/runtime/` (`data.db`, `cache.db`).
- Legacy paths `backend/data.db` and `backend/cache.db` may be symlinks for command compatibility.
- In `cloud-serve` mode, set non-empty auth tokens before exposing the app online:
  - `OPERATOR_API_TOKEN`
  - `EDITOR_API_TOKEN`
  - `REFRESH_API_TOKEN` only as a legacy fallback if you intentionally want the refresh proxy to reuse it
- Prefer manual or low-frequency refreshes (`serve-refresh` most days).
- Keep daily file backups of `data.db` and `cache.db`.
- Production backend command:
  - `BACKEND_WORKERS=1 uvicorn backend.main:app --host 0.0.0.0 --port 8000 --workers 1`
  - or `make backend-prod`

## Volume Pull Policy
- Canonical daily OHLCV ingest (`download_data_lseg.py`) maps `volume` from `TR.Volume`.
- Historical volume-repair path (`backfill_prices_range_lseg.py --volume-only`) maps `volume` from `TR.Volume`.
- Use `--only-null-volume` for targeted repairs so existing populated rows are not rewritten.
- After a broad historical volume repair, run `cold-core` refresh to rebuild raw cross-sections and risk caches from the updated volume series.

## Refresh Paths (When To Use)
- `serve-refresh`: quick serving refresh; no core recompute and no source ingest.
- `source-daily`: latest-source ingest plus serving refresh only.
- `source-daily-plus-core-if-due`: default daily maintenance lane; recomputes core only when cadence/version says due.
- `core-weekly`: force core recompute without rebuilding full raw history.
- `cold-core`: full historical reset for structural data changes (new/changed historical prices, volume, fundamentals, classification, or factor methodology).
  - This path rebuilds `barra_raw_cross_section_history` over full history and clears core cache tables before recomputing factor returns/risk.
  - UI now requires explicit confirmation before starting this lane from the operator deck.
- `universe-add`: finalization lane after explicit `security_master` merge and targeted source backfills for new names.

Runtime-role rule:
- `local-ingest`: all lanes may be used.
- `cloud-serve`: only `serve-refresh` is allowed.

## Operator UI Policy
- Data page is the primary control room.
- Fast diagnostics are the default because they are cheap and always available.
- Deep diagnostics are on-demand and compute exact row counts, ticker counts, duplicate checks, and update metadata.
- Health page is for deeper model diagnostics, not routine operator actions.
- Operator lane cards show:
  - plain-English lane purpose
  - latest run state
  - recent-run history strip
  - stage-level detail
  - separate Neon mirror and Neon parity status

## Local App Lifecycle
- Preferred local launch path: `make app-up`
- Stop local app cleanly: `make app-down`
- Restart from a clean state: `make app-restart`
- Verify backend/frontend/proxy health: `make app-check`
- Show tracked PIDs, URLs, and log paths: `make app-status`
- Canonical launcher scripts live under `scripts/local_app/` and write runtime state under `backend/runtime/local_app/`.

## Key Commands
- Orchestrated refresh via API (default profile from `mode=full` mapping):
  - `curl -X POST "http://localhost:8000/api/refresh"`
- API refresh explicit serve-refresh profile:
  - `curl -X POST "http://localhost:8000/api/refresh?profile=serve-refresh"`
- Cloud-mode authenticated serve-refresh:
  - `curl -X POST "http://localhost:8000/api/refresh?profile=serve-refresh" -H "X-Operator-Token: $OPERATOR_API_TOKEN"`
- API refresh explicit source-daily profile:
  - `curl -X POST "http://localhost:8000/api/refresh?profile=source-daily"`
- API refresh explicit weekly core recompute:
  - `curl -X POST "http://localhost:8000/api/refresh?profile=core-weekly&force_core=true"`
- API refresh explicit cold-core rebuild:
  - `curl -X POST "http://localhost:8000/api/refresh?profile=cold-core"`
- API refresh cold mode shortcut:
  - `curl -X POST "http://localhost:8000/api/refresh?mode=cold"`
- API refresh partial stage run:
  - `curl -X POST "http://localhost:8000/api/refresh?profile=source-daily-plus-core-if-due&from_stage=ingest&to_stage=risk_model"`
- Orchestrated refresh via CLI module:
  - `python3 -m backend.scripts.run_model_pipeline --profile serve-refresh`
- Orchestrated refresh via script wrapper:
  - `python3 -m backend.scripts.run_model_pipeline --profile source-daily-plus-core-if-due`
- Source-only refresh via script wrapper:
  - `python3 -m backend.scripts.run_model_pipeline --profile source-daily`
- Cold-core refresh via script wrapper:
  - `python3 -m backend.scripts.run_model_pipeline --profile cold-core`
- Resume a previous run id:
  - `python3 -m backend.scripts.run_model_pipeline --profile source-daily-plus-core-if-due --resume-run-id <run_id>`
- Refresh data from LSEG:
  - `python3 -m backend.scripts.download_data_lseg --db-path backend/runtime/data.db`
- Repair historical volume coverage only (writes `TR.Volume` into `security_prices_eod.volume`):
  - `python3 -m backend.scripts.backfill_prices_range_lseg --db-path backend/runtime/data.db --start-date 2012-01-03 --end-date 2026-03-04 --volume-only --only-null-volume`
- Bootstrap cUSE4 canonical source tables:
  - `python3 -m backend.scripts.bootstrap_cuse4_source_tables --db-path backend/runtime/data.db`
- Build cUSE4 ESTU audit snapshot:
  - `python3 -m backend.scripts.build_cuse4_estu_membership --db-path backend/runtime/data.db`

## What Gets Cached
- Refresh outputs are staged under a run snapshot and become live only when the snapshot pointer is published.
  - This prevents partial live state if refresh fails mid-run.
  - Old staged snapshots are pruned automatically; tune with `SQLITE_CACHE_SNAPSHOT_RETENTION` (default `3`).
- `risk_engine_meta`: recompute metadata (method version, last recompute date, latest factor-return date, settings).
- `risk_engine_cov`: serialized factor covariance matrix (weekly cache).
- `risk_engine_specific_risk`: stock-level specific risk map (weekly cache).
- `cuse4_foundation`: bootstrap + latest ESTU audit summary for cUSE4 transition layer.
- `portfolio`, `risk`, `exposures`, `universe_loadings`, `universe_factors`, `health_diagnostics`, `eligibility`, `refresh_meta`: refreshed on each `/api/refresh` call.
- `model_outputs_write`: latest relational model-output persistence status.
- `refresh_status`: background orchestrator state snapshot.

## Lookback Retention Policy
- Think in terms of target factor-return history horizon `H` (years).
- If you need to recompute and keep `H` years of factor returns, retain at least `H` years in:
  - `barra_raw_cross_section_history`
  - `security_prices_eod`
  - `security_fundamentals_pit`
  - `security_classification_pit`
- Keep factor-return outputs for at least `H` years in:
  - `cache.db.daily_factor_returns` (serving/history APIs)
  - `data.db.model_factor_returns_daily` (durable relational output)
- Important: `cold-core` clears core caches (`daily_factor_returns`, `daily_specific_residuals`) before recompute.
  - After a cold-core run, your retained history is bounded by the source/raw tables above.
  - If source/raw history is shorter than `H`, factor-return history will also be shorter than `H`.
- Practical rule:
  - For a 5-year history target (example: as of 2026-03-05, keep data from ~2021-03-05 onward), keep at least 5 years in the source/raw tables.
  - Add extra buffer if you rebuild raw descriptors from prices (rolling feature construction benefits from pre-window data).

### Storage vs Recompute Tradeoff
- Keep long source/raw history:
  - Pros: can recompute long history after methodology/data changes.
  - Cons: largest disk footprint (`barra_raw_cross_section_history` and `security_prices_eod` dominate).
- Keep only recent source/raw history:
  - Pros: much smaller `data.db`.
  - Cons: cold-core cannot regenerate older factor-return history.
- `VACUUM` guidance:
  - `VACUUM` only shrinks after deletes/pruning.
  - Run pruning first, then `VACUUM`.

## Validation Checklist
- Verify refresh status + orchestrator state:
  - `curl -s "http://localhost:8000/api/refresh/status" | jq`
- Verify operator lane matrix:
  - `curl -s "http://localhost:8000/api/operator/status" | jq`
  - Confirm:
    - `.runtime.app_runtime_role`
    - `.runtime.allowed_profiles`
    - `.runtime.serving_outputs_primary_reads_effective`
    - `.runtime.neon_auto_sync_enabled_effective`
- One-command operator check:
  - `make operator-check`
  - or `./scripts/operator_check.sh`
  - If Neon auto-sync is disabled, this check degrades gracefully instead of failing on missing parity artifacts.
- Verify latest refresh metadata:
  - `curl -s "http://localhost:8000/api/data/diagnostics" | jq '.cache_outputs[] | select(.key==\"refresh_meta\")'`
- Verify risk payload includes engine metadata:
  - `curl -s "http://localhost:8000/api/risk" | jq '.risk_engine'`
- Verify latest usable eligibility summary (regression members > 0 preferred):
  - `sqlite3 backend/runtime/cache.db "SELECT date,exp_date,regression_member_n,structural_coverage,regression_coverage FROM daily_universe_eligibility_summary ORDER BY date DESC LIMIT 10;"`
- Verify no compatibility views remain:
  - `sqlite3 backend/runtime/data.db "SELECT COUNT(*) FROM sqlite_master WHERE type='view';"`
- Verify no `sid` column remains in canonical time-series tables:
  - `sqlite3 backend/runtime/data.db "SELECT 'security_prices_eod', SUM(CASE WHEN name='sid' THEN 1 ELSE 0 END) FROM pragma_table_info('security_prices_eod') UNION ALL SELECT 'security_fundamentals_pit', SUM(CASE WHEN name='sid' THEN 1 ELSE 0 END) FROM pragma_table_info('security_fundamentals_pit') UNION ALL SELECT 'security_classification_pit', SUM(CASE WHEN name='sid' THEN 1 ELSE 0 END) FROM pragma_table_info('security_classification_pit') UNION ALL SELECT 'estu_membership_daily', SUM(CASE WHEN name='sid' THEN 1 ELSE 0 END) FROM pragma_table_info('estu_membership_daily');"`

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
  - `python3 -m backend.scripts.compact_sqlite_databases backend/runtime/data.db backend/runtime/cache.db`
- Rebuild raw cross-section history (targeted/incremental):
  - `python3 -m backend.scripts.build_barra_raw_cross_section_history --db-path backend/runtime/data.db --frequency weekly`
- Force clean core recompute + full historical raw rebuild (preferred cold path):
  - `python3 -m backend.scripts.run_model_pipeline --profile cold-core`
- Prune old history to a lookback horizon (dry run):
  - `python3 -m backend.scripts.prune_history_by_lookback --years 5 --dry-run`
- Prune old history to a lookback horizon + reclaim disk:
  - `python3 -m backend.scripts.prune_history_by_lookback --years 5 --apply --vacuum`

### Quick Health Checks
- Style-score completeness (recent):
  - `sqlite3 backend/runtime/data.db "SELECT as_of_date, ROUND(100.0*AVG(CASE WHEN beta_score IS NOT NULL AND momentum_score IS NOT NULL AND size_score IS NOT NULL AND value_score IS NOT NULL THEN 1 ELSE 0 END),2) FROM barra_raw_cross_section_history GROUP BY as_of_date ORDER BY as_of_date DESC LIMIT 10;"`
- Eligibility coverage (recent):
  - `sqlite3 backend/runtime/cache.db "SELECT date, structural_eligible_n, regression_member_n, ROUND(100.0*regression_coverage,2) FROM daily_universe_eligibility_summary ORDER BY date DESC LIMIT 10;"`

## Retention Tooling
- The pruning CLI enforces lookback retention across both `data.db` and `cache.db`.
- Safety default:
  - It runs in dry-run mode unless `--apply` is provided.
- Tables currently pruned by lookback:
  - `data.db`: `barra_raw_cross_section_history`, `security_prices_eod`, `security_fundamentals_pit`, `security_classification_pit`, `model_factor_returns_daily`
  - `cache.db`: `daily_factor_returns`, `daily_specific_residuals`, `daily_universe_eligibility_summary`
- Safety:
  - Start with `--dry-run` to inspect row counts.
  - Use `--vacuum` only after a non-dry-run prune to reclaim file size.
