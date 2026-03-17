# Barra Dashboard Operations Playbook

## Core Policy
- Holdings writes and holdings-serving reads are both Neon-authoritative when a Neon DSN is configured.
- Local SQLite is the only direct LSEG landing zone and the optional deep archive on the local machine.
- Neon is the intended authoritative operating database for serving and, once enabled, for core/cold-core rebuilds.
- Model outputs and durable serving payloads now write to Neon first when Neon is configured; local SQLite remains a mirror and local diagnostic surface during migration.
- Operator health/runtime truth is beginning to move to Neon-backed runtime state, but broader analytics cache state is still transitional.
- The Neon-backed runtime-state surface is intentionally small and operator-facing: `risk_engine_meta`, `neon_sync_health`, and the active snapshot pointer.
- `/api/health` and `/api/operator/status` now expose runtime-state status/source fields so missing Neon runtime truth is visible instead of looking healthy by omission.
- `RECALC`/holdings-dirty state is backend-persisted, not browser-local.
- Risk engine recompute cadence: weekly (`RISK_RECOMPUTE_INTERVAL_DAYS=7` by default).
- Cross-section recency guard: regressions only use exposure snapshots at least 7 calendar days old (`CROSS_SECTION_MIN_AGE_DAYS=7`).
- Loadings/UI cache refresh: can run daily; it reuses latest weekly risk-engine state unless recompute is due.
- This is intentional: served holdings, prices, and factor loadings can be fresher than the weekly core risk engine between rebuilds.
- The historical implementation plan for moving deep `health_diagnostics` work off the quick refresh path lives in [HEALTH_DIAGNOSTICS_REFRESH_PLAN.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/barra-dashboard/docs/archive/legacy-plans/HEALTH_DIAGNOSTICS_REFRESH_PLAN.md).
- Current live factor set: 45 total factors, including 14 style factors. `Book-to-Price` and `Earnings Yield` remain; there is no standalone `Value` factor.
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
  - it reuses the current stable core package only when the live cache is both present and current for the active method version
  - if that stable core package is missing, stale, or due for rebuild, `serve-refresh` now fails closed and requires a core lane instead of recomputing factor returns / covariance / specific risk on the serving path
  - holdings-triggered light refreshes now pass an explicit `holdings_only` scope and may reuse the current published `universe_loadings` payload when both of these still match:
    - `source_dates`
    - stable risk-engine fingerprint (`method_version`, `last_recompute_date`, `factor_returns_latest_date`, snapshot-age/lookback settings, specific-risk count)
  - on that same fast path, cached `eligibility` and `cov_matrix` are reused when present instead of being rebuilt from unchanged model state
  - deep `health_diagnostics` are no longer recomputed on the quick path; `serve-refresh` carries forward the last good diagnostics payload or records that diagnostics were deferred
  - when that reuse path is active, relational `model_outputs` persistence is skipped because the core model state is unchanged; serving payload persistence still runs normally
  - manual `serve-refresh` without that scope keeps the existing full serving-refresh behavior.
  - serving-time prices remain read-only inputs to the projection layer; they must never write into canonical historical model-estimation tables such as `security_prices_eod`
- `source-daily`: local LSEG ingest into SQLite for the latest completed XNYS session, repair any missing daily price sessions up to that session, purge open-month PIT rows, backfill any missing closed-month fundamentals/classification anchors, publish the retained working window into Neon, then refresh serving only.
- `source-daily-plus-core-if-due`: default daily maintenance lane; local ingest + Neon source-sync first, then recompute core only when cadence/version says due.
- `core-weekly`: force core recompute without rebuilding full raw history.
  - factor-return recompute now determines uncached dates before loading prices and only reads the bounded price window needed for those dates plus the immediately prior session.
  - this lane now owns deep `health_diagnostics` recompute for the current weekly core model state.
- `cold-core`: full historical reset for structural data changes (new/changed historical prices, volume, fundamentals, classification, or factor methodology).
  - This path rebuilds `barra_raw_cross_section_history` over full history and clears core cache tables before recomputing factor returns/risk.
  - This lane is an explicit operator/API path; it is not exposed as a one-click dashboard control in the current frontend.
  - During that run, `serving_refresh` must read the local/workspace source tables that just produced the rebuilt raw history; otherwise it can publish stale Neon factor-loadings metadata before the broad Neon mirror catches up.
  - During ordinary `serve-refresh`, the published weekly core-state should come from the latest durable `model_run_metadata` rather than a stale runtime cache key; otherwise a quick refresh can republish fresh loadings with regressed core metadata.
  - this lane also owns deep `health_diagnostics` recompute for structural rebuilds.
- `universe-add`: finalization lane after explicit `security_master` merge and targeted source backfills for new names.

Rebuild-authority rule:
- While `NEON_AUTHORITATIVE_REBUILDS=false`, `core-weekly` and `cold-core` still rebuild from local SQLite. Run `source-daily` first if you need the latest local ingest reflected.
- If Neon ever gets ahead of the intended `source-daily` target date because of a premature/invalid session stamp, rerunning `source-daily` now heals that state instead of refusing the publish.
- When `NEON_AUTHORITATIVE_REBUILDS=true`, those rebuild lanes execute in this order:
  - `source_sync`: publish source tables from local SQLite into Neon without downgrading newer Neon source dates
  - `neon_readiness`: validate Neon table coverage/retention and materialize a scratch SQLite rebuild workspace from Neon
  - core stages run from that Neon-backed scratch workspace
  - final mirror publishes rebuilt analytics back into Neon
  - local derived tables/cache are refreshed from the scratch workspace so the private mirror stays congruent
- In both cases, local SQLite remains the only direct LSEG ingress point.

Runtime-role rule:
- `local-ingest`: all lanes may be used.
- `cloud-serve`: only `serve-refresh` is allowed.
- In `cloud-serve`, a bare `POST /api/refresh` now defaults safely to `serve-refresh`.
- Explicit deeper lanes remain blocked in `cloud-serve` even if requested by old mode-based callers.

## Operator UI Policy
- Health page is the primary control room.
- Fast diagnostics are the default because they are cheap and always available.
- Deep diagnostics are on-demand and compute exact row counts, ticker counts, duplicate checks, and update metadata.
- Data page is for source-table lineage, coverage, cache surfaces, and integrity diagnostics.
- Operator Status and header health are the live runtime truth.
- Data/Health diagnostics are deeper local-instance maintenance panels and may lag the cloud-serving view.
- Health now shows compact per-lane status cards plus runtime/source-recency cards.
- Health page refresh prompts are intentionally split:
  - fresher loadings can be addressed with `serve-refresh`
  - core rebuild due states should point the operator to `core-weekly` / `cold-core`, not imply that a quick refresh can fix them
- Exposures and Positions should rely on the shared frontend truth banner for snapshot/loadings/core dates rather than reassembling those dates independently per page.
- Header refresh UI is intentionally a single context-aware quick action so `serve-refresh` is not duplicated under multiple top-bar buttons.
- Lane-specific refresh controls and detailed run-history drilldowns are currently API/CLI-driven rather than exposed directly in the frontend.

## Local App Lifecycle
- Preferred local launch path: `make app-up`
- Stop local app cleanly: `make app-down`
- Restart from a clean state: `make app-restart`
- Verify backend/frontend/proxy health: `make app-check`
- Show tracked PIDs, URLs, and log paths: `make app-status`
- Canonical launcher scripts live under `scripts/local_app/` and write runtime state under `backend/runtime/local_app/`.

## Key Commands
- Orchestrated refresh via API:
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
- API refresh partial stage run:
  - `curl -X POST "http://localhost:8000/api/refresh?profile=source-daily-plus-core-if-due&from_stage=ingest&to_stage=risk_model"`
  - if `NEON_AUTHORITATIVE_REBUILDS=true` and you target core stages explicitly, include `neon_readiness` in the window or the run will fail closed before core work starts
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
  - Explicit `--tickers`, `--rics`, and index-derived names only operate on instruments already present in `security_master`; the command now reports any requested names that were not seeded there.
- Repair historical volume coverage only (writes `TR.Volume` into `security_prices_eod.volume`):
  - `python3 -m backend.scripts.backfill_prices_range_lseg --db-path backend/runtime/data.db --start-date 2012-01-03 --end-date 2026-03-04 --volume-only --only-null-volume`
  - Explicit `--rics` repairs likewise only target seeded `security_master` rows and report unmatched requested RICs in the result payload.
- PIT history backfill (monthly/quarterly anchor dates for fundamentals/classification, with optional sparse anchor-date prices):
  - `python3 -m backend.scripts.backfill_pit_history_lseg --db-path backend/runtime/data.db --start-date 2018-01-01 --end-date 2026-03-04 --frequency monthly`
  - This is not a substitute for full daily price-history repair. Use `backfill_prices_range_lseg.py` when you need continuous daily `security_prices_eod` history for raw-history rebuilds or factor-return recompute coverage.
- Bootstrap cUSE4 canonical source tables:
  - `python3 -m backend.scripts.bootstrap_cuse4_source_tables --db-path backend/runtime/data.db`
- Build cUSE4 ESTU audit snapshot:
  - `python3 -m backend.scripts.build_cuse4_estu_membership --db-path backend/runtime/data.db`

## What Gets Cached
- Refresh outputs are staged under a run snapshot and become live only when the snapshot pointer is published.
  - This prevents partial live state if refresh fails mid-run.
  - Old staged snapshots are pruned automatically; tune with `SQLITE_CACHE_SNAPSHOT_RETENTION` (default `3`).
- `risk_engine_meta`: recompute metadata (method version, last recompute date, latest factor-return date, settings).
  - factor-return cache invalidation now also tracks `CROSS_SECTION_MIN_AGE_DAYS` so snapshot-age policy changes clear stale factor-return/residual/eligibility rows.
- `risk_engine_cov`: serialized factor covariance matrix (weekly cache).
- `risk_engine_specific_risk`: stock-level specific risk map (weekly cache).
- `daily_factor_returns`: factor-return workspace table in `cache.db`, now including `robust_se` and `t_stat`.
- Health regression diagnostics prefer stored `t_stat`; the older proxy path remains only as a compatibility fallback for historical rows that predate the widened inference fields.
- `daily_specific_residuals`: residual workspace table in `cache.db`, now storing both `model_residual` and `raw_residual`.
- `cuse4_foundation`: bootstrap + latest ESTU audit summary for cUSE4 transition layer.
- `portfolio`, `risk`, `exposures`, `universe_loadings`, `universe_factors`, `health_diagnostics`, `eligibility`, `refresh_meta`: refreshed on each `/api/refresh` call.
  - `health_diagnostics` is persisted into the durable serving surface, but quick refreshes carry it forward while core lanes refresh it.
  - `/api/health/diagnostics` prefers the durable current payload before falling back to cache.
- if Neon-backed holdings cannot be read during serving projection, refresh fails instead of publishing an empty-success portfolio payload
- `model_outputs_write`: latest relational model-output persistence status.
  - for the holdings-only fast path, this now reports `status=skipped` with reason `holdings_only_fast_path`.
  - when model persistence does run, factor returns now load incrementally from the latest durable date when the risk-engine fingerprint still matches; schema/method drift falls back to a full reload.
- durable serving payload writes now default to partial upsert semantics.
  - only the canonical serving-refresh writer opts into `replace_all=true`, which keeps destructive delete behavior explicit instead of implicit.
- `refresh_status`: background orchestrator state snapshot.
  - includes current stage progress for in-flight runs (`current_stage`, `stage_index`, `stage_count`, `stage_started_at`) and the optional `refresh_scope` used by holdings-triggered refreshes.
- operator lane summaries expose the latest persisted run state, while richer in-flight stage progress remains part of `refresh_status` and backend/operator diagnostics.

## Factor-Return Durability And Parity
- Durable SQLite factor-return persistence now replaces stale date slices instead of only writing rows from the latest durable date forward.
- Durable SQLite covariance persistence now prunes retired factor names from `model_factor_covariance_daily` so removed factors do not survive across later runs.
- Neon factor-return sync now carries `robust_se` and `t_stat` into `model_factor_returns_daily`.
- Bounded Neon parity for factor returns now checks:
  - required column presence
  - non-null coverage for inference columns
  - per-date factor-count parity on sampled dates
  - sampled row-value equality, not just row counts and date windows

## Lookback Retention Policy
- Treat three horizons separately:
  - active Barra model history: the retained `barra_raw_cross_section_history` window that drives factor-return recomputes
  - risk-model lookback: the rolling covariance/specific-risk window (`LOOKBACK_DAYS`, currently ~2 trading years)
  - source archive retention: deeper local history plus Neon publish retention
- Ordinary `core-weekly` recomputes respect the active Barra model-history floor from `barra_raw_cross_section_history`; they do not try to backfill the full price archive.
- The first usable factor-return date may be a few sessions later than raw-history start because cross-sectional regressions honor `CROSS_SECTION_MIN_AGE_DAYS`.
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
  - Deeper local source archives are allowed and expected; they do not, by themselves, widen the active Barra model window.
  - Neon is the pruned publish surface:
    - source tables: rolling 10 years
    - analytics tables: rolling 5 years

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
    - `.runtime.source_authority`
    - `.runtime.rebuild_authority`
    - lane metadata shows `source_sync_required` / `neon_readiness_required` for Neon-authoritative core lanes
    - `.runtime.serving_outputs_primary_reads_effective`
    - `.runtime.neon_auto_sync_enabled_effective`
    - `.source_dates` is the authoritative operating view
    - `.local_archive_source_dates` matches or intentionally exceeds Neon after local ingest
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
- Verify `security_master` no longer carries deprecated compatibility metadata:
  - `sqlite3 backend/runtime/data.db "SELECT name FROM pragma_table_info('security_master') WHERE name IN ('sid','permid','instrument_type','asset_category_description');"`

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
  - Orchestrator live ingest runs as one full-universe pass; use the direct LSEG ingest script for any manual shard/chunk workflow.
- Security master key policy:
  - `security_master` is physically keyed by `ric`.
  - deprecated `sid`/`permid` and dead instrument metadata were removed from the canonical schema.
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
  - `sqlite3 backend/runtime/data.db "SELECT as_of_date, ROUND(100.0*AVG(CASE WHEN beta_score IS NOT NULL AND momentum_score IS NOT NULL AND size_score IS NOT NULL AND book_to_price_score IS NOT NULL THEN 1 ELSE 0 END),2) FROM barra_raw_cross_section_history GROUP BY as_of_date ORDER BY as_of_date DESC LIMIT 10;"`
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
