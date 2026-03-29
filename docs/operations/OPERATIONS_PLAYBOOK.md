# cUSE Operations Playbook

## Core Policy
- Holdings writes and holdings-serving reads are both Neon-authoritative when a Neon DSN is configured.
- Local SQLite is the only direct LSEG landing zone and the optional deep archive on the local machine.
- Neon is the authoritative operating database for serving and, by default when Neon is the active backend with a configured DSN, for core/cold-core rebuilds too.
- Model outputs and durable serving payloads now write to Neon first when Neon is configured; local SQLite remains a mirror and local diagnostic surface during migration.
- Durable `model_outputs` reads are now interpreted by contract:
  - rebuild/runtime consumers should use the current rebuild-authority surface
  - data diagnostics should read only the local SQLite archive surface
- Operator health/runtime truth now persists through the Neon-backed runtime-state surface for the core operator/control-room keys, while broader analytics cache state remains transitional.
- The Neon-backed runtime-state surface is intentionally operator-facing: `risk_engine_meta`, `neon_sync_health`, `refresh_status`, `holdings_sync_state`, and the active snapshot pointer.
- `/api/health` and `/api/operator/status` now expose runtime-state status/source fields so missing Neon runtime truth is visible instead of looking healthy by omission.
- `RECALC`/holdings-dirty state is backend-persisted, not browser-local.
- Risk engine recompute cadence: weekly (`RISK_RECOMPUTE_INTERVAL_DAYS=7` by default).
- Cross-section recency guard: regressions only use exposure snapshots at least 7 calendar days old (`CROSS_SECTION_MIN_AGE_DAYS=7`).
- Loadings/UI cache refresh: can run daily; it reuses latest weekly risk-engine state unless recompute is due.
- This is intentional: served holdings, prices, and factor loadings can be fresher than the weekly core risk engine between rebuilds.
- Non-core served exposures now use two explicit methodologies:
  - `Fundamental Projection` for single-name equities carried by descriptor/fundamental scoring outside the US-core ESTU
  - `Returns Projection` for ETFs/ETPs derived from durable `model_factor_returns_daily`
- Returns-projection outputs are a core-bound derived surface: they persist into `projected_instrument_*`, sync through the normal Neon stage-2 table flow, and remain frozen with the active core package until the next core lane refreshes them.
- Canonical source-read ownership is split intentionally:
  - `backend/data/core_reads.py` is the higher public facade used by cUSE analytics and route-facing payload assembly.
  - `backend/data/source_reads.py` remains the public source-domain facade.
  - `backend/data/source_read_authority.py` is the lower registry-first helper layer and should not be imported directly from higher layers.
- The historical implementation plan for moving deep `health_diagnostics` work off the quick refresh path lives in [HEALTH_DIAGNOSTICS_REFRESH_PLAN.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/archive/legacy-plans/HEALTH_DIAGNOSTICS_REFRESH_PLAN.md).
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
- Cloud-native prep entrypoints are now available but not required for the hobby profile:
  - serve app: `make backend-serve-prod`
  - control app: `make backend-control-prod`
  - process split details live in [CLOUD_NATIVE_RUNBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/operations/CLOUD_NATIVE_RUNBOOK.md)

## Fresh Machine Cloud-Serve Bootstrap
- A fresh `cloud-serve` machine should not require a preexisting large local `backend/runtime/data.db` to serve the app.
- Required authority inputs for that machine are:
  - Neon access (`NEON_DATABASE_URL`)
  - runtime role `APP_RUNTIME_ROLE=cloud-serve`
  - operator/editor tokens as needed for exposed endpoints
- In that mode:
  - cUSE durable serving payloads should read from Neon and not fall back to local SQLite
  - `backend/data/serving_outputs.py` remains the boundary module for those reads through `load_current_payload(s)` / `load_runtime_payload(s)`; higher layers should not import `backend/data/serving_output_read_authority.py` directly
  - runtime/operator state should read from Neon and not fall back to local SQLite
  - holdings should read from Neon and fail closed if Neon is unavailable
  - cPAR package reads should use the Neon authority store and fail closed if no package exists there
  - the public/editor-facing serve app should not expose refresh execution routes
  - operator/control routes should be served from the separate control app surface
  - the frontend may target a separate control origin through `BACKEND_CONTROL_ORIGIN`; when unset it falls back to `BACKEND_API_ORIGIN`
  - the frontend must not hold operator/editor secrets in runtime env; privileged frontend `/api/*` routes should forward caller-supplied auth headers instead
- Small local scratch/cache/workspace files may still appear, but they are not the historical source warehouse and are not the serving authority.
- Local SQLite remains required only for:
  - direct LSEG ingest
  - deep archive retention
  - explicit local diagnostics
  - rebuild scratch/workspace paths during the current migration state

## Volume Pull Policy
- Canonical daily OHLCV ingest (`download_data_lseg.py`) maps `volume` from `TR.Volume`.
- Historical volume-repair path (`backfill_prices_range_lseg.py --volume-only`) maps `volume` from `TR.Volume`.
- Use `--only-null-volume` for targeted repairs so existing populated rows are not rewritten.
- After a broad historical volume repair, run `cold-core` refresh to rebuild raw cross-sections and risk caches from the updated volume series.

## Refresh Paths (When To Use)
- `serve-refresh`: quick serving refresh; no core recompute and no source ingest.
  - it reuses the current stable core package only when the live cache is both present and current for the active method version
  - if that stable core package is missing, stale, or due for rebuild, `serve-refresh` now fails closed and requires a core lane instead of recomputing factor returns / covariance / specific risk on the serving path
  - serving payload publish is the freshness boundary: after durable payload persistence plus active snapshot publish, the run emits an explicit `serving_publish_complete` milestone and app-facing surfaces may revalidate immediately even if deep diagnostics are still running
  - operator progress during `serving_refresh` is expected to show granular substages such as `universe_inputs`, `universe_loadings`, `persist_outputs`, and `health.sectionN`; a long-running refresh without substage heartbeats should be treated as suspect
  - holdings-triggered light refreshes now pass an explicit `holdings_only` scope and may reuse the current published `universe_loadings` payload when both of these still match:
    - `source_dates`
    - stable risk-engine fingerprint (`method_version`, `last_recompute_date`, `factor_returns_latest_date`, snapshot-age/lookback settings, specific-risk count)
  - the `source_dates` reuse check must reflect the current serving snapshot's actual exposure availability dates; stale carried-forward eligibility metadata must not force rebuilds when the underlying serving exposure date is unchanged
  - on that same fast path, cached `eligibility` and `cov_matrix` are reused when present instead of being rebuilt from unchanged model state
  - deep `health_diagnostics` are no longer recomputed on the quick path; `serve-refresh` carries forward the last good diagnostics payload or records that diagnostics were deferred
  - when that reuse path is active, relational `model_outputs` persistence is skipped because the core model state is unchanged; serving payload persistence still runs normally
  - manual `serve-refresh` without that scope keeps the existing full serving-refresh behavior.
  - serving-time prices remain read-only inputs to the projection layer; they must never write into canonical historical model-estimation tables such as `security_prices_eod`
  - projection-only outputs are read from persisted `projected_instrument_*` rows for the active `core_state_through_date`; if those rows are missing, the instrument is surfaced as projection-unavailable rather than being recomputed on the quick path
- `source-daily`: local LSEG ingest into SQLite for the latest completed XNYS session, repair any missing daily price sessions up to that session, purge open-month PIT rows, backfill any missing closed-month fundamentals/classification anchors, publish the retained working window into Neon, then refresh serving only.
- For identifier-based historical source/serving surfaces such as `security_prices_eod`, `security_fundamentals_pit`, and `security_classification_pit`, stage-2 Neon sync is identifier-aware:
  - existing identifiers that are already fully initialized in Neon continue to use the normal overlap-window sync
  - newly introduced or partially initialized identifiers receive retained-history backfill from local SQLite up to Neon's retained-history floor
- This prevents the Neon-primary app from seeing truncated history after a local ticker add/backfill.
- `source-daily-plus-core-if-due`: default daily maintenance lane; local ingest + Neon source-sync first, then recompute core only when cadence/version says due.
- `core-weekly`: force core recompute without rebuilding full raw history.
  - factor-return recompute now determines uncached dates before loading prices and only reads the bounded price window needed for those dates plus the immediately prior session.
  - when projection-only instruments are registered, this lane also refreshes their persisted projected outputs from durable `model_factor_returns_daily` for the active core package date.
  - this lane now owns deep `health_diagnostics` recompute for the current weekly core model state.
  - during the `serving_refresh` stage inside this lane, app-facing payloads still publish before deep diagnostics; the rest of the stage is diagnostics tail work plus diagnostics persistence
- `cold-core`: full historical reset for structural data changes (new/changed historical prices, volume, fundamentals, classification, or factor methodology).
  - This path rebuilds `barra_raw_cross_section_history` over full history and clears core cache tables before recomputing factor returns/risk.
  - This lane is an explicit operator/API path; it is not exposed as a one-click dashboard control in the current frontend.
  - During that run, `serving_refresh` must read the local/workspace source tables that just produced the rebuilt raw history; otherwise it can publish stale Neon factor-loadings metadata before the broad Neon mirror catches up.
  - During ordinary `serve-refresh`, the published weekly core-state should come from the latest durable `model_run_metadata` rather than a stale runtime cache key; otherwise a quick refresh can republish fresh loadings with regressed core metadata.
  - this lane also owns deep `health_diagnostics` recompute for structural rebuilds.
- `universe-add`: finalization lane after explicit registry/policy merge and targeted source backfills for new names.
  - if the add includes new projection-only / returns-projection instruments that do not yet have persisted projected outputs for the active core package date, do not stop at `serve-refresh`; run at least `core-weekly` so durable `projected_instrument_*` rows are produced before app refresh

Rebuild-authority rule:
- By default, `core-weekly` and `cold-core` rebuild from Neon after source sync whenever `DATA_BACKEND=neon` and a Neon DSN is configured.
- Set `NEON_AUTHORITATIVE_REBUILDS=false` only if you intentionally need to roll those lanes back to local SQLite. In that rollback mode, run `source-daily` first if you need the latest local ingest reflected.
- If Neon ever gets ahead of the intended `source-daily` target date because of a premature or invalid session stamp, `source_sync` now fails closed rather than trying to heal across the newer-than-target boundary. Investigate the bad stamp or advance the intended target first.
- In the default Neon-authoritative path, those rebuild lanes execute in this order:
  - `source_sync`: publish source tables from local SQLite into Neon only when Neon is not already newer than the allowed target boundary
  - `neon_readiness`: validate Neon table coverage/retention and materialize a scratch SQLite rebuild workspace from Neon
  - core stages run from that Neon-backed scratch workspace
  - final mirror publishes rebuilt analytics back into Neon
  - local derived tables/cache are refreshed from the scratch workspace so the private mirror stays congruent
- Rehearsal/cutover safety rule:
  - `neon_readiness` must surface a valid scratch workspace payload; malformed workspace metadata now fails the run closed instead of letting later stages guess paths
  - if syncing the rebuilt workspace derivatives back into the local mirror fails, the run also fails closed
- Workspace retention rule:
  - Neon rebuild workspaces under `backend/runtime/neon_rebuild_workspace/job_*` are scratch artifacts, not durable archives
  - retain only a small recent set via `NEON_REBUILD_WORKSPACE_RETENTION` (default `2`)
  - older `job_*` workspace directories are pruned automatically after runs; use a short-term retention override only when you intentionally need extra local forensics
- In both cases, local SQLite remains the only direct LSEG ingress point.

Runtime-role rule:
- `local-ingest`: all lanes may be used.
- `cloud-serve`: only `serve-refresh` is allowed.
- In `cloud-serve`, a bare `POST /api/refresh` now defaults safely to `serve-refresh`.
- Explicit deeper lanes remain blocked in `cloud-serve` even if requested by old mode-based callers.
- In the split app model, `/api/refresh` and `/api/refresh/status` belong to the control app, not the serve app.

Parallel cPAR note:
- cPAR has its own dedicated operating assumptions and does not share the cUSE4 refresh API/operator flow.
- Current cPAR runtime-role, build-entrypoint, and authority behavior is documented in [CPAR_OPERATIONS_PLAYBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/operations/CPAR_OPERATIONS_PLAYBOOK.md).

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
- `scripts/local_app/up.sh` now waits for the backend/frontend listeners to bind and exits nonzero if either process dies during startup; if startup fails, check the tailed log output immediately rather than trusting the pid files.
- Additional local split entrypoints are available for cloud-native prep validation:
  - `make backend-serve`
  - `make backend-control`
- `make setup` now provisions `.venv_local` for the real local app/runtime path. `backend/.venv` may still exist for repo tests, but the launcher scripts and ingest/rebuild commands should use `.venv_local`.
- Live local ingest and Neon-authoritative rebuild commands should run from `.venv_local` (or another environment with real `lseg-data` installed). `serve-refresh` and other non-ingest lanes should not require `lseg-data` just to import/run.

## Key Commands
- Orchestrated refresh via API:
  - `curl -X POST "http://localhost:8000/api/refresh"`
- API refresh explicit serve-refresh profile:
  - `curl -X POST "http://localhost:8000/api/refresh?profile=serve-refresh"`
- Cloud-mode authenticated serve-refresh:
  - `curl -X POST "http://localhost:8000/api/refresh?profile=serve-refresh" -H "X-Operator-Token: $OPERATOR_API_TOKEN"`
- Split-control authenticated serve-refresh:
  - `curl -X POST "http://localhost:8001/api/refresh?profile=serve-refresh" -H "X-Operator-Token: $OPERATOR_API_TOKEN"`
- API refresh explicit source-daily profile:
  - `curl -X POST "http://localhost:8000/api/refresh?profile=source-daily"`
- API refresh explicit weekly core recompute:
  - `curl -X POST "http://localhost:8000/api/refresh?profile=core-weekly&force_core=true"`
- API refresh explicit cold-core rebuild:
  - `curl -X POST "http://localhost:8000/api/refresh?profile=cold-core"`
- API refresh partial stage run:
  - `curl -X POST "http://localhost:8000/api/refresh?profile=source-daily-plus-core-if-due&from_stage=ingest&to_stage=risk_model"`
  - if Neon-authoritative rebuilds are active and you target core stages explicitly, include `neon_readiness` in the window or the run will fail closed before core work starts
- Orchestrated refresh via CLI module:
  - `python3 -m backend.scripts.run_model_pipeline --profile serve-refresh`
- Orchestrated refresh via script wrapper:
  - `python3 -m backend.scripts.run_model_pipeline --profile source-daily-plus-core-if-due`
- Source-only refresh via script wrapper:
  - `python3 -m backend.scripts.run_model_pipeline --profile source-daily`
- Repair Neon sync health from an already-successful workspace without rerunning the full core lane:
  - `python3 -m backend.scripts.repair_neon_sync_health --run-id job_20260322T172131Z --profile core-weekly --as-of-date 2026-03-20 --json`
- Cold-core refresh via script wrapper:
  - `python3 -m backend.scripts.run_model_pipeline --profile cold-core`
- Resume a previous run id:
  - `python3 -m backend.scripts.run_model_pipeline --profile source-daily-plus-core-if-due --resume-run-id <run_id>`
- Refresh data from LSEG:
  - `.venv_local/bin/python -m backend.scripts.download_data_lseg --db-path backend/runtime/data.db`
  - Explicit `--tickers`, `--rics`, and index-derived names only operate on instruments already present in the runtime ingest scope; the command reports any requested names that are not yet represented in the seeded registry/runtime surfaces.
- When running those local-ingest commands directly, prefer `.venv_local/bin/python -m ...` so the process uses the same LSEG-capable environment as the local app scripts.
- `make doctor` verifies `.venv_local`, core backend imports, whether `lseg.data` is available in that environment, and whether clean duplicate aliases are still present in the primary registry seed plus the local compatibility surfaces when they exist.
- Repair historical volume coverage only (writes `TR.Volume` into `security_prices_eod.volume`):
  - `python3 -m backend.scripts.backfill_prices_range_lseg --db-path backend/runtime/data.db --start-date 2012-01-03 --end-date 2026-03-04 --volume-only --only-null-volume`
  - Explicit `--rics` repairs likewise only target names already present in the runtime ingest scope and report unmatched requested RICs in the result payload.
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
  - `replace_all=true` is now reserved for the canonical serving payload set only:
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
  - partial updates like the health/refresh-meta patch path remain explicit `replace_all=false` writes and must not be treated as a full publish.
- projection-only publish invariant:
  - if persisted projected loadings exist for a `projection_only` ticker at the active core date, serving publish must fail unless that ticker lands as `model_status=projected_only` and `exposure_origin=projected` in both `portfolio` and `universe_loadings`.
- operator repair path for serving payload drift:
  - dry-run diff local mirror vs Neon:
    - `python3 -m backend.scripts.repair_serving_payloads_neon --dry-run --json`
  - canonical payload-set repair back to Neon:
    - `python3 -m backend.scripts.repair_serving_payloads_neon --write-mode row_by_row --json`
  - targeted payload repair is allowed only by explicit `--payload-names ...`; the canonical-set repair remains the default and preferred path.
- `refresh_status`: background orchestrator state snapshot, persisted through `runtime_state_current` with local SQLite only as the local-ingest mirror/fallback lane.
  - includes current stage progress for in-flight runs (`current_stage`, `stage_index`, `stage_count`, `stage_started_at`) and the optional `refresh_scope` used by holdings-triggered refreshes.
- `holdings_sync_state`: holdings-dirty and serving-refresh bookkeeping state, likewise persisted through `runtime_state_current` with local SQLite only as the local-ingest mirror/fallback lane.
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
  - active cUSE model history: the retained `barra_raw_cross_section_history` window that drives factor-return recomputes
  - risk-model lookback: the rolling covariance/specific-risk window (`LOOKBACK_DAYS`, currently ~2 trading years)
  - source archive retention: deeper local history plus Neon publish retention
- Ordinary `core-weekly` recomputes respect the active cUSE model-history floor from `barra_raw_cross_section_history`; they do not try to backfill the full price archive.
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
  - Deeper local source archives are allowed and expected; they do not, by themselves, widen the active cUSE model window.
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
  - `curl -s "${BACKEND_CONTROL_ORIGIN:-http://localhost:8001}/api/refresh/status" | jq`
- Verify operator lane matrix:
  - `curl -s "${BACKEND_CONTROL_ORIGIN:-http://localhost:8001}/api/operator/status" | jq`
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
  - live cloud check:
    - `APP_BASE_URL=https://app.ceiora.com CONTROL_BASE_URL=https://control.ceiora.com OPERATOR_API_TOKEN=... make operator-check`
  - live cloud dispatch + reconciliation:
    - `APP_BASE_URL=https://app.ceiora.com CONTROL_BASE_URL=https://control.ceiora.com OPERATOR_API_TOKEN=... RUN_REFRESH_DISPATCH=1 make operator-check`
  - If Neon auto-sync is disabled, this check degrades gracefully instead of failing on missing parity artifacts.
- Verify latest refresh metadata:
  - `curl -s "http://localhost:8000/api/data/diagnostics" | jq '.cache_outputs[] | select(.key==\"refresh_meta\")'`
- Verify risk payload includes engine metadata:
  - `curl -s "http://localhost:8000/api/risk" | jq '.risk_engine'`
- Verify serving payload manifests are on a single live snapshot:
  - `python3 -m backend.scripts.repair_serving_payloads_neon --dry-run --json | jq '.diff,.local_snapshot_ids,.neon_snapshot_ids'`
- Verify latest usable eligibility summary (regression members > 0 preferred):
  - `sqlite3 backend/runtime/cache.db "SELECT date,exp_date,regression_member_n,structural_coverage,regression_coverage FROM daily_universe_eligibility_summary ORDER BY date DESC LIMIT 10;"`
- Verify no compatibility views remain:
  - `sqlite3 backend/runtime/data.db "SELECT COUNT(*) FROM sqlite_master WHERE type='view';"`
- Verify the compatibility projection no longer carries deprecated metadata:
  - `sqlite3 backend/runtime/data.db "SELECT name FROM pragma_table_info('security_master_compat_current') WHERE name IN ('sid','permid','instrument_type','asset_category_description');"`
- Verify active universe authority remains registry-first:
  - `sqlite3 backend/runtime/data.db "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('security_registry','security_policy_current','security_taxonomy_current','security_source_status_current');"`

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
  - it is a compatibility mirror only; registry/policy/taxonomy/source-observation surfaces now define active universe behavior.
- Price schema policy:
  - `security_prices_eod` canonical columns are `open/high/low/close/adj_close/volume/currency` (no `exchange` field).
- Risk API readiness gate:
  - `/api/risk` now returns not-ready if covariance/specific-risk payload is incomplete.
- Neon rebuild readiness gate:
  - required model-output tables such as `model_factor_covariance_daily` and `model_run_metadata` must be present and non-empty; table existence alone is not sufficient to treat Neon as rebuild-ready.
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
