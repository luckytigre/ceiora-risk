# Neon Migration Execution Plan

Date: 2026-03-06
Owner: Codex

## Objective
Use local SQLite as the full historical ingest/source authority while Neon operates as:
1. Post-refresh canonical mirror.
2. Pruned serving store (10y source + 5y analytics windows).
3. Controlled read target, enabled surface-by-surface.
4. Holdings runtime source (positions) in Neon.

## Implemented Now

### 1) Post-refresh Neon mirror in orchestrator
- `backend/orchestration/run_model_pipeline.py` now runs a Neon mirror cycle after a successful pipeline run when `NEON_AUTO_SYNC_ENABLED=true`.
- Mirror cycle includes:
  - incremental/full sync,
  - factor-return sync from `cache.db` (`daily_factor_returns` -> `model_factor_returns_daily`),
  - optional Neon prune,
  - optional bounded parity audit.
- Pipeline output now includes `neon_mirror` status payload.
- If `NEON_AUTO_SYNC_REQUIRED=true`, mirror/parity failure fails the pipeline run.

### 2) Neon-only retention (SQLite stays full)
- Mirror prune uses two windows:
  - source tables: `NEON_SOURCE_RETENTION_YEARS` (default 10),
  - analytics tables: `NEON_ANALYTICS_RETENTION_YEARS` (default 5).
- This pruning is applied in Neon only.
- Local SQLite is unchanged by this flow.

### 3) Controlled read cutover (no silent fallback)
- Added read-surface routing via `NEON_READ_SURFACES`:
  - `core_reads` -> `backend/data/core_reads.py` canonical read path (used by refresh pipeline source fetches).
  - `factor_history` -> `/api/exposures/history` path via `backend/data/history_queries.py`.
  - `price_history` -> `/api/universe/ticker/{ticker}/history` path via `backend/data/history_queries.py`.
- If a surface is enabled, reads go to Neon for that surface.
- If `DATA_BACKEND=neon`, all surfaces route to Neon.
- Current runtime setting for controlled cutover can include all three surfaces:
  - `NEON_READ_SURFACES=core_reads,factor_history,price_history`

### 4) Holdings runtime cutover
- `backend/portfolio/positions_store.py` now reads from Neon `holdings_positions_current` when `DATA_BACKEND=neon`.
- In non-Neon mode, existing in-code mock positions remain the local fallback.
- Neon mode intentionally does not fall back to in-code mocks on query failure.

### 4b) Durable serving-output cutover
- `serving_payload_current` now holds the latest persisted dashboard-serving payloads (`portfolio`, `risk`, `exposures`, `universe_loadings`, `universe_factors`, `model_sanity`, `refresh_meta`, `eligibility`).
- This table is written during refresh publish in SQLite and also upserted directly into Neon.
- In `cloud-serve` mode, this durable layer is the effective primary serving authority.
- `SERVING_OUTPUTS_PRIMARY_READS=true` still exists for staged local rehearsal, but cloud mode no longer depends on it being manually flipped.

### 5) Parity artifact + health signal
- Every post-refresh Neon mirror run now writes a formal JSON artifact:
  - `backend/runtime/audit_reports/neon_parity/neon_mirror_<timestamp>_<run_id>.json`
  - rolling pointer: `backend/runtime/audit_reports/neon_parity/latest_neon_mirror_report.json`
- Pipeline publishes `neon_sync_health` cache state (`ok|warning|error`) with mirror/parity status and issue examples.
- `/api/health` now includes `neon_sync_health` and degrades to `status=degraded` when `neon_sync_health.status=error`.
- Header health signal consumes mirror/parity status from refresh payload and turns red on non-OK Neon mirror/parity.

## Environment Controls
- `DATA_BACKEND=sqlite|neon`
- `NEON_DATABASE_URL=...`
- `APP_RUNTIME_ROLE=local-ingest|cloud-serve`
- `NEON_AUTO_SYNC_ENABLED=true|false`
- `NEON_AUTO_SYNC_REQUIRED=true|false`
- `NEON_AUTO_SYNC_MODE=incremental|full`
- `NEON_AUTO_SYNC_TABLES=...` (optional CSV)
- `NEON_AUTO_PARITY_ENABLED=true|false`
- `NEON_AUTO_PRUNE_ENABLED=true|false`
- `NEON_SOURCE_RETENTION_YEARS=10`
- `NEON_ANALYTICS_RETENTION_YEARS=5`
- `NEON_READ_SURFACES=core_reads,factor_history,price_history`
- `SERVING_OUTPUTS_PRIMARY_READS=true|false`
- `OPERATOR_API_TOKEN=...`
- `EDITOR_API_TOKEN=...`

## Recommended Cutover Sequence
1. Keep `DATA_BACKEND=sqlite` and set:
   - `NEON_AUTO_SYNC_ENABLED=true`
   - `NEON_AUTO_PARITY_ENABLED=true`
   - `NEON_AUTO_PRUNE_ENABLED=true`
2. Run normal refreshes and confirm repeated `neon_mirror.status=ok`.
3. Enable one read surface at a time in `NEON_READ_SURFACES`:
   - `factor_history`, then `price_history`, then `core_reads`.
4. After stable operation, either:
   - keep mixed mode with selected surfaces, or
   - set `DATA_BACKEND=neon` for full read cutover.
5. For holdings, seed/import Neon positions and verify the dashboard position set before switching `DATA_BACKEND=neon`.

## Operator Notes
- Local SQLite remains the LSEG ingest authority and keeps full history.
- Neon is expected to be the serving-oriented windowed store.
- In `local-ingest`, broad post-run mirror/parity/prune remain the publish path.
- In `cloud-serve`, broad mirror/parity/prune are intentionally skipped; serving payloads are written directly and holdings stay Neon-authoritative.
- Manual emergency mirror still available via:
  - `python3 -m backend.scripts.neon_sync_from_sqlite --mode incremental --json`
  - `python3 -m backend.scripts.neon_parity_audit --json`
