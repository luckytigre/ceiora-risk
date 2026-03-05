# Neon Migration Execution Plan

Date: 2026-03-05
Owner: Codex

## Objective
Move canonical source-of-truth and holdings workflows to Neon in a controlled way:
- local SQLite remains LSEG ingest authority for now,
- Neon becomes parity mirror first,
- read cutover happens only after repeated parity pass,
- holdings become Neon-backed with deterministic import/edit behavior.

## Revised Codebase Snapshot (latest audit)

### Architecture shifts now present
- Analytics pipeline was refactored into service modules:
  - `backend/analytics/services/cache_publisher.py`
  - `backend/analytics/services/risk_views.py`
  - `backend/analytics/services/universe_loadings.py`
- API route shaping/query split added:
  - `backend/routes/presenters.py`
  - `backend/db/history_queries.py`
- Retention tooling added:
  - `backend/db/retention.py`
  - `backend/scripts/prune_history_by_lookback.py`
- CI workflow present:
  - `.github/workflows/ci.yml`

### Test status
- `pytest -q backend/tests` -> **37 passed**.

### Canonical DB profile (local `backend/data.db`)
- `security_master`: 5,828 total / 5,820 eligible.
- `security_prices_eod`: 10,681,457 rows (`2012-01-03` -> `2026-03-04`).
- `security_fundamentals_pit`: 990,600 rows (`2012-01-31` -> `2026-03-04`).
- `security_classification_pit`: 990,600 rows (`2012-01-31` -> `2026-03-04`).
- `barra_raw_cross_section_history`: 10,681,457 rows (`2012-01-03` -> `2026-03-04`).
- Duplicate-key groups on canonical PKs: **0**.
- Orphan RIC rows vs `security_master`: **0**.
- Latest-date sparsity remains:
  - `2026-03-04` has only 10 distinct RICs in daily canonical tables.
  - recent complete-ish dates are around 3,688-3,694 RICs.

## Migration Readiness Status

### Phase A - Baseline and guardrails
- [x] Stage-1 preflight/audit tooling implemented.
- [x] Stage-1 prep bundle tooling implemented.
- [x] Sparse-latest-date caveat documented and reflected in gating policy.

### Phase B - Stage-2 Neon mirror tooling
- [x] Canonical Neon DDL implemented.
- [x] Full + incremental overlap sync scripts implemented.
- [x] SQLite vs Neon parity-audit script implemented.
- [x] Operator runbook commands documented.

### Phase C - Holdings in Neon
- [x] Holdings schema + constraints implemented.
- [x] CSV import engine implemented (`replace_account`, `upsert_absolute`, `increment_delta`).
- [x] Deterministic ticker->RIC resolver implemented.
- [x] Mock holdings seeding script implemented.

### Phase D - Integration/cutover plumbing (pending)
- [ ] Add one-command "post-refresh sync + parity gate" operator entrypoint.
- [ ] Wire backend holdings runtime off `positions_store` to Neon tables (API-backed reads/writes).
- [ ] Add backend read-routing switch (`sqlite` vs `neon`) with hard parity gate.

### Phase E - Controlled read cutover (pending)
- [ ] Run first full Neon load.
- [ ] Run parity audits and resolve any mismatches.
- [ ] Run repeated incremental sync + parity cycles after local refresh runs.
- [ ] Flip read routing to Neon only after parity is stable.

## Operator Sequence (current expected flow)
1. Set `NEON_DATABASE_URL`.
2. Preflight:
   - `python3 -m backend.scripts.neon_preflight_check --json`
3. Apply schema:
   - `python3 -m backend.scripts.neon_apply_schema --include-holdings --json`
4. Initial full mirror:
   - `python3 -m backend.scripts.neon_sync_from_sqlite --mode full --json`
5. Parity gate:
   - `python3 -m backend.scripts.neon_parity_audit --json`
6. Seed mock holdings (optional initial data):
   - `python3 -m backend.scripts.neon_holdings_seed_mock --account-id main_mock --json`
7. Ongoing cycle after local refresh:
   - `python3 -m backend.scripts.neon_sync_from_sqlite --mode incremental --json`
   - `python3 -m backend.scripts.neon_parity_audit --json`

## Current Risks / Notes
- Latest date in source tables can be partial; parity checks should include broad-coverage dates, not only `MAX(date)`.
- Runtime holdings still come from in-repo mock store (`backend/portfolio/positions_store.py`) until Phase D wiring is done.
- Worktree currently has additional uncommitted refactor changes; migration cutover should be done only from a clean, committed baseline.

## Plan Refresh Log
### 2026-03-05 (latest)
- Re-audited revised codebase structure, tests, and canonical DB profile.
- Updated plan phases to align with new service-layer architecture and retention tooling.
- Preserved Neon execution sequence and clarified remaining cutover tasks.
