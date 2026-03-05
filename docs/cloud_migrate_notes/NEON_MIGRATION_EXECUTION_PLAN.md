# Neon Migration Execution Plan

Date: 2026-03-05
Owner: Codex

## Goals
- Keep local SQLite as LSEG ingest authority for now.
- Establish Neon as parity mirror for canonical source-of-truth tables.
- Keep runtime read path on SQLite until parity is stable.
- Add Neon-native holdings store with locked import behavior.

## Current Project State (2026-03-05 scan)
- Canonical source table row counts in local `backend/data.db`:
  - `security_master`: 5,828 total rows, 5,820 eligible.
  - `security_prices_eod`: 10,681,457 rows (`2012-01-03` to `2026-03-04`).
  - `security_fundamentals_pit`: 990,600 rows (`2012-01-31` to `2026-03-04`).
  - `security_classification_pit`: 990,600 rows (`2012-01-31` to `2026-03-04`).
  - `barra_raw_cross_section_history`: 10,681,457 rows (`2012-01-03` to `2026-03-04`).
- Integrity shape checks (fast structural):
  - duplicate canonical key groups: `0` across all 5 canonical tables.
  - orphan RIC rows vs `security_master`: `0` across all canonical time-series tables.
- Drift detected:
  - Latest date `2026-03-04` is sparse (`10` distinct RICs) in prices/fund/classification/barra.
  - Prior recent dates (`2026-03-03` to `2026-02-20`) are around `3,688`-`3,694` distinct RICs.
  - Monthly PIT anchors remain broad (`5,827` distinct RICs on `2026-02-27` and prior monthly anchors).

## Key Implication
- Migration should not assume "latest date == complete date".
- Parity checks and runtime gating should use broad-coverage dates, not blindly `MAX(date)`.

## Phase Plan

### Phase A - Baseline and Guardrails
- [x] Stage-1 preflight/audit tooling exists.
- [x] Stage-1 prep bundle workflow exists.
- [x] Document sparse-latest-date reality and include in migration gates.

### Phase B - Stage-2 Neon Mirror (Implement now)
- [x] Add canonical Neon DDL for the 5 source tables.
- [x] Add sync tooling (full + incremental overlap reload).
- [x] Add parity audit tooling (counts/date windows/latest distinct coverage/duplicates/orphans).
- [x] Add operator runbook commands for Stage-2 execution.

### Phase C - Holdings in Neon (Implement now)
- [x] Keep dedicated holdings schema in Neon.
- [x] Add import engine with locked modes:
  - `replace_account`
  - `upsert_absolute`
  - `increment_delta`
- [x] Enforce rules:
  - account_id required/validated
  - 6-decimal quantity storage
  - negative quantities allowed
  - zero resulting positions removed
  - unknown RIC rejected
  - ticker-only deterministic `ticker -> RIC` resolver with warnings
- [x] Add mock holdings seed tool for initial Neon population.

### Phase D - Cutover Prep (next)
- [ ] Script local run -> Neon sync as one operator command.
- [ ] Add post-sync parity gate to block read cutover on mismatch.
- [ ] Implement backend read-routing switch (`sqlite` vs `neon`) only after repeated parity pass.
- [ ] Add API-backed holdings endpoints/UI edits (currently still file-backed mock positions).

## Stage-2 Canonical Execution Sequence
1. Apply Neon canonical schema:
   - `python3 -m backend.scripts.neon_apply_schema --include-holdings --json`
2. Run first full mirror load:
   - `python3 -m backend.scripts.neon_sync_from_sqlite --mode full --json`
3. Run parity audit:
   - `python3 -m backend.scripts.neon_parity_audit --json`
4. For regular operation after local refresh:
   - `python3 -m backend.scripts.neon_sync_from_sqlite --mode incremental --json`
   - `python3 -m backend.scripts.neon_parity_audit --json`

## Progress Log

### 2026-03-05 11:xx ET
- Re-audited current repository and database state.
- Added migration-aware plan with concrete row/date baseline.
- Added Stage-2 Neon migration toolkit and holdings import toolkit.
- Updated operator documentation to reflect new scripts and gates.
