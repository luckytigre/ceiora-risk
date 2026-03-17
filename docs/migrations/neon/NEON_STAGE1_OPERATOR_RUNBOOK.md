# Neon Migration Operator Runbook (Stage 1/2 Prep)

## Objective
Prepare and execute a safe migration of canonical backend source-of-truth tables from local SQLite to Neon Postgres, while keeping LSEG ingestion on local machine.

## Confirmed Decisions
- Migration mode starts as **read-only parity** (no runtime cutover first).
- Canonical tables in first migration set:
  - `security_master`
  - `security_prices_eod`
  - `security_fundamentals_pit`
  - `security_classification_pit`
  - `barra_raw_cross_section_history`
- Identity model:
  - RIC = physical key for time-series.
  - `security_master` = canonical identity registry.
- One Neon branch/environment (single-user setup).
- Ingestion host remains local machine due LSEG Desktop dependency.
- Constraints/indexes can be phased in after initial load.
- Neon PITR is enabled.
- Migration run does not require per-endpoint cutover flags.

## Stage 1 Tooling
- `backend/scripts/neon_preflight_check.py`
  - DSN/SSL validation and optional connectivity check.
- `backend/scripts/_archive/prepare_neon_stage1_bundle.py`
  - Schema snapshot + SQLite audit + manifest checksums (+ optional exports).
- `backend/data/health_audit.py`
  - Reusable consistency and health checks.

## Stage 2 Execution Outline
1. Run Neon preflight.
2. Build prep bundle from local SQLite.
3. Apply canonical Postgres schema.
4. Load canonical tables to Neon (bulk sync).
5. Run parity audits:
  - row counts
  - key uniqueness
  - orphan checks
  - date window checks
  - random sampled row diffs
6. Only after parity passes, enable controlled read cutover.

### Stage 2 Commands
Apply canonical + holdings schema in Neon:
```bash
python3 -m backend.scripts.neon_apply_schema --include-holdings --json
```

Initial full sync from SQLite:
```bash
python3 -m backend.scripts.neon_sync_from_sqlite \
  --db-path backend/runtime/data.db \
  --mode full \
  --json
```

Ongoing incremental sync (overlap reload windows):
```bash
python3 -m backend.scripts.neon_sync_from_sqlite \
  --db-path backend/runtime/data.db \
  --mode incremental \
  --json
```

Parity gate:
```bash
python3 -m backend.scripts.neon_parity_audit \
  --db-path backend/runtime/data.db \
  --json
```

Post-run artifact + signal checks:
```bash
ls -1 backend/runtime/audit_reports/neon_parity | tail -n 5
curl -s http://127.0.0.1:8000/api/health
curl -s http://127.0.0.1:8000/api/refresh/status
```

## Local -> Neon Sync Model
- Triggered immediately after each successful local run.
- Target lag budget: 10-20 minutes.
- Local machine remains write authority while LSEG Desktop is local-only.
- Lower is preferred when possible, but stability and auditability are prioritized over chasing minimum lag.
- Incremental sync uses overlap windows by table to safely refresh recent dates:
  - prices: 10 days
  - fundamentals/classification: 62 days
  - barra raw history: 14 days

## Coverage Caveat (Current DB State)
- The latest canonical date can be sparse when a run is partial (for example `2026-03-04` currently has only 10 RICs).
- Do not treat `MAX(date)` as a complete cross section by default.
- Use parity + coverage checks before read cutover.

## Safety Gates
- Abort migration/cutover if any of:
  - integrity check fails
  - duplicate-key groups > 0 on canonical keys
  - orphan rows > 0 for canonical relations
  - parity count mismatch beyond approved tolerance
  - sampled row diff mismatch beyond approved tolerance

### Non-critical tolerance policy
- Tolerance is allowed only for non-critical fields during parity checks.
- Applies to:
  - floating-point rounding differences (for example `1e-9` level)
  - timestamp precision/format normalization differences
  - benign text normalization (`NULL` vs empty string where explicitly mapped)
- Does **not** apply to:
  - primary keys
  - RIC identity mappings
  - row-count parity on canonical tables
  - account-position quantities in holdings tables

## Recovery Model
- Neon PITR + periodic Neon exports.
- Keep local SQLite snapshot workflow for emergency fallback during transition.

## Open Decisions (to finalize before cutover)
1. Edit authorization model:
   - currently single-user trusted operator.
   - if multi-user is introduced later, add row-level ownership/audit controls.

## Locked Holdings Decisions
- Holdings quantity precision:
  - store at up to 6 decimal places.
  - UI display precision is adaptive (for readability), independent of stored precision.
- Zero quantity policy:
  - zero-quantity resulting positions are removed from `holdings_positions_current`.
- Unknown identifiers in CSV:
  - rows must resolve to a valid RIC.
  - unresolved rows are rejected with warnings (no silent insert).
  - ticker-only rows are allowed; resolver attempts `ticker -> RIC` via canonical mapping.
  - if multiple mappings exist, deterministic pick is applied and alternatives are logged as warnings.
- `account_id` behavior:
  - required at import submit time.
  - import workflow prompts for account when absent.
- Negative quantities:
  - allowed (short positions).
- Cost basis:
  - deferred to later phase (not in initial holdings schema/load).

### Holdings Commands
Import holdings from a canonical CSV file:
```bash
python3 -m backend.scripts.neon_holdings_import_csv --csv-path holdings.csv --mode replace_account --account-id main --json
```

Import holdings CSV:
```bash
python3 -m backend.scripts.neon_holdings_import_csv \
  --csv-path /path/to/holdings.csv \
  --mode replace_account \
  --account-id ibkr_main \
  --json
```
