# Backend Layout

## Runtime Entry
- `main.py`: FastAPI app entrypoint and route wiring.
- `config.py`: environment + path configuration.

## Core Domains
- `api/`: API router registry and HTTP boundary composition.
- `analytics/`: refresh pipeline, health diagnostics, and service-layer composition.
- `risk_model/`: Barra-style risk model logic and analytics.
- `universe/`: ESTU/foundation data preparation and membership logic.
- `data/`: data-access boundary for SQLite/Neon history and retention queries.
- `orchestration/`: profile-based pipeline orchestration.
- `ops/`: operational tooling namespace.
- `services/`: background refresh manager and operational services.

## Scripts
- `scripts/`: operational CLIs (ingest, refresh, pruning, compaction, migration helpers).
- `scripts/_archive/`: no-longer-active scripts retained for reference.

## Testing
- `tests/`: backend test suite including route snapshots and service tests.

## Local Artifacts (ignored)
- `runtime/` (primary local DB/runtime artifacts), `*.bak`, `logs/`, `tmp/`, `tmp_logs/`, `offline_backups/`.
- Compatibility symlinks may exist at `backend/data.db` and `backend/cache.db`.
- Keep generated artifacts out of source folders and under ignored paths only.
