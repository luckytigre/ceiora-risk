from __future__ import annotations

from pathlib import Path
from typing import Any

from backend.services.neon_stage2 import canonical_tables, sync_from_sqlite_to_neon


def _summarize_factor_returns_sync(sync_payload: dict[str, Any]) -> dict[str, Any]:
    factor_sync_table = (sync_payload.get("tables") or {}).get("model_factor_returns_daily")
    if isinstance(factor_sync_table, dict):
        return {
            "status": "ok",
            "source_table": "model_factor_returns_daily",
            **factor_sync_table,
        }
    return {
        "status": "skipped",
        "reason": "table_not_selected",
        "source_table": "model_factor_returns_daily",
    }


def ensure_neon_canonical_schema(*, dsn: str | None = None) -> dict[str, Any]:
    # Keep the public schema-ensure seam in neon_mirror while avoiding an import cycle.
    from backend.services.neon_mirror import ensure_neon_canonical_schema as _ensure_neon_canonical_schema

    return _ensure_neon_canonical_schema(dsn=dsn)


def run_neon_source_sync_cycle(
    *,
    sqlite_path: Path,
    dsn: str | None = None,
    mode: str = "incremental",
    tables: list[str] | None = None,
    batch_size: int = 25_000,
) -> dict[str, Any]:
    selected_tables = list(tables or canonical_tables())
    out: dict[str, Any] = {
        "status": "ok",
        "mode": str(mode),
        "tables": selected_tables,
        "schema_ensure": None,
        "sync": None,
        "factor_returns_sync": None,
    }

    out["schema_ensure"] = ensure_neon_canonical_schema(dsn=dsn)
    out["sync"] = sync_from_sqlite_to_neon(
        sqlite_path=Path(sqlite_path),
        dsn=dsn,
        tables=selected_tables,
        mode=str(mode),
        batch_size=int(batch_size),
    )
    out["factor_returns_sync"] = _summarize_factor_returns_sync(dict(out.get("sync") or {}))
    return out
