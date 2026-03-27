#!/usr/bin/env python3
"""Sync canonical tables from local SQLite into Neon."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from backend.services.neon_stage2 import canonical_tables, sync_from_sqlite_to_neon


def _parse_tables(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    parts = [p.strip() for p in str(raw).split(",")]
    clean = [p for p in parts if p]
    return clean or None


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn", default=None, help="Neon DSN (defaults to NEON_DATABASE_URL)")
    p.add_argument(
        "--db-path",
        type=Path,
        default=Path("backend/data.db"),
        help="Source SQLite DB path",
    )
    p.add_argument(
        "--mode",
        choices=["incremental", "full"],
        default="incremental",
        help="Sync mode",
    )
    p.add_argument(
        "--tables",
        default=None,
        help=(
            "Comma-separated table names. "
            f"Default: {','.join(canonical_tables())}"
        ),
    )
    p.add_argument("--batch-size", type=int, default=25_000, help="Batch size for transfer")
    p.add_argument(
        "--verify-source-integrity",
        action="store_true",
        help="Run source-integrity validation for selected SQLite source tables before marking sync success.",
    )
    p.add_argument(
        "--run-sqlite-integrity-check",
        action="store_true",
        help="Also run PRAGMA quick_check/integrity_check during source-integrity validation.",
    )
    p.add_argument("--json", action="store_true", help="Emit JSON output")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    out = sync_from_sqlite_to_neon(
        sqlite_path=Path(args.db_path),
        dsn=args.dsn,
        tables=_parse_tables(args.tables),
        mode=str(args.mode),
        batch_size=max(1_000, int(args.batch_size)),
        verify_source_integrity=bool(args.verify_source_integrity),
        run_sqlite_integrity_check=bool(args.run_sqlite_integrity_check),
    )
    if args.json:
        print(json.dumps(out, indent=2))
    else:
        print(f"Sync status: {out.get('status')} ({out.get('mode')})")
        for table, t in (out.get("tables") or {}).items():
            print(
                f"- {table}: action={t.get('action')} "
                f"source_rows={t.get('source_rows')} rows_loaded={t.get('rows_loaded')}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
