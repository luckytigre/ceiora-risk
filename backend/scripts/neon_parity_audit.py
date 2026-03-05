#!/usr/bin/env python3
"""Run SQLite vs Neon parity audit for canonical tables."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from backend.services.neon_stage2 import canonical_tables, run_parity_audit


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
        "--tables",
        default=None,
        help=(
            "Comma-separated table names. "
            f"Default: {','.join(canonical_tables())}"
        ),
    )
    p.add_argument(
        "--allow-mismatch",
        action="store_true",
        help="Return exit code 0 even when mismatches are found",
    )
    p.add_argument("--json", action="store_true", help="Emit JSON output")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    out = run_parity_audit(
        sqlite_path=Path(args.db_path),
        dsn=args.dsn,
        tables=_parse_tables(args.tables),
    )

    if args.json:
        print(json.dumps(out, indent=2))
    else:
        print(f"Parity status: {out.get('status')}")
        for issue in out.get("issues", []):
            print(f"- {issue}")

    if str(out.get("status")) != "ok" and not bool(args.allow_mismatch):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
