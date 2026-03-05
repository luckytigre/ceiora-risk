#!/usr/bin/env python3
"""Apply Neon canonical/holdings schema SQL files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from backend.data.neon import connect, resolve_dsn
from backend.services.neon_stage2 import apply_sql_file


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn", default=None, help="Neon DSN (defaults to NEON_DATABASE_URL)")
    p.add_argument(
        "--canonical-schema",
        type=Path,
        default=Path("docs/migrations/neon/NEON_CANONICAL_SCHEMA.sql"),
        help="Canonical source-table schema SQL path",
    )
    p.add_argument(
        "--holdings-schema",
        type=Path,
        default=Path("docs/migrations/neon/NEON_HOLDINGS_SCHEMA.sql"),
        help="Holdings schema SQL path",
    )
    p.add_argument(
        "--include-holdings",
        action="store_true",
        help="Also apply holdings schema after canonical schema",
    )
    p.add_argument("--json", action="store_true", help="Emit JSON output")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    dsn = resolve_dsn(args.dsn)
    out: dict[str, object] = {"status": "ok", "applied": []}

    conn = connect(dsn=dsn, autocommit=False)
    try:
        out["applied"].append(apply_sql_file(conn, sql_path=Path(args.canonical_schema)))
        if bool(args.include_holdings):
            out["applied"].append(apply_sql_file(conn, sql_path=Path(args.holdings_schema)))
    finally:
        conn.close()

    if args.json:
        print(json.dumps(out, indent=2))
    else:
        print("Schema apply ok")
        for item in out["applied"]:
            print(f"- {item['sql_path']} ({item['bytes']} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
