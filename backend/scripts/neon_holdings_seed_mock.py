#!/usr/bin/env python3
"""Seed Neon holdings tables from backend/portfolio/positions_store mock holdings."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from backend.db.neon import connect, resolve_dsn
from backend.portfolio.positions_store import PORTFOLIO_POSITIONS
from backend.services.neon_holdings import (
    apply_holdings_import,
    build_rows_from_ticker_quantities,
    ensure_holdings_schema,
)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn", default=None, help="Neon DSN (defaults to NEON_DATABASE_URL)")
    p.add_argument("--account-id", default="main_mock", help="Target account id")
    p.add_argument(
        "--schema-sql",
        type=Path,
        default=Path("docs/cloud_migrate_notes/NEON_HOLDINGS_SCHEMA.sql"),
        help="Holdings schema SQL file",
    )
    p.add_argument("--requested-by", default="seed_mock", help="Operator identity")
    p.add_argument("--dry-run", action="store_true", help="Resolve/apply then rollback")
    p.add_argument("--json", action="store_true", help="Emit JSON output")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    conn = connect(dsn=resolve_dsn(args.dsn), autocommit=False)
    try:
        schema = ensure_holdings_schema(conn, schema_sql_path=Path(args.schema_sql))
        parsed = build_rows_from_ticker_quantities(
            conn,
            account_id=str(args.account_id),
            ticker_to_qty=dict(PORTFOLIO_POSITIONS),
            source="seed_mock",
        )
        out = apply_holdings_import(
            conn,
            parsed=parsed,
            mode="replace_account",
            account_id=str(args.account_id).strip().lower(),
            requested_by=args.requested_by,
            filename="<positions_store>",
            notes="seeded from backend.portfolio.positions_store",
            dry_run=bool(args.dry_run),
        )
        out["schema_apply"] = schema
        out["input_positions"] = len(PORTFOLIO_POSITIONS)
    finally:
        conn.close()

    if args.json:
        print(json.dumps(out, indent=2, default=str))
    else:
        print(f"Seed status: {out.get('status')}")
        print(
            f"account_id={out.get('account_id')} accepted={out.get('accepted_rows')} "
            f"rejected={out.get('rejected_rows')}"
        )
        for msg in out.get("warnings", [])[:20]:
            print(f"- warning: {msg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
