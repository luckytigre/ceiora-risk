#!/usr/bin/env python3
"""Import holdings CSV into Neon using locked import semantics."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from backend.data.neon import connect, resolve_dsn
from backend.services.neon_holdings import (
    IMPORT_MODES,
    apply_holdings_import,
    ensure_holdings_schema,
    parse_holdings_csv,
)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn", default=None, help="Neon DSN (defaults to NEON_DATABASE_URL)")
    p.add_argument("--csv-path", type=Path, required=True, help="Input CSV path")
    p.add_argument(
        "--mode",
        required=True,
        choices=sorted(IMPORT_MODES),
        help="Import mode",
    )
    p.add_argument(
        "--account-id",
        default=None,
        help="Default account_id when file does not contain account_id column",
    )
    p.add_argument(
        "--source",
        default="csv_import",
        help="Default source label",
    )
    p.add_argument(
        "--schema-sql",
        type=Path,
        default=Path("docs/migrations/neon/NEON_HOLDINGS_SCHEMA.sql"),
        help="Holdings schema SQL file",
    )
    p.add_argument(
        "--skip-ensure-schema",
        action="store_true",
        help="Skip applying holdings schema before import",
    )
    p.add_argument("--requested-by", default=None, help="Operator identity")
    p.add_argument("--notes", default=None, help="Optional batch notes")
    p.add_argument("--dry-run", action="store_true", help="Parse/resolve/apply then rollback")
    p.add_argument("--json", action="store_true", help="Emit JSON output")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    dsn = resolve_dsn(args.dsn)

    conn = connect(dsn=dsn, autocommit=False)
    try:
        applied_schema = None
        if not bool(args.skip_ensure_schema):
            applied_schema = ensure_holdings_schema(conn, schema_sql_path=Path(args.schema_sql))

        parsed = parse_holdings_csv(
            conn,
            csv_path=Path(args.csv_path),
            mode=str(args.mode),
            default_account_id=args.account_id,
            default_source=str(args.source),
        )

        account_id = str(args.account_id or "").strip().lower()
        if not account_id:
            accounts = sorted({r.account_id for r in parsed.get("accepted", [])})
            if len(accounts) != 1:
                raise ValueError(
                    "account_id required: provide --account-id or include exactly one valid account_id in CSV"
                )
            account_id = accounts[0]

        out = apply_holdings_import(
            conn,
            parsed=parsed,
            mode=str(args.mode),
            account_id=account_id,
            requested_by=args.requested_by,
            filename=Path(args.csv_path).name,
            notes=args.notes,
            dry_run=bool(args.dry_run),
        )
        if applied_schema is not None:
            out["schema_apply"] = applied_schema
    finally:
        conn.close()

    if args.json:
        print(json.dumps(out, indent=2, default=str))
    else:
        print(f"Holdings import status: {out.get('status')}")
        print(f"account_id={out.get('account_id')} mode={out.get('mode')} batch_id={out.get('import_batch_id')}")
        print(
            f"accepted={out.get('accepted_rows')} rejected={out.get('rejected_rows')} "
            f"upserts={out.get('applied_upserts')} deletes={out.get('applied_deletes')}"
        )
        for code, n in sorted((out.get("rejection_counts") or {}).items()):
            print(f"- rejected[{code}]={n}")
        for msg in out.get("warnings", [])[:20]:
            print(f"- warning: {msg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
