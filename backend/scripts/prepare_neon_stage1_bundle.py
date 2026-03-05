#!/usr/bin/env python3
"""Create a Stage-1 Neon prep bundle from the local SQLite backend."""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.data.health_audit import CORE_TABLES, run_sqlite_health_audit


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_schema_dump(conn: sqlite3.Connection, out_path: Path) -> None:
    rows = conn.execute(
        """
        SELECT type, name, sql
        FROM sqlite_master
        WHERE sql IS NOT NULL
          AND type IN ('table', 'index', 'view', 'trigger')
        ORDER BY
          CASE type
            WHEN 'table' THEN 1
            WHEN 'index' THEN 2
            WHEN 'view' THEN 3
            WHEN 'trigger' THEN 4
            ELSE 9
          END,
          name
        """
    ).fetchall()
    chunks = [
        "-- SQLite schema snapshot for Neon Stage-1 prep",
        f"-- generated_at_utc: {datetime.now(timezone.utc).isoformat()}",
        "",
    ]
    for r in rows:
        chunks.append(f"-- {r[0]}: {r[1]}")
        chunks.append(str(r[2]).rstrip(";") + ";")
        chunks.append("")
    out_path.write_text("\n".join(chunks), encoding="utf-8")


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(r[1]) for r in rows]


def _export_table_csv_gz(
    conn: sqlite3.Connection,
    *,
    table: str,
    out_path: Path,
    batch_size: int,
) -> dict[str, Any]:
    cols = _table_columns(conn, table)
    if not cols:
        raise RuntimeError(f"table not found or has no columns: {table}")

    cur = conn.execute(f"SELECT * FROM {table}")
    rows_written = 0
    with gzip.open(out_path, "wt", newline="", encoding="utf-8") as gz:
        writer = csv.writer(gz)
        writer.writerow(cols)
        while True:
            chunk = cur.fetchmany(batch_size)
            if not chunk:
                break
            writer.writerows(chunk)
            rows_written += len(chunk)
    return {
        "table": table,
        "path": str(out_path),
        "rows_written": int(rows_written),
        "sha256": _sha256(out_path),
        "size_bytes": int(out_path.stat().st_size),
    }


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--db-path",
        type=Path,
        default=Path("backend/data.db"),
        help="Path to SQLite source DB",
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        default=Path("backend/neon_stage1_prep"),
        help="Output directory root for generated bundle",
    )
    p.add_argument(
        "--bundle-name",
        default=None,
        help="Optional explicit bundle folder name (default: neon_stage1_<timestamp>)",
    )
    p.add_argument(
        "--export-core-data",
        action="store_true",
        help="Also export core tables to gzipped CSV files (large output).",
    )
    p.add_argument(
        "--table-batch-size",
        type=int,
        default=50_000,
        help="Batch size for CSV export streaming.",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    if not db_path.exists():
        raise FileNotFoundError(f"DB file not found: {db_path}")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bundle_dir = Path(args.out_dir).expanduser().resolve()
    bundle_name = str(args.bundle_name or f"neon_stage1_{stamp}")
    out = bundle_dir / bundle_name
    out.mkdir(parents=True, exist_ok=False)

    schema_path = out / "sqlite_schema.sql"
    audit_path = out / "sqlite_health_audit.json"
    manifest_path = out / "manifest.json"
    exports_dir = out / "exports"

    conn = sqlite3.connect(str(db_path))
    try:
        _write_schema_dump(conn, schema_path)
    finally:
        conn.close()

    audit = run_sqlite_health_audit(db_path, include_integrity_pragmas=True)
    audit_path.write_text(json.dumps(audit, indent=2), encoding="utf-8")

    exports: list[dict[str, Any]] = []
    if bool(args.export_core_data):
        exports_dir.mkdir(parents=True, exist_ok=True)
        exp_conn = sqlite3.connect(str(db_path))
        try:
            for table in CORE_TABLES:
                target = exports_dir / f"{table}.csv.gz"
                exports.append(
                    _export_table_csv_gz(
                        exp_conn,
                        table=table,
                        out_path=target,
                        batch_size=max(1, int(args.table_batch_size)),
                    )
                )
        finally:
            exp_conn.close()

    manifest = {
        "bundle_name": bundle_name,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_db": str(db_path),
        "source_db_size_bytes": int(db_path.stat().st_size),
        "source_db_sha256": _sha256(db_path),
        "files": {
            "schema": {
                "path": str(schema_path),
                "sha256": _sha256(schema_path),
                "size_bytes": int(schema_path.stat().st_size),
            },
            "audit": {
                "path": str(audit_path),
                "sha256": _sha256(audit_path),
                "size_bytes": int(audit_path.stat().st_size),
            },
            "exports": exports,
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(str(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

