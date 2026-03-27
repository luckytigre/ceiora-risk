#!/usr/bin/env python3
"""Repair the local SQLite security_prices_eod archive on a working copy and optionally swap it into place."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.services.neon_stage2 import inspect_sqlite_source_integrity


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _sqlite_backup(source_path: Path, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists():
        target_path.unlink()
    source = sqlite3.connect(str(source_path))
    try:
        target = sqlite3.connect(str(target_path))
        try:
            source.backup(target)
        finally:
            target.close()
    finally:
        source.close()


def _create_prices_table(conn: sqlite3.Connection, table: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            ric TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adj_close REAL,
            volume REAL,
            currency TEXT,
            source TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (ric, date)
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_date ON {table}(date)")


def rebuild_security_prices_eod(conn: sqlite3.Connection) -> dict[str, Any]:
    if conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name='security_prices_eod'
        LIMIT 1
        """
    ).fetchone() is None:
        raise RuntimeError("security_prices_eod is missing from the working database")

    source_table = "security_prices_eod__repair_source"
    rebuilt_table = "security_prices_eod__repair_new"
    conn.execute(f"DROP TABLE IF EXISTS {rebuilt_table}")
    conn.execute(f"DROP TABLE IF EXISTS {source_table}")
    conn.execute("ALTER TABLE security_prices_eod RENAME TO security_prices_eod__repair_source")
    _create_prices_table(conn, rebuilt_table)
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {rebuilt_table} (
            ric, date, open, high, low, close, adj_close, volume, currency, source, updated_at
        )
        SELECT
            ric, date, open, high, low, close, adj_close, volume, currency, source, updated_at
        FROM (
            SELECT
                UPPER(TRIM(ric)) AS ric,
                TRIM(date) AS date,
                CAST(open AS REAL) AS open,
                CAST(high AS REAL) AS high,
                CAST(low AS REAL) AS low,
                CAST(close AS REAL) AS close,
                CAST(adj_close AS REAL) AS adj_close,
                CAST(volume AS REAL) AS volume,
                NULLIF(TRIM(currency), '') AS currency,
                NULLIF(TRIM(source), '') AS source,
                COALESCE(NULLIF(TRIM(updated_at), ''), datetime('now')) AS updated_at,
                ROW_NUMBER() OVER (
                    PARTITION BY UPPER(TRIM(ric)), TRIM(date)
                    ORDER BY COALESCE(NULLIF(TRIM(updated_at), ''), datetime('now')) DESC, rowid DESC
                ) AS rn
            FROM {source_table}
            WHERE ric IS NOT NULL
              AND TRIM(ric) <> ''
              AND date IS NOT NULL
              AND TRIM(date) <> ''
        ) ranked
        WHERE rn = 1
        """
    )
    source_row_count = int(conn.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()[0] or 0)
    rebuilt_row_count = int(conn.execute(f"SELECT COUNT(*) FROM {rebuilt_table}").fetchone()[0] or 0)
    conn.execute(f"DROP TABLE {source_table}")
    conn.execute(f"ALTER TABLE {rebuilt_table} RENAME TO security_prices_eod")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_security_prices_eod_date ON security_prices_eod(date)")
    return {
        "source_row_count": source_row_count,
        "rebuilt_row_count": rebuilt_row_count,
    }


def repair_security_prices_archive(
    *,
    source_db: Path,
    working_db: Path | None = None,
    backup_dir: Path | None = None,
    apply_changes: bool = False,
) -> dict[str, Any]:
    source_path = Path(source_db).expanduser().resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"sqlite db not found: {source_path}")

    working_path = (
        Path(working_db).expanduser().resolve()
        if working_db is not None
        else source_path.with_name(f"{source_path.stem}.prices_repair_{_timestamp()}.db")
    )
    backup_path = (
        Path(backup_dir).expanduser().resolve() / f"{source_path.stem}.pre_prices_repair_{_timestamp()}.db"
        if backup_dir is not None
        else source_path.with_name(f"{source_path.stem}.pre_prices_repair_{_timestamp()}.db")
    )

    _sqlite_backup(source_path, working_path)
    pre_repair = inspect_sqlite_source_integrity(
        sqlite_path=working_path,
        selected_tables=["security_prices_eod"],
        run_sqlite_integrity_check=False,
    )

    working_conn = sqlite3.connect(str(working_path))
    try:
        working_conn.execute("BEGIN IMMEDIATE")
        repair_stats = rebuild_security_prices_eod(working_conn)
        working_conn.commit()
    except Exception:
        working_conn.rollback()
        raise
    finally:
        working_conn.close()

    vacuum_conn = sqlite3.connect(str(working_path))
    try:
        vacuum_conn.execute("VACUUM")
        vacuum_conn.execute("ANALYZE")
        vacuum_conn.commit()
    finally:
        vacuum_conn.close()

    post_repair = inspect_sqlite_source_integrity(
        sqlite_path=working_path,
        selected_tables=["security_prices_eod"],
        run_sqlite_integrity_check=True,
    )
    if str(post_repair.get("status") or "") != "ok":
        raise RuntimeError(
            "repaired working database failed integrity validation: "
            + "; ".join(list(post_repair.get("issues") or []))
        )

    out = {
        "status": "ok",
        "source_db": str(source_path),
        "working_db": str(working_path),
        "backup_path": None,
        "applied": False,
        "pre_repair": pre_repair,
        "repair_stats": repair_stats,
        "post_repair": post_repair,
    }

    if not apply_changes:
        return out

    backup_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, backup_path)
    try:
        os.replace(str(working_path), str(source_path))
    except Exception:
        if backup_path.exists():
            shutil.copy2(backup_path, source_path)
        raise
    out["backup_path"] = str(backup_path)
    out["applied"] = True
    out["working_db"] = str(source_path)
    return out


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("backend/runtime/data.db"),
        help="Source SQLite archive to repair.",
    )
    parser.add_argument(
        "--working-db",
        type=Path,
        default=None,
        help="Optional working-copy path. Defaults to a sibling timestamped file.",
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        default=None,
        help="Optional directory for the pre-repair backup when --apply is used.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Atomically replace the source DB with the repaired working copy after validation.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    out = repair_security_prices_archive(
        source_db=Path(args.db_path),
        working_db=(Path(args.working_db) if args.working_db is not None else None),
        backup_dir=(Path(args.backup_dir) if args.backup_dir is not None else None),
        apply_changes=bool(args.apply),
    )
    if args.json:
        print(json.dumps(out, indent=2))
    else:
        print(f"Repair status: {out.get('status')}")
        print(f"- source_db: {out.get('source_db')}")
        print(f"- working_db: {out.get('working_db')}")
        print(f"- applied: {out.get('applied')}")
        if out.get("backup_path"):
            print(f"- backup_path: {out.get('backup_path')}")
        print(
            "- prices repair: "
            f"source_rows={out['repair_stats']['source_row_count']} "
            f"rebuilt_rows={out['repair_stats']['rebuilt_row_count']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
