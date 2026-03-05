"""Delete rows whose date keys are not valid XNYS trading sessions."""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from trading_calendar import non_xnys_dates

DATA_TABLE_SPECS = [
    ("security_prices_eod", "date"),
    ("security_fundamentals_pit", "as_of_date"),
    ("security_classification_pit", "as_of_date"),
    ("estu_membership_daily", "date"),
    ("barra_raw_cross_section_history", "as_of_date"),
    ("universe_cross_section_snapshot", "as_of_date"),
]

CACHE_TABLE_SPECS = [
    ("daily_factor_returns", "date"),
    ("daily_specific_residuals", "date"),
    ("daily_universe_eligibility_summary", "date"),
]


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        LIMIT 1
        """,
        (table,),
    ).fetchone()
    return row is not None


def _purge_table(
    conn: sqlite3.Connection,
    *,
    table: str,
    date_col: str,
    dry_run: bool,
) -> dict[str, Any]:
    if not _table_exists(conn, table):
        return {
            "table": table,
            "date_col": date_col,
            "status": "missing",
            "invalid_dates": 0,
            "rows_deleted": 0,
        }

    rows = conn.execute(
        f"SELECT DISTINCT {date_col} FROM {table} WHERE {date_col} IS NOT NULL AND TRIM({date_col}) <> ''"
    ).fetchall()
    distinct_dates = [str(r[0]) for r in rows if r and r[0]]
    invalid_dates = non_xnys_dates(distinct_dates)
    if not invalid_dates:
        return {
            "table": table,
            "date_col": date_col,
            "status": "ok",
            "invalid_dates": 0,
            "rows_deleted": 0,
        }

    placeholders = ",".join("?" for _ in invalid_dates)
    deleted = int(
        conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {date_col} IN ({placeholders})",
            invalid_dates,
        ).fetchone()[0]
        or 0
    )
    if not dry_run and deleted > 0:
        chunk_size = 500
        for i in range(0, len(invalid_dates), chunk_size):
            chunk = invalid_dates[i : i + chunk_size]
            chunk_placeholders = ",".join("?" for _ in chunk)
            conn.execute(
                f"DELETE FROM {table} WHERE {date_col} IN ({chunk_placeholders})",
                chunk,
            )
    return {
        "table": table,
        "date_col": date_col,
        "status": "ok",
        "invalid_dates": len(invalid_dates),
        "rows_deleted": deleted,
    }


def _purge_db(db_path: Path, specs: list[tuple[str, str]], *, dry_run: bool) -> dict[str, Any]:
    if not db_path.exists():
        return {"db_path": str(db_path), "status": "missing", "tables": []}
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        table_results = [
            _purge_table(conn, table=table, date_col=date_col, dry_run=dry_run)
            for table, date_col in specs
        ]
        if not dry_run:
            conn.commit()
    finally:
        conn.close()
    return {
        "db_path": str(db_path),
        "status": "ok",
        "tables": table_results,
        "rows_deleted_total": int(sum(int(t["rows_deleted"]) for t in table_results)),
    }


def _parse_args() -> argparse.Namespace:
    base = Path(__file__).resolve().parent.parent
    p = argparse.ArgumentParser(description="Delete non-XNYS rows from local SQLite tables.")
    p.add_argument("--data-db", default=str(base / "data.db"), help="Path to data SQLite DB.")
    p.add_argument("--cache-db", default=str(base / "cache.db"), help="Path to cache SQLite DB.")
    p.add_argument("--skip-data", action="store_true", help="Skip purging data DB.")
    p.add_argument("--skip-cache", action="store_true", help="Skip purging cache DB.")
    p.add_argument("--dry-run", action="store_true", help="Report deletions without writing.")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    reports: list[dict[str, Any]] = []
    if not args.skip_data:
        reports.append(_purge_db(Path(args.data_db).expanduser(), DATA_TABLE_SPECS, dry_run=bool(args.dry_run)))
    if not args.skip_cache:
        reports.append(_purge_db(Path(args.cache_db).expanduser(), CACHE_TABLE_SPECS, dry_run=bool(args.dry_run)))
    print({"dry_run": bool(args.dry_run), "reports": reports})
