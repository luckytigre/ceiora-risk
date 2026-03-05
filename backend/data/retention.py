"""Lookback-aware pruning helpers for analytics history tables."""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TypedDict


DATA_TABLES: tuple[tuple[str, str], ...] = (
    ("barra_raw_cross_section_history", "as_of_date"),
    ("security_prices_eod", "date"),
    ("security_fundamentals_pit", "as_of_date"),
    ("security_classification_pit", "as_of_date"),
    ("model_factor_returns_daily", "date"),
)

CACHE_TABLES: tuple[tuple[str, str], ...] = (
    ("daily_factor_returns", "date"),
    ("daily_specific_residuals", "date"),
    ("daily_universe_eligibility_summary", "date"),
)


class TablePruneResult(TypedDict):
    table: str
    date_column: str
    exists: bool
    rows_before: int
    rows_older_than_cutoff: int
    rows_deleted: int
    rows_after: int


class PruneResult(TypedDict):
    status: str
    cutoff_date: str
    keep_years: int
    dry_run: bool
    data_db: str
    cache_db: str
    data_tables: list[TablePruneResult]
    cache_tables: list[TablePruneResult]


def _parse_as_of_date(value: str | None) -> date:
    if value and str(value).strip():
        return datetime.fromisoformat(str(value).strip()).date()
    return datetime.utcnow().date()


def _cutoff_date(*, keep_years: int, as_of_date: str | None) -> date:
    years = max(1, int(keep_years))
    return _parse_as_of_date(as_of_date) - timedelta(days=365 * years)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def _count_rows(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] or 0)


def _count_older_rows(conn: sqlite3.Connection, table: str, date_col: str, cutoff_iso: str) -> int:
    return int(
        conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {date_col} < ?",
            (cutoff_iso,),
        ).fetchone()[0]
        or 0
    )


def _delete_older_rows(conn: sqlite3.Connection, table: str, date_col: str, cutoff_iso: str) -> int:
    before = conn.total_changes
    conn.execute(
        f"DELETE FROM {table} WHERE {date_col} < ?",
        (cutoff_iso,),
    )
    return int(conn.total_changes - before)


def _prune_one_db(
    *,
    db_path: Path,
    table_specs: tuple[tuple[str, str], ...],
    cutoff_iso: str,
    dry_run: bool,
) -> list[TablePruneResult]:
    conn = sqlite3.connect(str(db_path))
    out: list[TablePruneResult] = []
    try:
        for table, date_col in table_specs:
            if not _table_exists(conn, table):
                out.append(
                    {
                        "table": table,
                        "date_column": date_col,
                        "exists": False,
                        "rows_before": 0,
                        "rows_older_than_cutoff": 0,
                        "rows_deleted": 0,
                        "rows_after": 0,
                    }
                )
                continue

            rows_before = _count_rows(conn, table)
            older_rows = _count_older_rows(conn, table, date_col, cutoff_iso)
            rows_deleted = 0
            if not dry_run and older_rows > 0:
                rows_deleted = _delete_older_rows(conn, table, date_col, cutoff_iso)
            rows_after = rows_before if dry_run else _count_rows(conn, table)

            out.append(
                {
                    "table": table,
                    "date_column": date_col,
                    "exists": True,
                    "rows_before": rows_before,
                    "rows_older_than_cutoff": older_rows,
                    "rows_deleted": rows_deleted,
                    "rows_after": rows_after,
                }
            )

        if not dry_run:
            conn.commit()
    finally:
        conn.close()
    return out


def prune_history_by_lookback(
    *,
    data_db: Path,
    cache_db: Path,
    keep_years: int,
    as_of_date: str | None = None,
    dry_run: bool = True,
) -> PruneResult:
    """Prune historical analytics inputs/outputs older than a lookback window."""
    cutoff = _cutoff_date(keep_years=keep_years, as_of_date=as_of_date)
    cutoff_iso = cutoff.isoformat()

    data_tables = _prune_one_db(
        db_path=data_db,
        table_specs=DATA_TABLES,
        cutoff_iso=cutoff_iso,
        dry_run=bool(dry_run),
    )
    cache_tables = _prune_one_db(
        db_path=cache_db,
        table_specs=CACHE_TABLES,
        cutoff_iso=cutoff_iso,
        dry_run=bool(dry_run),
    )

    return {
        "status": "ok",
        "cutoff_date": cutoff_iso,
        "keep_years": max(1, int(keep_years)),
        "dry_run": bool(dry_run),
        "data_db": str(data_db),
        "cache_db": str(cache_db),
        "data_tables": data_tables,
        "cache_tables": cache_tables,
    }
