"""SQLite inspection helpers for the data-diagnostics service."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_INTERNAL_CACHE_PREFIXES = ("__snap__:",)
_INTERNAL_CACHE_KEYS = {"__cache_snapshot_active"}


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
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


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def first_existing(cols: set[str], candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in cols:
            return candidate
    return None


def approx_row_count_from_stats(conn: sqlite3.Connection, table: str) -> int | None:
    if not table_exists(conn, "sqlite_stat1"):
        return None
    rows = conn.execute(
        "SELECT stat FROM sqlite_stat1 WHERE tbl = ?",
        (table,),
    ).fetchall()
    estimates: list[int] = []
    for (stat,) in rows:
        if not stat:
            continue
        head = str(stat).split(" ", 1)[0].strip()
        if not head:
            continue
        try:
            estimates.append(int(head))
        except ValueError:
            continue
    if not estimates:
        return None
    return max(estimates)


def table_stats(
    conn: sqlite3.Connection,
    table: str,
    *,
    include_exact_row_counts: bool = False,
    include_expensive_checks: bool = False,
) -> dict[str, Any]:
    cols = table_columns(conn, table)
    if not cols:
        return {"table": table, "exists": False}
    date_col = first_existing(cols, ["as_of_date", "fetch_date", "date", "snapshot_date", "start_date"])
    updated_col = first_existing(cols, ["updated_at"])
    ticker_col = "ticker" if "ticker" in cols else None
    job_col = "job_run_id" if "job_run_id" in cols else None

    approx_row_count = approx_row_count_from_stats(conn, table)
    if include_exact_row_counts:
        row_count = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] or 0)
        row_count_mode = "exact"
    else:
        row_count = int(approx_row_count) if approx_row_count is not None else None
        row_count_mode = "approx" if approx_row_count is not None else "unknown"

    ticker_count = None
    if ticker_col and include_expensive_checks:
        ticker_count = int(conn.execute(f"SELECT COUNT(DISTINCT {ticker_col}) FROM {table}").fetchone()[0] or 0)

    min_date = max_date = None
    if date_col:
        min_max_row = conn.execute(
            f"""
            SELECT MIN({date_col}), MAX({date_col})
            FROM {table}
            WHERE {date_col} IS NOT NULL
            """
        ).fetchone()
        min_date = min_max_row[0] if min_max_row else None
        max_date = min_max_row[1] if min_max_row else None

    last_updated_at = None
    if updated_col and include_expensive_checks:
        last_updated_at = conn.execute(f"SELECT MAX({updated_col}) FROM {table}").fetchone()[0]

    last_job_run_id = None
    if job_col and include_expensive_checks:
        if updated_col:
            row = conn.execute(
                f"""
                SELECT {job_col}
                FROM {table}
                WHERE {job_col} IS NOT NULL AND TRIM({job_col}) <> ''
                ORDER BY {updated_col} DESC
                LIMIT 1
                """
            ).fetchone()
        else:
            row = conn.execute(
                f"""
                SELECT {job_col}
                FROM {table}
                WHERE {job_col} IS NOT NULL AND TRIM({job_col}) <> ''
                ORDER BY {job_col} DESC
                LIMIT 1
                """
            ).fetchone()
        last_job_run_id = row[0] if row else None

    return {
        "table": table,
        "exists": True,
        "row_count": row_count,
        "row_count_mode": row_count_mode,
        "ticker_count": ticker_count,
        "date_column": date_col,
        "min_date": str(min_date) if min_date is not None else None,
        "max_date": str(max_date) if max_date is not None else None,
        "last_updated_at": str(last_updated_at) if last_updated_at is not None else None,
        "last_job_run_id": str(last_job_run_id) if last_job_run_id is not None else None,
    }


def exposure_duplicate_stats(conn: sqlite3.Connection, table: str) -> dict[str, Any]:
    if not table_exists(conn, table):
        return {"table": table, "exists": False}
    cols = table_columns(conn, table)
    if "ticker" not in cols or "as_of_date" not in cols:
        return {"table": table, "exists": True, "duplicate_groups": 0, "duplicate_extra_rows": 0}
    dup_groups, dup_extra = conn.execute(
        f"""
        SELECT COUNT(*), COALESCE(SUM(cnt - 1), 0)
        FROM (
            SELECT ticker, as_of_date, COUNT(*) AS cnt
            FROM {table}
            GROUP BY ticker, as_of_date
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()
    return {
        "table": table,
        "exists": True,
        "duplicate_groups": int(dup_groups or 0),
        "duplicate_extra_rows": int(dup_extra or 0),
    }


def load_cache_rows(cache_db: Path) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(cache_db))
    try:
        if not table_exists(conn, "cache"):
            return []
        rows = conn.execute(
            """
            SELECT key, updated_at
            FROM cache
            ORDER BY updated_at DESC
            """
        ).fetchall()
    finally:
        conn.close()
    out: list[dict[str, Any]] = []
    for key, ts in rows:
        key_txt = str(key)
        if key_txt in _INTERNAL_CACHE_KEYS or key_txt.startswith(_INTERNAL_CACHE_PREFIXES):
            continue
        iso = None
        if ts is not None:
            iso = datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
        out.append(
            {
                "key": key_txt,
                "updated_at_unix": float(ts) if ts is not None else None,
                "updated_at_utc": str(iso) if iso is not None else None,
            }
        )
    return out
