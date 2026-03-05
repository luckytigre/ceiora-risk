"""Schema helpers for canonical TRBC naming."""

from __future__ import annotations

import sqlite3
from typing import Iterable


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


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cols = _table_columns(conn, table)
    if column in cols:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def pick_trbc_industry_column(columns: Iterable[str]) -> str | None:
    cols = set(columns)
    for col in ("trbc_industry_group", "gics_industry_group", "industry_group"):
        if col in cols:
            return col
    return None


def pick_trbc_economic_sector_short_column(columns: Iterable[str]) -> str | None:
    cols = set(columns)
    for col in ("trbc_economic_sector_short", "trbc_sector", "trbc_economic_sector", "sector"):
        if col in cols:
            return col
    return None


def pick_trbc_business_sector_column(columns: Iterable[str]) -> str | None:
    cols = set(columns)
    for col in ("trbc_business_sector", "business_sector"):
        if col in cols:
            return col
    return None


def pick_trbc_industry_name_column(columns: Iterable[str]) -> str | None:
    cols = set(columns)
    for col in ("trbc_industry", "industry_name"):
        if col in cols:
            return col
    return None


def pick_trbc_activity_column(columns: Iterable[str]) -> str | None:
    cols = set(columns)
    for col in ("trbc_activity", "activity_name"):
        if col in cols:
            return col
    return None


def ensure_trbc_naming(conn: sqlite3.Connection) -> None:
    """Ensure canonical TRBC columns exist on the canonical classification PIT table."""
    table = "security_classification_pit"
    if not _table_exists(conn, table):
        return
    _ensure_column(conn, table, "trbc_economic_sector", "TEXT")
    _ensure_column(conn, table, "trbc_business_sector", "TEXT")
    _ensure_column(conn, table, "trbc_industry_group", "TEXT")
    _ensure_column(conn, table, "trbc_industry", "TEXT")
    _ensure_column(conn, table, "trbc_activity", "TEXT")
