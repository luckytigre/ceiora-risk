"""Schema helpers to normalize legacy industry naming to TRBC naming."""

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


def _index_exists(conn: sqlite3.Connection, index_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='index' AND name=?
        LIMIT 1
        """,
        (index_name,),
    ).fetchone()
    return row is not None


def pick_trbc_industry_column(columns: Iterable[str]) -> str | None:
    cols = set(columns)
    for col in ("trbc_industry_group", "gics_industry_group", "industry_group"):
        if col in cols:
            return col
    return None


def ensure_trbc_naming(conn: sqlite3.Connection) -> None:
    """Apply idempotent table/column/index renames from legacy names."""
    # Rename legacy history table.
    if _table_exists(conn, "gics_industry_history") and not _table_exists(conn, "trbc_industry_history"):
        conn.execute("ALTER TABLE gics_industry_history RENAME TO trbc_industry_history")

    # Rename history column.
    hist_cols = _table_columns(conn, "trbc_industry_history")
    if "gics_industry_group" in hist_cols and "trbc_industry_group" not in hist_cols:
        conn.execute(
            "ALTER TABLE trbc_industry_history "
            "RENAME COLUMN gics_industry_group TO trbc_industry_group"
        )

    # Rename exposure column.
    exp_cols = _table_columns(conn, "barra_exposures")
    if "gics_industry_group" in exp_cols and "trbc_industry_group" not in exp_cols:
        conn.execute(
            "ALTER TABLE barra_exposures "
            "RENAME COLUMN gics_industry_group TO trbc_industry_group"
        )

    # Normalize fundamental snapshots naming.
    fund_cols = _table_columns(conn, "fundamental_snapshots")
    if "sector" in fund_cols and "trbc_sector" not in fund_cols:
        conn.execute(
            "ALTER TABLE fundamental_snapshots "
            "RENAME COLUMN sector TO trbc_sector"
        )
        fund_cols = _table_columns(conn, "fundamental_snapshots")
    if "industry" in fund_cols and "trbc_industry_group" not in fund_cols:
        conn.execute(
            "ALTER TABLE fundamental_snapshots "
            "RENAME COLUMN industry TO trbc_industry_group"
        )

    # Normalize historical index names.
    if _table_exists(conn, "trbc_industry_history"):
        if _index_exists(conn, "idx_gics_industry_history_date"):
            conn.execute("DROP INDEX idx_gics_industry_history_date")
        if _index_exists(conn, "idx_gics_industry_history_ticker"):
            conn.execute("DROP INDEX idx_gics_industry_history_ticker")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_trbc_industry_history_date "
            "ON trbc_industry_history(as_of_date)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_trbc_industry_history_ticker "
            "ON trbc_industry_history(ticker)"
        )
