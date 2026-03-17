"""Schema helpers for the canonical cross-section snapshot table."""

from __future__ import annotations

import sqlite3


TABLE = "universe_cross_section_snapshot"


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
    if not table_exists(conn, table):
        return set()
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def table_column_list(conn: sqlite3.Connection, table: str) -> list[str]:
    if not table_exists(conn, table):
        return []
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(r[1]) for r in rows]


def pk_cols(conn: sqlite3.Connection, table: str) -> list[str]:
    if not table_exists(conn, table):
        return []
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    ranked = sorted(
        [(int(r[5] or 0), str(r[1])) for r in rows if int(r[5] or 0) > 0],
        key=lambda x: x[0],
    )
    return [name for _, name in ranked]


def ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cols = table_columns(conn, table)
    if column in cols:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def create_cross_section_snapshot_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            ric TEXT NOT NULL,
            ticker TEXT,
            as_of_date TEXT NOT NULL,
            fundamental_fetch_date TEXT,
            fundamental_period_end_date TEXT,
            market_cap REAL,
            shares_outstanding REAL,
            dividend_yield REAL,
            common_name TEXT,
            book_value REAL,
            forward_eps REAL,
            trailing_eps REAL,
            total_debt REAL,
            cash_and_equivalents REAL,
            long_term_debt REAL,
            free_cash_flow REAL,
            gross_profit REAL,
            net_income REAL,
            operating_cashflow REAL,
            capital_expenditures REAL,
            shares_basic REAL,
            shares_diluted REAL,
            free_float_shares REAL,
            free_float_percent REAL,
            revenue REAL,
            ebitda REAL,
            ebit REAL,
            total_assets REAL,
            total_liabilities REAL,
            return_on_equity REAL,
            operating_margins REAL,
            report_currency TEXT,
            fiscal_year INTEGER,
            period_type TEXT,
            trbc_economic_sector_short TEXT,
            trbc_economic_sector TEXT,
            trbc_business_sector TEXT,
            trbc_industry_group TEXT,
            trbc_industry TEXT,
            trbc_activity TEXT,
            trbc_effective_date TEXT,
            price_date TEXT,
            price_close REAL,
            price_currency TEXT,
            fundamental_source TEXT,
            trbc_source TEXT,
            price_source TEXT,
            fundamental_job_run_id TEXT,
            trbc_job_run_id TEXT,
            snapshot_job_run_id TEXT,
            updated_at TEXT,
            PRIMARY KEY (ric, as_of_date)
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_asof ON {TABLE}(as_of_date)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_ticker ON {TABLE}(ticker)")


def rebuild_snapshot_table(conn: sqlite3.Connection, legacy_suffix: str) -> None:
    legacy = f"{TABLE}__legacy_{legacy_suffix}"
    conn.execute(f"DROP TABLE IF EXISTS {legacy}")
    conn.execute(f"ALTER TABLE {TABLE} RENAME TO {legacy}")
    create_cross_section_snapshot_table(conn)
    new_cols = table_column_list(conn, TABLE)
    legacy_cols = set(table_column_list(conn, legacy))
    keep_cols = [c for c in new_cols if c in legacy_cols and c not in {"ric", "as_of_date"}]
    ric_expr = "UPPER(TRIM(ric))"
    if "ric" not in legacy_cols:
        ric_expr = "NULL"
    asof_expr = "TRIM(as_of_date)"
    if "as_of_date" not in legacy_cols:
        asof_expr = "NULL"
    updated_sort = "datetime('now')"
    if "updated_at" in legacy_cols:
        updated_sort = "COALESCE(NULLIF(TRIM(updated_at), ''), datetime('now'))"
    if "ric" in legacy_cols and "as_of_date" in legacy_cols:
        projected_cols = ",\n                ".join([f"{c}" for c in keep_cols])
        selected_cols = ",\n                ".join([f"{c}" for c in keep_cols])
        if projected_cols:
            projected_cols = ",\n                " + projected_cols
            selected_cols = ",\n                " + selected_cols
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {TABLE} (
                ric, as_of_date{projected_cols}
            )
            SELECT
                ric, as_of_date{selected_cols}
            FROM (
                SELECT
                    {ric_expr} AS ric,
                    {asof_expr} AS as_of_date{selected_cols},
                    ROW_NUMBER() OVER (
                        PARTITION BY {ric_expr}, {asof_expr}
                        ORDER BY {updated_sort} DESC, rowid DESC
                    ) AS rn
                FROM {legacy}
                WHERE {ric_expr} IS NOT NULL AND TRIM({ric_expr}) <> ''
                  AND {asof_expr} IS NOT NULL AND TRIM({asof_expr}) <> ''
            ) ranked
            WHERE rn = 1
            """
        )
    conn.execute(f"DROP TABLE IF EXISTS {legacy}")


def ensure_cross_section_snapshot_table(conn: sqlite3.Connection) -> None:
    create_cross_section_snapshot_table(conn)
    cols = table_columns(conn, TABLE)
    pk = pk_cols(conn, TABLE)
    if "price_exchange" in cols or pk != ["ric", "as_of_date"]:
        rebuild_snapshot_table(conn, "canonical_keys")
        cols = table_columns(conn, TABLE)
    for col, ddl in [
        ("ric", "TEXT"),
        ("cash_and_equivalents", "REAL"),
        ("long_term_debt", "REAL"),
        ("gross_profit", "REAL"),
        ("net_income", "REAL"),
        ("operating_cashflow", "REAL"),
        ("capital_expenditures", "REAL"),
        ("shares_basic", "REAL"),
        ("shares_diluted", "REAL"),
        ("free_float_shares", "REAL"),
        ("free_float_percent", "REAL"),
        ("report_currency", "TEXT"),
        ("fiscal_year", "INTEGER"),
        ("period_type", "TEXT"),
        ("trbc_economic_sector_short", "TEXT"),
    ]:
        ensure_column(conn, TABLE, col, ddl)
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_ric ON {TABLE}(ric)")
