#!/usr/bin/env python3
"""Migrate/backfill canonical SID-keyed time-series tables and drop deprecated physical tables."""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from cuse4.schema import (
    FUNDAMENTALS_HISTORY_TABLE,
    PRICES_TABLE,
    SECURITY_MASTER_TABLE,
    TRBC_HISTORY_TABLE,
    ensure_cuse4_schema,
)

DEPRECATED_TABLES = [
    "ticker_ric_map",
    "fundamental_snapshots",
    "trbc_industry_history",
    "prices_daily",
    "fundamentals_history",
    "trbc_industry_country_history",
    "universe_candidate_holdings",
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


def _view_exists(conn: sqlite3.Connection, view: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='view' AND name=?
        LIMIT 1
        """,
        (view,),
    ).fetchone()
    return row is not None


def _distinct_dates(conn: sqlite3.Connection, table: str, col: str) -> list[str]:
    if not _table_exists(conn, table):
        return []
    rows = conn.execute(
        f"""
        SELECT DISTINCT {col}
        FROM {table}
        WHERE {col} IS NOT NULL
          AND TRIM({col}) <> ''
        ORDER BY {col}
        """
    ).fetchall()
    return [str(r[0]) for r in rows if r and r[0]]


def _iter_chunks(values: list[str], chunk_size: int) -> Iterable[list[str]]:
    n = max(1, int(chunk_size))
    for i in range(0, len(values), n):
        yield values[i : i + n]


def _clear_canonical_tables(conn: sqlite3.Connection) -> None:
    conn.execute(f"DELETE FROM {FUNDAMENTALS_HISTORY_TABLE}")
    conn.execute(f"DELETE FROM {TRBC_HISTORY_TABLE}")
    conn.execute(f"DELETE FROM {PRICES_TABLE}")
    conn.commit()


def _backfill_fundamentals(conn: sqlite3.Connection, *, date_chunk_size: int) -> int:
    src = "fundamental_snapshots"
    if not _table_exists(conn, src):
        return 0

    dates = _distinct_dates(conn, src, "fetch_date")
    total = 0
    now_iso = datetime.now(timezone.utc).isoformat()

    for chunk in _iter_chunks(dates, date_chunk_size):
        placeholders = ",".join("?" for _ in chunk)
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {FUNDAMENTALS_HISTORY_TABLE} (
                sid, as_of_date, stat_date, period_end_date, fiscal_year, period_type, report_currency,
                market_cap, shares_outstanding, dividend_yield, book_value_per_share,
                total_assets, total_debt, cash_and_equivalents, long_term_debt,
                operating_cashflow, capital_expenditures, trailing_eps, forward_eps,
                revenue, ebitda, ebit, roe_pct, roa_pct, operating_margin_pct,
                common_name, source, job_run_id, updated_at
            )
            SELECT
                sm.sid,
                f.fetch_date AS as_of_date,
                COALESCE(NULLIF(TRIM(f.fundamental_period_end_date), ''), f.fetch_date) AS stat_date,
                NULLIF(TRIM(f.fundamental_period_end_date), '') AS period_end_date,
                f.fiscal_year,
                f.period_type,
                f.report_currency,
                CAST(f.market_cap AS REAL),
                CAST(f.shares_outstanding AS REAL),
                CAST(f.dividend_yield AS REAL),
                CAST(f.book_value AS REAL),
                CAST(f.total_assets AS REAL),
                CAST(f.total_debt AS REAL),
                CAST(f.cash_and_equivalents AS REAL),
                CAST(f.long_term_debt AS REAL),
                CAST(f.operating_cashflow AS REAL),
                CAST(f.capital_expenditures AS REAL),
                CAST(f.trailing_eps AS REAL),
                CAST(f.forward_eps AS REAL),
                CAST(f.revenue AS REAL),
                CAST(f.ebitda AS REAL),
                CAST(f.ebit AS REAL),
                CAST(f.return_on_equity AS REAL),
                CASE
                    WHEN f.total_assets IS NOT NULL AND ABS(CAST(f.total_assets AS REAL)) > 1e-12 AND f.net_income IS NOT NULL
                    THEN CAST(f.net_income AS REAL) / CAST(f.total_assets AS REAL)
                    ELSE NULL
                END AS roa_pct,
                CAST(f.operating_margins AS REAL),
                f.common_name,
                COALESCE(NULLIF(TRIM(f.source), ''), 'legacy_backfill'),
                COALESCE(NULLIF(TRIM(f.job_run_id), ''), 'canonical_migration'),
                COALESCE(NULLIF(TRIM(f.updated_at), ''), ?)
            FROM fundamental_snapshots f
            JOIN {SECURITY_MASTER_TABLE} sm
              ON UPPER(TRIM(f.ticker)) = sm.ticker
            WHERE f.fetch_date IN ({placeholders})
              AND COALESCE(sm.classification_ok, 0) = 1
              AND COALESCE(sm.is_equity_eligible, 0) = 1
            """,
            (now_iso, *chunk),
        )
        change_row = conn.execute("SELECT changes()").fetchone()
        total += int(change_row[0] or 0) if change_row else 0
        conn.commit()
    return total


def _backfill_classification(conn: sqlite3.Connection, *, date_chunk_size: int) -> int:
    src = "trbc_industry_history"
    if not _table_exists(conn, src):
        return 0

    dates = _distinct_dates(conn, src, "as_of_date")
    total = 0
    now_iso = datetime.now(timezone.utc).isoformat()

    for chunk in _iter_chunks(dates, date_chunk_size):
        placeholders = ",".join("?" for _ in chunk)
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {TRBC_HISTORY_TABLE} (
                sid, as_of_date, trbc_economic_sector, trbc_business_sector,
                trbc_industry_group, trbc_industry, trbc_activity, hq_country_code,
                source, job_run_id, updated_at
            )
            SELECT
                sm.sid,
                h.as_of_date,
                NULLIF(TRIM(h.trbc_economic_sector), ''),
                NULLIF(TRIM(h.trbc_business_sector), ''),
                NULLIF(TRIM(h.trbc_industry_group), ''),
                NULLIF(TRIM(h.trbc_industry), ''),
                NULLIF(TRIM(h.trbc_activity), ''),
                NULLIF(UPPER(TRIM(h.hq_country_code)), ''),
                COALESCE(NULLIF(TRIM(h.source), ''), 'legacy_backfill'),
                COALESCE(NULLIF(TRIM(h.job_run_id), ''), 'canonical_migration'),
                COALESCE(NULLIF(TRIM(h.updated_at), ''), ?)
            FROM trbc_industry_history h
            JOIN {SECURITY_MASTER_TABLE} sm
              ON UPPER(TRIM(h.ticker)) = sm.ticker
            WHERE h.as_of_date IN ({placeholders})
              AND COALESCE(sm.classification_ok, 0) = 1
              AND COALESCE(sm.is_equity_eligible, 0) = 1
            """,
            (now_iso, *chunk),
        )
        change_row = conn.execute("SELECT changes()").fetchone()
        total += int(change_row[0] or 0) if change_row else 0
        conn.commit()
    return total


def _backfill_prices(conn: sqlite3.Connection, *, date_chunk_size: int) -> int:
    src = "prices_daily"
    if not _table_exists(conn, src):
        return 0

    dates = _distinct_dates(conn, src, "date")
    total = 0
    now_iso = datetime.now(timezone.utc).isoformat()

    for chunk in _iter_chunks(dates, date_chunk_size):
        placeholders = ",".join("?" for _ in chunk)
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {PRICES_TABLE} (
                sid, date, open, high, low, close, adj_close, volume,
                currency, exchange, source, updated_at
            )
            SELECT
                sm.sid,
                p.date,
                CAST(p.open AS REAL),
                CAST(p.high AS REAL),
                CAST(p.low AS REAL),
                CAST(p.close AS REAL),
                CAST(p.adj_close AS REAL),
                CAST(p.volume AS REAL),
                p.currency,
                p.exchange,
                COALESCE(NULLIF(TRIM(p.source), ''), 'legacy_backfill'),
                COALESCE(NULLIF(TRIM(p.updated_at), ''), ?)
            FROM prices_daily p
            JOIN {SECURITY_MASTER_TABLE} sm
              ON UPPER(TRIM(p.ticker)) = sm.ticker
            WHERE p.date IN ({placeholders})
              AND COALESCE(sm.classification_ok, 0) = 1
              AND COALESCE(sm.is_equity_eligible, 0) = 1
            """,
            (now_iso, *chunk),
        )
        change_row = conn.execute("SELECT changes()").fetchone()
        total += int(change_row[0] or 0) if change_row else 0
        conn.commit()
    return total


def _drop_deprecated_tables(conn: sqlite3.Connection) -> None:
    for name in DEPRECATED_TABLES:
        row = conn.execute(
            """
            SELECT type
            FROM sqlite_master
            WHERE name=?
            LIMIT 1
            """,
            (name,),
        ).fetchone()
        if row is None:
            continue
        obj_type = str(row[0]).strip().lower()
        if obj_type == "view":
            conn.execute(f"DROP VIEW IF EXISTS {name}")
        else:
            conn.execute(f"DROP TABLE IF EXISTS {name}")
    conn.commit()


def _create_compat_views(conn: sqlite3.Connection) -> None:
    # Backward-compatible read views only (no duplicate storage).
    conn.execute(
        f"""
        CREATE VIEW IF NOT EXISTS fundamentals_history AS
        SELECT *
        FROM {FUNDAMENTALS_HISTORY_TABLE}
        """
    )
    conn.execute(
        f"""
        CREATE VIEW IF NOT EXISTS trbc_industry_country_history AS
        SELECT *
        FROM {TRBC_HISTORY_TABLE}
        """
    )
    conn.execute(
        f"""
        CREATE VIEW IF NOT EXISTS prices_daily AS
        SELECT
            sm.ticker,
            p.date,
            p.open,
            p.high,
            p.low,
            p.close,
            p.adj_close,
            p.volume,
            p.currency,
            p.exchange,
            p.source,
            p.updated_at
        FROM {PRICES_TABLE} p
        JOIN {SECURITY_MASTER_TABLE} sm
          ON sm.sid = p.sid
        """
    )
    conn.execute(
        f"""
        CREATE VIEW IF NOT EXISTS ticker_ric_map AS
        SELECT
            sm.ticker AS ticker,
            sm.ric AS ric,
            'security_master' AS resolution_method,
            COALESCE(sm.classification_ok, 0) AS classification_ok,
            NULL AS as_of_date,
            'security_master' AS source,
            sm.updated_at AS updated_at
        FROM {SECURITY_MASTER_TABLE} sm
        WHERE sm.ticker IS NOT NULL
          AND TRIM(sm.ticker) <> ''
          AND sm.ric IS NOT NULL
          AND TRIM(sm.ric) <> ''
        """
    )
    conn.execute(
        f"""
        CREATE VIEW IF NOT EXISTS fundamental_snapshots AS
        SELECT
            sm.ticker,
            f.as_of_date AS fetch_date,
            f.market_cap,
            f.shares_outstanding,
            f.dividend_yield,
            f.common_name,
            f.book_value_per_share AS book_value,
            f.forward_eps,
            f.trailing_eps,
            f.total_debt,
            f.cash_and_equivalents,
            f.long_term_debt,
            NULL AS free_cash_flow,
            NULL AS gross_profit,
            NULL AS net_income,
            f.operating_cashflow,
            f.capital_expenditures,
            NULL AS shares_basic,
            NULL AS shares_diluted,
            NULL AS free_float_shares,
            NULL AS free_float_percent,
            f.revenue,
            f.ebitda,
            f.ebit,
            f.total_assets,
            NULL AS total_liabilities,
            f.roe_pct AS return_on_equity,
            f.operating_margin_pct AS operating_margins,
            f.period_end_date AS fundamental_period_end_date,
            f.report_currency,
            f.fiscal_year,
            f.period_type,
            f.source,
            f.job_run_id,
            f.updated_at
        FROM {FUNDAMENTALS_HISTORY_TABLE} f
        JOIN {SECURITY_MASTER_TABLE} sm
          ON sm.sid = f.sid
        """
    )
    conn.execute(
        f"""
        CREATE VIEW IF NOT EXISTS trbc_industry_history AS
        SELECT
            sm.ticker,
            t.as_of_date,
            t.trbc_industry_group,
            t.trbc_economic_sector,
            t.source,
            t.job_run_id,
            t.updated_at,
            t.trbc_business_sector,
            t.trbc_industry,
            t.trbc_activity,
            t.hq_country_code
        FROM {TRBC_HISTORY_TABLE} t
        JOIN {SECURITY_MASTER_TABLE} sm
          ON sm.sid = t.sid
        """
    )
    conn.commit()


def migrate(
    *,
    db_path: Path,
    date_chunk_size: int,
    drop_deprecated: bool,
) -> dict[str, int | str]:
    conn = sqlite3.connect(str(db_path), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")

    try:
        ensure_cuse4_schema(conn)
        _clear_canonical_tables(conn)

        n_f = _backfill_fundamentals(conn, date_chunk_size=max(1, int(date_chunk_size)))
        n_c = _backfill_classification(conn, date_chunk_size=max(1, int(date_chunk_size)))
        n_p = _backfill_prices(conn, date_chunk_size=max(1, int(date_chunk_size)))

        if drop_deprecated:
            _drop_deprecated_tables(conn)
            _create_compat_views(conn)

        out = {
            "status": "ok",
            "db_path": str(db_path),
            "fundamentals_rows_backfilled": int(n_f),
            "classification_rows_backfilled": int(n_c),
            "prices_rows_backfilled": int(n_p),
            "drop_deprecated": int(bool(drop_deprecated)),
        }
        return out
    finally:
        conn.close()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-path", type=Path, default=Path("backend/data.db"), help="Path to SQLite DB")
    p.add_argument("--date-chunk-size", type=int, default=40, help="Number of dates per backfill batch commit")
    p.add_argument("--drop-deprecated", action="store_true", help="Drop deprecated physical tables and create compatibility views")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    print(
        migrate(
            db_path=args.db_path,
            date_chunk_size=args.date_chunk_size,
            drop_deprecated=bool(args.drop_deprecated),
        )
    )
