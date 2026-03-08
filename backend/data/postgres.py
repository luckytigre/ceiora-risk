"""Canonical data queries for Barra dashboard."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from psycopg.rows import dict_row

from backend import config
from backend.data.neon import connect, resolve_dsn
from backend.data.trbc_schema import ensure_trbc_naming
from backend.trading_calendar import previous_or_same_xnys_session

DATA_DB = Path(config.DATA_DB_PATH)
_CORE_READ_SURFACE = "core_reads"


def _use_neon_core_reads() -> bool:
    return bool(config.neon_surface_enabled(_CORE_READ_SURFACE))


def _to_pg_sql(query: str) -> str:
    # Runtime SQL in this module is simple and uses `?` only as parameter placeholders.
    return str(query).replace("?", "%s")


def _fetch_rows(sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    if _use_neon_core_reads():
        pg_conn = connect(dsn=resolve_dsn(None), autocommit=True)
        try:
            with pg_conn.cursor(row_factory=dict_row) as cur:
                cur.execute(_to_pg_sql(sql), params or [])
                return [dict(row) for row in cur.fetchall()]
        finally:
            pg_conn.close()

    conn = sqlite3.connect(str(DATA_DB))
    conn.row_factory = sqlite3.Row
    try:
        ensure_trbc_naming(conn)
        cur = conn.execute(sql, params or [])
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def _table_exists(table: str) -> bool:
    if _use_neon_core_reads():
        rows = _fetch_rows(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema='public' AND table_name=?
            LIMIT 1
            """,
            [table],
        )
        return bool(rows)

    rows = _fetch_rows(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        LIMIT 1
        """,
        [table],
    )
    return bool(rows)


def _missing_tables(*tables: str) -> list[str]:
    return [t for t in tables if not _table_exists(t)]


def _resolve_latest_barra_tuple() -> dict[str, str] | None:
    table = _exposure_source_table_required()
    rows = _fetch_rows(
        f"""
        SELECT as_of_date, barra_model_version, descriptor_schema_version, assumption_set_version
        FROM {table}
        ORDER BY as_of_date DESC, updated_at DESC
        LIMIT 1
        """,
    )
    if not rows:
        return None
    row = rows[0]
    return {
        "as_of_date": str(row.get("as_of_date") or ""),
        "barra_model_version": str(row.get("barra_model_version") or ""),
        "descriptor_schema_version": str(row.get("descriptor_schema_version") or ""),
        "assumption_set_version": str(row.get("assumption_set_version") or ""),
    }


def _exposure_source_table_required() -> str:
    table = "barra_raw_cross_section_history"
    if not _table_exists(table):
        raise RuntimeError(
            "Required exposure table missing: barra_raw_cross_section_history. "
            "Build it via backend/scripts/build_barra_raw_cross_section_history.py."
        )
    return table


def load_raw_cross_section_latest(tickers: list[str] | None = None) -> pd.DataFrame:
    table = _exposure_source_table_required()
    params: list[Any] = []
    ticker_clause = ""
    if tickers:
        clean = [t.upper() for t in tickers if t.strip()]
        if clean:
            placeholders = ",".join("?" for _ in clean)
            ticker_clause = f" WHERE ticker IN ({placeholders})"
            params.extend(clean)

    rows = _fetch_rows(
        f"""
        WITH ranked AS (
            SELECT
                e.*,
                ROW_NUMBER() OVER (
                    PARTITION BY e.ric
                    ORDER BY e.as_of_date DESC, e.updated_at DESC
                ) AS rn
            FROM {table} e
            {ticker_clause}
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
        ORDER BY ric ASC
        """,
        params,
    )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def load_latest_fundamentals(
    tickers: list[str] | None = None,
    as_of_date: str | None = None,
) -> pd.DataFrame:
    clean = [t.upper() for t in (tickers or []) if t.strip()]
    as_of = previous_or_same_xnys_session(
        str(as_of_date or datetime.now(timezone.utc).date().isoformat())
    )
    ticker_filter = ""
    params: list[Any] = [as_of]
    if clean:
        placeholders = ",".join("?" for _ in clean)
        ticker_filter = f" AND sm.ticker IN ({placeholders})"
        params.extend(clean)
    params_for_cls = [as_of, *params[1:]]

    missing = _missing_tables(
        "security_master",
        "security_fundamentals_pit",
        "security_classification_pit",
    )
    if missing:
        raise RuntimeError(
            f"Missing required canonical table(s): {', '.join(sorted(missing))}"
        )

    rows = _fetch_rows(
        f"""
        WITH latest_fund_ric AS (
            SELECT f.ric, MAX(f.as_of_date) AS as_of_date
            FROM security_fundamentals_pit f
            JOIN security_master sm
              ON sm.ric = f.ric
            WHERE f.as_of_date <= ?
              {ticker_filter}
            GROUP BY f.ric
        ),
        latest_fund AS (
            SELECT
                f.*,
                ROW_NUMBER() OVER (
                    PARTITION BY f.ric, f.as_of_date
                    ORDER BY f.stat_date DESC, f.updated_at DESC
                ) AS rn
            FROM security_fundamentals_pit f
            JOIN latest_fund_ric lf
              ON lf.ric = f.ric
             AND lf.as_of_date = f.as_of_date
        ),
        latest_cls_ric AS (
            SELECT c.ric, MAX(c.as_of_date) AS as_of_date
            FROM security_classification_pit c
            JOIN security_master sm
              ON sm.ric = c.ric
            WHERE c.as_of_date <= ?
              {ticker_filter}
            GROUP BY c.ric
        ),
        latest_cls AS (
            SELECT
                c.*,
                ROW_NUMBER() OVER (
                    PARTITION BY c.ric, c.as_of_date
                    ORDER BY c.updated_at DESC
                ) AS rn
            FROM security_classification_pit c
            JOIN latest_cls_ric lc
              ON lc.ric = c.ric
             AND lc.as_of_date = c.as_of_date
        )
        SELECT
            UPPER(sm.ric) AS ric,
            UPPER(sm.ticker) AS ticker,
            f.as_of_date AS fetch_date,
            CAST(f.market_cap AS REAL) AS market_cap,
            CAST(f.shares_outstanding AS REAL) AS shares_outstanding,
            CAST(f.dividend_yield AS REAL) AS dividend_yield,
            f.common_name AS common_name,
            CAST(f.book_value_per_share AS REAL) AS book_value,
            CAST(f.forward_eps AS REAL) AS forward_eps,
            CAST(f.trailing_eps AS REAL) AS trailing_eps,
            CAST(f.total_debt AS REAL) AS total_debt,
            CAST(f.cash_and_equivalents AS REAL) AS cash_and_equivalents,
            CAST(f.long_term_debt AS REAL) AS long_term_debt,
            CAST(f.operating_cashflow AS REAL) AS operating_cashflow,
            CAST(f.capital_expenditures AS REAL) AS capital_expenditures,
            CAST(f.revenue AS REAL) AS revenue,
            CAST(f.ebitda AS REAL) AS ebitda,
            CAST(f.ebit AS REAL) AS ebit,
            CAST(f.total_assets AS REAL) AS total_assets,
            CAST(f.roe_pct AS REAL) AS return_on_equity,
            CAST(f.operating_margin_pct AS REAL) AS operating_margins,
            f.period_end_date AS fundamental_period_end_date,
            f.report_currency AS report_currency,
            f.fiscal_year AS fiscal_year,
            f.period_type AS period_type,
            f.source AS source,
            f.job_run_id AS job_run_id,
            f.updated_at AS updated_at,
            lc.trbc_business_sector AS trbc_business_sector,
            lc.trbc_industry_group AS trbc_industry_group,
            COALESCE(
                NULLIF(lc.trbc_economic_sector, ''),
                ''
            ) AS trbc_economic_sector_short,
            lc.trbc_economic_sector AS trbc_economic_sector
        FROM latest_fund f
        JOIN security_master sm
          ON sm.ric = f.ric
        LEFT JOIN latest_cls lc
          ON lc.ric = f.ric
         AND lc.rn = 1
        WHERE f.rn = 1
        ORDER BY sm.ric ASC
        """,
        [*params, *params_for_cls],
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def load_latest_prices(tickers: list[str] | None = None) -> pd.DataFrame:
    clean = [t.upper() for t in (tickers or []) if t.strip()]
    ticker_filter = ""
    params: list[Any] = []
    if clean:
        placeholders = ",".join("?" for _ in clean)
        ticker_filter = f" WHERE sm.ticker IN ({placeholders})"
        params = clean

    missing = _missing_tables("security_master", "security_prices_eod")
    if missing:
        raise RuntimeError(
            f"Missing required canonical table(s): {', '.join(sorted(missing))}"
        )

    rows = _fetch_rows(
        f"""
        WITH latest AS (
            SELECT p.ric, MAX(p.date) AS date
            FROM security_prices_eod p
            JOIN security_master sm
              ON sm.ric = p.ric
            {ticker_filter}
            GROUP BY p.ric
        )
        SELECT UPPER(sm.ric) AS ric, UPPER(sm.ticker) AS ticker, p.date, CAST(p.close AS REAL) AS close
        FROM security_prices_eod p
        JOIN latest l
          ON p.ric = l.ric
         AND p.date = l.date
        JOIN security_master sm
          ON sm.ric = p.ric
        ORDER BY sm.ric ASC
        """,
        params,
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def load_source_dates() -> dict[str, str | None]:
    def _max_val(sql: str) -> str | None:
        rows = _fetch_rows(sql)
        if not rows:
            return None
        val = rows[0].get("latest")
        return str(val) if val else None

    fundamentals_asof = None
    if _table_exists("security_fundamentals_pit"):
        fundamentals_asof = _max_val(
            "SELECT MAX(as_of_date) AS latest FROM security_fundamentals_pit"
        )
    classification_asof = None
    if _table_exists("security_classification_pit"):
        classification_asof = _max_val(
            "SELECT MAX(as_of_date) AS latest FROM security_classification_pit"
        )
    prices_asof = None
    if _table_exists("security_prices_eod"):
        prices_asof = _max_val(
            "SELECT MAX(date) AS latest FROM security_prices_eod"
        )

    return {
        "fundamentals_asof": fundamentals_asof,
        "classification_asof": classification_asof,
        "prices_asof": prices_asof,
        "exposures_asof": _max_val(
            f"SELECT MAX(as_of_date) AS latest FROM {_exposure_source_table_required()}"
        ),
    }
