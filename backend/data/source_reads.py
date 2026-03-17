"""Source-domain query helpers for canonical source reads."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from backend.data.trbc_schema import ensure_trbc_naming
from backend.trading_calendar import previous_or_same_xnys_session

_LATEST_PRICES_TABLE = "security_prices_latest_cache"
_LATEST_PRICES_META_TABLE = "security_prices_latest_cache_meta"
logger = logging.getLogger(__name__)


def ensure_latest_prices_cache_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_LATEST_PRICES_TABLE} (
            ric TEXT PRIMARY KEY,
            date TEXT NOT NULL,
            close REAL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_LATEST_PRICES_META_TABLE} (
            cache_key TEXT PRIMARY KEY,
            cache_value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{_LATEST_PRICES_TABLE}_date ON {_LATEST_PRICES_TABLE}(date)"
    )


def latest_prices_signature(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        """
        SELECT MAX(date), COUNT(DISTINCT ric)
        FROM security_prices_eod
        """
    ).fetchone()
    latest_date = str(row[0] or "")
    ric_count = int(row[1] or 0)
    return f"{latest_date}|{ric_count}"


def latest_prices_cache_signature(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        f"""
        SELECT cache_value
        FROM {_LATEST_PRICES_META_TABLE}
        WHERE cache_key = 'source_signature'
        LIMIT 1
        """
    ).fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0])


def refresh_latest_prices_cache(conn: sqlite3.Connection) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    source_signature = latest_prices_signature(conn)
    conn.execute(f"DELETE FROM {_LATEST_PRICES_TABLE}")
    conn.execute(
        f"""
        INSERT INTO {_LATEST_PRICES_TABLE} (ric, date, close, updated_at)
        SELECT p.ric, p.date, CAST(p.close AS REAL), ?
        FROM security_prices_eod p
        JOIN (
            SELECT ric, MAX(date) AS date
            FROM security_prices_eod
            GROUP BY ric
        ) latest
          ON latest.ric = p.ric
         AND latest.date = p.date
        """,
        (now_iso,),
    )
    conn.execute(f"DELETE FROM {_LATEST_PRICES_META_TABLE} WHERE cache_key = 'source_signature'")
    conn.execute(
        f"""
        INSERT INTO {_LATEST_PRICES_META_TABLE} (cache_key, cache_value, updated_at)
        VALUES ('source_signature', ?, ?)
        """,
        (source_signature, now_iso),
    )


def load_latest_prices_sqlite(
    *,
    data_db: Path,
    tickers: list[str] | None,
    missing_tables_fn: Callable[..., list[str]],
) -> pd.DataFrame:
    missing = missing_tables_fn("security_master", "security_prices_eod")
    if missing:
        raise RuntimeError(
            f"Missing required canonical table(s): {', '.join(sorted(missing))}"
        )
    clean = [t.upper() for t in (tickers or []) if t.strip()]
    conn = sqlite3.connect(str(data_db))
    conn.row_factory = sqlite3.Row
    try:
        ensure_trbc_naming(conn)
        ensure_latest_prices_cache_schema(conn)
        if latest_prices_cache_signature(conn) != latest_prices_signature(conn):
            refresh_latest_prices_cache(conn)
            conn.commit()

        ticker_filter = ""
        params: list[Any] = []
        if clean:
            placeholders = ",".join("?" for _ in clean)
            ticker_filter = f" WHERE UPPER(sm.ticker) IN ({placeholders})"
            params = clean

        rows = conn.execute(
            f"""
            SELECT
                UPPER(sm.ric) AS ric,
                UPPER(sm.ticker) AS ticker,
                lp.date,
                CAST(lp.close AS REAL) AS close
            FROM {_LATEST_PRICES_TABLE} lp
            JOIN security_master sm
              ON sm.ric = lp.ric
            {ticker_filter}
            ORDER BY sm.ric ASC
            """,
            params,
        ).fetchall()
        return pd.DataFrame([dict(row) for row in rows]) if rows else pd.DataFrame()
    finally:
        conn.close()


def exposure_source_table_required(*, table_exists_fn: Callable[[str], bool]) -> str:
    table = "barra_raw_cross_section_history"
    if not table_exists_fn(table):
        raise RuntimeError(
            "Required exposure table missing: barra_raw_cross_section_history. "
            "Build it via backend/scripts/build_barra_raw_cross_section_history.py."
        )
    return table


def resolve_latest_well_covered_exposure_asof(
    table: str,
    *,
    fetch_rows_fn: Callable[[str, list[Any] | None], list[dict[str, Any]]],
) -> str | None:
    rows = fetch_rows_fn(
        f"""
        SELECT as_of_date, COUNT(*) AS row_count
        FROM {table}
        GROUP BY as_of_date
        ORDER BY as_of_date DESC
        """,
        None,
    )
    if not rows:
        return None

    latest_raw = str(rows[0].get("as_of_date") or "")
    max_count = max(int(row.get("row_count") or 0) for row in rows)
    min_coverage = max(100, int(0.50 * max_count))
    well_covered_dates = sorted(
        str(row.get("as_of_date") or "")
        for row in rows
        if int(row.get("row_count") or 0) >= min_coverage
    )
    selected = well_covered_dates[-1] if well_covered_dates else latest_raw
    if selected and selected != latest_raw:
        logger.warning(
            "Using well-covered exposure as-of date %s instead of sparse latest %s "
            "(coverage threshold=%s, max_count=%s)",
            selected,
            latest_raw,
            min_coverage,
            max_count,
        )
    return selected or None


def load_raw_cross_section_latest(
    *,
    tickers: list[str] | None,
    fetch_rows_fn: Callable[[str, list[Any] | None], list[dict[str, Any]]],
    exposure_source_table_required_fn: Callable[[], str],
    resolve_latest_well_covered_exposure_asof_fn: Callable[[str], str | None],
) -> pd.DataFrame:
    table = exposure_source_table_required_fn()
    selected_asof = resolve_latest_well_covered_exposure_asof_fn(table)
    if selected_asof is None:
        return pd.DataFrame()

    params: list[Any] = [selected_asof]
    ticker_clause = ""
    if tickers:
        clean = [t.upper() for t in tickers if t.strip()]
        if clean:
            placeholders = ",".join("?" for _ in clean)
            ticker_clause = f" AND UPPER(e.ticker) IN ({placeholders})"
            params.extend(clean)

    rows = fetch_rows_fn(
        f"""
        WITH ranked AS (
            SELECT
                e.*,
                ROW_NUMBER() OVER (
                    PARTITION BY e.ric
                    ORDER BY e.updated_at DESC
                ) AS rn
            FROM {table} e
            WHERE e.as_of_date = ?
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
    *,
    tickers: list[str] | None,
    as_of_date: str | None,
    fetch_rows_fn: Callable[[str, list[Any] | None], list[dict[str, Any]]],
    missing_tables_fn: Callable[..., list[str]],
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

    missing = missing_tables_fn(
        "security_master",
        "security_fundamentals_pit",
        "security_classification_pit",
    )
    if missing:
        raise RuntimeError(
            f"Missing required canonical table(s): {', '.join(sorted(missing))}"
        )

    rows = fetch_rows_fn(
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


def load_latest_prices(
    *,
    tickers: list[str] | None,
    fetch_rows_fn: Callable[[str, list[Any] | None], list[dict[str, Any]]],
    missing_tables_fn: Callable[..., list[str]],
) -> pd.DataFrame:
    clean = [t.upper() for t in (tickers or []) if t.strip()]
    ticker_filter = ""
    params: list[Any] = []
    if clean:
        placeholders = ",".join("?" for _ in clean)
        ticker_filter = f" WHERE sm.ticker IN ({placeholders})"
        params = clean

    missing = missing_tables_fn("security_master", "security_prices_eod")
    if missing:
        raise RuntimeError(
            f"Missing required canonical table(s): {', '.join(sorted(missing))}"
        )

    rows = fetch_rows_fn(
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
