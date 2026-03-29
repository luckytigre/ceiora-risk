"""Source-domain query helpers for canonical source reads."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from backend.data import source_read_authority
from backend.data.trbc_schema import ensure_trbc_naming
from backend.trading_calendar import previous_or_same_xnys_session
from backend.universe.runtime_rows import load_security_runtime_rows

_LATEST_PRICES_TABLE = "security_prices_latest_cache"
_LATEST_PRICES_META_TABLE = "security_prices_latest_cache_meta"
logger = logging.getLogger(__name__)


def _compat_current_available(
    *,
    missing_tables_fn: Callable[..., list[str]],
    fetch_rows_fn: Callable[[str, list[Any] | None], list[dict[str, Any]]],
) -> bool:
    if missing_tables_fn("security_master_compat_current"):
        return False
    try:
        rows = fetch_rows_fn(
            """
            SELECT 1 AS has_row
            FROM security_master_compat_current
            LIMIT 1
            """,
            None,
        )
    except Exception:
        return False
    return bool(rows)


def _compat_identity_cte_sql(
    *,
    ticker_filter: str,
) -> str:
    compat_ticker_filter = ticker_filter.replace("WHERE ", "AND ", 1).replace("sm.ticker", "UPPER(TRIM(comp.ticker))")
    return f"""
        compat_identity AS (
        SELECT
            UPPER(TRIM(comp.ric)) AS ric,
            UPPER(TRIM(COALESCE(comp.ticker, ''))) AS ticker
        FROM security_master_compat_current comp
        WHERE comp.ric IS NOT NULL
          AND TRIM(comp.ric) <> ''
          {compat_ticker_filter}
        )
    """


def _compat_identity_params(
    *,
    clean: list[str],
) -> list[Any]:
    if not clean:
        return []
    return [*clean]


def _load_runtime_identity_sqlite(
    conn: sqlite3.Connection,
    *,
    tickers: list[str] | None,
    require_price_ingest: bool = False,
    require_pit_fundamentals: bool = False,
) -> pd.DataFrame:
    runtime_rows = load_security_runtime_rows(
        conn,
        tickers=tickers,
        include_disabled=False,
    )
    runtime_df = pd.DataFrame(runtime_rows)
    if runtime_df.empty:
        return pd.DataFrame(columns=["ric", "ticker"])
    runtime_df["ric"] = runtime_df["ric"].astype(str).str.upper()
    runtime_df["ticker"] = runtime_df["ticker"].astype(str).str.upper()
    if require_price_ingest and "price_ingest_enabled" in runtime_df.columns:
        runtime_df = runtime_df[runtime_df["price_ingest_enabled"].astype(int).eq(1)]
    if require_pit_fundamentals:
        if "pit_fundamentals_enabled" in runtime_df.columns:
            runtime_df = runtime_df[runtime_df["pit_fundamentals_enabled"].astype(int).eq(1)]
        if "is_single_name_equity" in runtime_df.columns:
            runtime_df = runtime_df[runtime_df["is_single_name_equity"].astype(int).eq(1)]
    return runtime_df[["ric", "ticker"]].drop_duplicates(subset=["ric"], keep="last")


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
    missing = missing_tables_fn("security_prices_eod")
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

        runtime_identity = _load_runtime_identity_sqlite(
            conn,
            tickers=tickers,
            require_price_ingest=True,
        )
        if runtime_identity.empty:
            return pd.DataFrame()

        prices = pd.read_sql_query(
            f"""
            SELECT
                UPPER(lp.ric) AS ric,
                lp.date,
                CAST(lp.close AS REAL) AS close
            FROM {_LATEST_PRICES_TABLE} lp
            ORDER BY UPPER(lp.ric) ASC
            """,
            conn,
        )
        if prices.empty:
            return prices
        prices["ric"] = prices["ric"].astype(str).str.upper()
        out = prices.merge(runtime_identity, on="ric", how="inner")
        if out.empty:
            return out
        out["ticker"] = out["ticker"].astype(str).str.upper()
        if clean:
            out = out[out["ticker"].isin(clean)].copy()
        return out[["ric", "ticker", "date", "close"]].sort_values(["ric", "date"]).reset_index(drop=True)
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
        "security_fundamentals_pit",
        "security_classification_pit",
    )
    if missing:
        raise RuntimeError(
            f"Missing required canonical table(s): {', '.join(sorted(missing))}"
        )

    if source_read_authority.prefer_runtime_registry(
        missing_tables_fn=missing_tables_fn,
        fetch_rows_fn=fetch_rows_fn,
        tickers=clean,
        require_taxonomy=True,
    ):
        compat_available = not missing_tables_fn("security_master_compat_current")
        compat_join = ""
        compat_equity_expr = "0"
        if compat_available:
            compat_join = """
                LEFT JOIN security_master_compat_current comp
                  ON comp.ric = reg.ric
            """
            compat_equity_expr = "COALESCE(comp.is_equity_eligible, 0)"
        runtime_params: list[Any] = []
        if clean:
            runtime_params.extend(clean)
        runtime_params.extend([as_of, as_of])
        rows = fetch_rows_fn(
            f"""
            WITH runtime_registry AS (
                SELECT
                    UPPER(reg.ric) AS ric,
                    UPPER(TRIM(reg.ticker)) AS ticker
                FROM security_registry reg
                LEFT JOIN security_policy_current pol
                  ON pol.ric = reg.ric
                LEFT JOIN security_taxonomy_current tax
                  ON tax.ric = reg.ric
                {compat_join}
                WHERE COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') <> 'disabled'
                  AND reg.ticker IS NOT NULL
                  AND TRIM(reg.ticker) <> ''
                  AND COALESCE(
                        pol.pit_fundamentals_enabled,
                        CASE
                            WHEN COALESCE(
                                tax.is_single_name_equity,
                                {compat_equity_expr},
                                0
                            ) = 1
                            THEN 1
                            ELSE 0
                        END
                  ) = 1
                  AND COALESCE(
                        tax.is_single_name_equity,
                        {compat_equity_expr},
                        0
                  ) = 1
                  {ticker_filter.replace("sm.ticker", "UPPER(TRIM(reg.ticker))")}
            ),
            latest_fund_ric AS (
                SELECT f.ric, MAX(f.as_of_date) AS as_of_date
                FROM security_fundamentals_pit f
                JOIN runtime_registry rr
                  ON rr.ric = UPPER(f.ric)
                WHERE f.as_of_date <= ?
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
                JOIN runtime_registry rr
                  ON rr.ric = UPPER(c.ric)
                WHERE c.as_of_date <= ?
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
                rr.ric AS ric,
                rr.ticker AS ticker,
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
                COALESCE(NULLIF(lc.trbc_economic_sector, ''), '') AS trbc_economic_sector_short,
                lc.trbc_economic_sector AS trbc_economic_sector
            FROM latest_fund f
            JOIN runtime_registry rr
              ON rr.ric = UPPER(f.ric)
            LEFT JOIN latest_cls lc
              ON lc.ric = f.ric
             AND lc.rn = 1
            WHERE f.rn = 1
            ORDER BY rr.ric ASC
            """,
            runtime_params,
        )
    elif _compat_current_available(missing_tables_fn=missing_tables_fn, fetch_rows_fn=fetch_rows_fn):
        compat_params: list[Any] = [
            *_compat_identity_params(clean=clean),
            as_of,
            as_of,
        ]
        rows = fetch_rows_fn(
            f"""
            WITH {_compat_identity_cte_sql(ticker_filter=ticker_filter)},
            latest_fund_ric AS (
                SELECT f.ric, MAX(f.as_of_date) AS as_of_date
                FROM security_fundamentals_pit f
                JOIN compat_identity comp
                  ON comp.ric = f.ric
                WHERE f.as_of_date <= ?
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
                JOIN compat_identity comp
                  ON comp.ric = c.ric
                WHERE c.as_of_date <= ?
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
                UPPER(comp.ric) AS ric,
                UPPER(comp.ticker) AS ticker,
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
                COALESCE(NULLIF(lc.trbc_economic_sector, ''), '') AS trbc_economic_sector_short,
                lc.trbc_economic_sector AS trbc_economic_sector
            FROM latest_fund f
            JOIN compat_identity comp
              ON comp.ric = f.ric
            LEFT JOIN latest_cls lc
              ON lc.ric = f.ric
             AND lc.rn = 1
            WHERE f.rn = 1
            ORDER BY UPPER(comp.ric) ASC
            """,
            compat_params,
        )
    else:
        raise RuntimeError(
            "Latest fundamentals require registry-first runtime tables or security_master_compat_current."
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

    missing = missing_tables_fn("security_prices_eod")
    if missing:
        raise RuntimeError(
            f"Missing required canonical table(s): {', '.join(sorted(missing))}"
        )

    if source_read_authority.prefer_runtime_registry(
        missing_tables_fn=missing_tables_fn,
        fetch_rows_fn=fetch_rows_fn,
        tickers=clean,
        require_taxonomy=False,
    ):
        rows = fetch_rows_fn(
            f"""
            WITH runtime_registry AS (
                SELECT
                    UPPER(reg.ric) AS ric,
                    UPPER(TRIM(reg.ticker)) AS ticker
                FROM security_registry reg
                LEFT JOIN security_policy_current pol
                  ON pol.ric = reg.ric
                WHERE COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') <> 'disabled'
                  AND reg.ticker IS NOT NULL
                  AND TRIM(reg.ticker) <> ''
                  AND COALESCE(pol.price_ingest_enabled, 1) = 1
                  {ticker_filter.replace("sm.ticker", "UPPER(TRIM(reg.ticker))")}
            ),
            latest AS (
                SELECT p.ric, MAX(p.date) AS date
                FROM security_prices_eod p
                JOIN runtime_registry rr
                  ON rr.ric = UPPER(p.ric)
                GROUP BY p.ric
            )
            SELECT rr.ric AS ric, rr.ticker AS ticker, p.date, CAST(p.close AS REAL) AS close
            FROM security_prices_eod p
            JOIN latest l
              ON p.ric = l.ric
             AND p.date = l.date
            JOIN runtime_registry rr
              ON rr.ric = UPPER(p.ric)
            ORDER BY rr.ric ASC
            """,
            params,
        )
    elif _compat_current_available(missing_tables_fn=missing_tables_fn, fetch_rows_fn=fetch_rows_fn):
        compat_params = _compat_identity_params(clean=clean)
        rows = fetch_rows_fn(
            f"""
            WITH {_compat_identity_cte_sql(ticker_filter=ticker_filter)},
            latest AS (
                SELECT p.ric, MAX(p.date) AS date
                FROM security_prices_eod p
                JOIN compat_identity comp
                  ON comp.ric = p.ric
                GROUP BY p.ric
            )
            SELECT UPPER(comp.ric) AS ric, UPPER(comp.ticker) AS ticker, p.date, CAST(p.close AS REAL) AS close
            FROM security_prices_eod p
            JOIN latest l
              ON p.ric = l.ric
             AND p.date = l.date
            JOIN compat_identity comp
              ON comp.ric = p.ric
            ORDER BY UPPER(comp.ric) ASC
            """,
            compat_params,
        )
    else:
        raise RuntimeError(
            "Latest prices require registry-first runtime tables or security_master_compat_current."
        )
    return pd.DataFrame(rows) if rows else pd.DataFrame()
