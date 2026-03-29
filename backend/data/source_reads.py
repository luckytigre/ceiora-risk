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
from backend.universe.runtime_rows import load_security_runtime_rows

_LATEST_PRICES_TABLE = "security_prices_latest_cache"
_LATEST_PRICES_META_TABLE = "security_prices_latest_cache_meta"
logger = logging.getLogger(__name__)


def _prefer_runtime_registry(
    *,
    missing_tables_fn: Callable[..., list[str]],
    fetch_rows_fn: Callable[[str, list[Any] | None], list[dict[str, Any]]] | None = None,
    tickers: list[str] | None = None,
    require_taxonomy: bool,
) -> bool:
    required_tables = ["security_registry", "security_policy_current"]
    if require_taxonomy:
        required_tables.append("security_taxonomy_current")
    if missing_tables_fn(*required_tables):
        return False
    if fetch_rows_fn is None:
        return False
    clean = [t.upper() for t in (tickers or []) if t.strip()]
    ticker_filter = ""
    params: list[Any] | None = None
    if clean:
        placeholders = ",".join("?" for _ in clean)
        ticker_filter = f" AND UPPER(TRIM(COALESCE(reg.ticker, ''))) IN ({placeholders})"
        params = list(clean)
    taxonomy_join = ""
    missing_companion_expr = "pol.ric IS NULL"
    if require_taxonomy:
        taxonomy_join = """
        LEFT JOIN security_taxonomy_current tax
          ON UPPER(TRIM(tax.ric)) = UPPER(TRIM(reg.ric))
        """
        missing_companion_expr = "pol.ric IS NULL OR tax.ric IS NULL"
    try:
        rows = fetch_rows_fn(
            f"""
            SELECT
                COUNT(*) AS registry_row_count,
                SUM(
                    CASE
                        WHEN COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') = 'active'
                        THEN 1
                        ELSE 0
                    END
                ) AS active_registry_row_count,
                SUM(
                    CASE
                        WHEN COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') = 'active'
                         AND ({missing_companion_expr})
                        THEN 1
                        ELSE 0
                    END
                ) AS active_missing_companion_count
            FROM security_registry reg
            LEFT JOIN security_policy_current pol
              ON UPPER(TRIM(pol.ric)) = UPPER(TRIM(reg.ric))
            {taxonomy_join}
            WHERE reg.ric IS NOT NULL
              AND TRIM(reg.ric) <> ''
              {ticker_filter}
            """,
            params,
        )
    except Exception:
        return False
    if not rows:
        return False
    row = rows[0]
    registry_row_count = int(row.get("registry_row_count") or 0)
    if registry_row_count == 0:
        return False
    return True


def _registry_table_available(
    *,
    missing_tables_fn: Callable[..., list[str]],
    fetch_rows_fn: Callable[[str, list[Any] | None], list[dict[str, Any]]] | None = None,
) -> bool:
    if missing_tables_fn("security_registry"):
        return False
    if fetch_rows_fn is None:
        return True
    try:
        rows = fetch_rows_fn(
            """
            SELECT COUNT(*) AS registry_row_count
            FROM security_registry
            WHERE ric IS NOT NULL
              AND TRIM(ric) <> ''
            """,
            None,
        )
    except Exception:
        return False
    if not rows:
        return False
    return int(rows[0].get("registry_row_count") or 0) > 0


def _table_exists_sqlite(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type IN ('table', 'view') AND name=?
        LIMIT 1
        """,
        (table,),
    ).fetchone()
    return row is not None


def _table_has_rows_sqlite(conn: sqlite3.Connection, table: str) -> bool:
    if not _table_exists_sqlite(conn, table):
        return False
    row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return bool(row and int(row[0] or 0) > 0)


def _load_legacy_identity_sqlite(
    conn: sqlite3.Connection,
    *,
    tickers: list[str] | None,
) -> pd.DataFrame:
    if not _table_exists_sqlite(conn, "security_master"):
        return pd.DataFrame(columns=["ric", "ticker"])
    clean = [t.upper() for t in (tickers or []) if t.strip()]
    where = ""
    params: list[Any] = []
    if clean:
        placeholders = ",".join("?" for _ in clean)
        where = f" AND UPPER(TRIM(COALESCE(ticker, ''))) IN ({placeholders})"
        params.extend(clean)
    identity = pd.read_sql_query(
        f"""
        SELECT
            UPPER(TRIM(ric)) AS ric,
            UPPER(TRIM(COALESCE(ticker, ''))) AS ticker
        FROM security_master
        WHERE ric IS NOT NULL
          AND TRIM(ric) <> ''
          AND ticker IS NOT NULL
          AND TRIM(ticker) <> ''
          {where}
        ORDER BY UPPER(TRIM(ric)) ASC
        """,
        conn,
        params=params,
    )
    if identity.empty:
        return pd.DataFrame(columns=["ric", "ticker"])
    return identity[["ric", "ticker"]].drop_duplicates(subset=["ric"], keep="last")


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


def _requested_price_duplicate_registry_ticker_exists(
    *,
    tickers: list[str],
    fetch_rows_fn: Callable[[str, list[Any] | None], list[dict[str, Any]]],
) -> bool:
    clean = [t.upper() for t in tickers if t.strip()]
    if not clean:
        return False
    placeholders = ",".join("?" for _ in clean)
    rows = fetch_rows_fn(
        f"""
        SELECT UPPER(TRIM(reg.ticker)) AS ticker
        FROM security_registry reg
        LEFT JOIN security_policy_current pol
          ON pol.ric = reg.ric
        WHERE reg.ticker IS NOT NULL
          AND TRIM(reg.ticker) <> ''
          AND UPPER(TRIM(reg.ticker)) IN ({placeholders})
        GROUP BY UPPER(TRIM(reg.ticker))
        HAVING COUNT(*) > 1
           AND SUM(
                CASE
                    WHEN COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') <> 'disabled'
                    THEN 1
                    ELSE 0
                END
           ) > 0
           AND MAX(CASE WHEN COALESCE(pol.price_ingest_enabled, 0) = 1 THEN 1 ELSE 0 END) = 1
        """,
        clean,
    )
    return bool(rows)


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
        if runtime_identity.empty and not _table_has_rows_sqlite(conn, "security_registry"):
            runtime_identity = _load_legacy_identity_sqlite(
                conn,
                tickers=tickers,
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
    if clean:
        placeholders = ",".join("?" for _ in clean)
        ticker_filter = f" AND sm.ticker IN ({placeholders})"

    missing = missing_tables_fn(
        "security_fundamentals_pit",
        "security_classification_pit",
    )
    if missing:
        raise RuntimeError(
            f"Missing required canonical table(s): {', '.join(sorted(missing))}"
        )

    rows_df = pd.DataFrame()
    if _prefer_runtime_registry(
        missing_tables_fn=missing_tables_fn,
        fetch_rows_fn=fetch_rows_fn,
        tickers=clean,
        require_taxonomy=True,
    ):
        taxonomy_equity_expr = (
            "CASE "
            "WHEN COALESCE(tax.classification_ready, 0) = 1 "
            "AND COALESCE(tax.is_single_name_equity, 0) = 1 THEN 1 "
            "ELSE 0 END"
        )
        runtime_identity_params: list[Any] = []
        if clean:
            runtime_identity_params.extend(clean)
        fundamentals_rows = fetch_rows_fn(
            f"""
            WITH runtime_registry AS (
                SELECT
                    reg.ric AS ric,
                    UPPER(TRIM(reg.ticker)) AS ticker
                FROM security_registry reg
                LEFT JOIN security_policy_current pol
                  ON pol.ric = reg.ric
                LEFT JOIN security_taxonomy_current tax
                  ON tax.ric = reg.ric
                WHERE COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') <> 'disabled'
                  AND reg.ticker IS NOT NULL
                  AND TRIM(reg.ticker) <> ''
                  AND pol.ric IS NOT NULL
                  AND tax.ric IS NOT NULL
                  AND COALESCE(
                        pol.pit_fundamentals_enabled,
                        CASE
                            WHEN {taxonomy_equity_expr} = 1
                            THEN 1
                            ELSE 0
                        END
                  ) = 1
                  AND {taxonomy_equity_expr} = 1
                  {ticker_filter.replace("sm.ticker", "UPPER(TRIM(reg.ticker))")}
            ),
            latest_fund AS (
                SELECT
                    f.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY f.ric
                        ORDER BY f.as_of_date DESC, f.stat_date DESC, f.updated_at DESC
                    ) AS rn
                FROM security_fundamentals_pit f
                JOIN runtime_registry rr
                  ON rr.ric = f.ric
                WHERE f.as_of_date <= ?
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
                f.updated_at AS updated_at
            FROM latest_fund f
            JOIN runtime_registry rr
              ON rr.ric = f.ric
            WHERE f.rn = 1
            ORDER BY rr.ric ASC
            """,
            [*runtime_identity_params, as_of],
        )
        if not fundamentals_rows:
            return pd.DataFrame()
        classification_rows = fetch_rows_fn(
            f"""
            WITH runtime_registry AS (
                SELECT
                    reg.ric AS ric
                FROM security_registry reg
                LEFT JOIN security_policy_current pol
                  ON pol.ric = reg.ric
                LEFT JOIN security_taxonomy_current tax
                  ON tax.ric = reg.ric
                WHERE COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') <> 'disabled'
                  AND reg.ticker IS NOT NULL
                  AND TRIM(reg.ticker) <> ''
                  AND pol.ric IS NOT NULL
                  AND tax.ric IS NOT NULL
                  AND COALESCE(
                        pol.pit_fundamentals_enabled,
                        CASE
                            WHEN {taxonomy_equity_expr} = 1
                            THEN 1
                            ELSE 0
                        END
                  ) = 1
                  AND {taxonomy_equity_expr} = 1
                  {ticker_filter.replace("sm.ticker", "UPPER(TRIM(reg.ticker))")}
            ),
            latest_cls AS (
                SELECT
                    c.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY c.ric
                        ORDER BY c.as_of_date DESC, c.updated_at DESC
                    ) AS rn
                FROM security_classification_pit c
                JOIN runtime_registry rr
                  ON rr.ric = c.ric
                WHERE c.as_of_date <= ?
            )
            SELECT
                ric,
                trbc_business_sector,
                trbc_industry_group,
                COALESCE(NULLIF(trbc_economic_sector, ''), '') AS trbc_economic_sector_short,
                trbc_economic_sector AS trbc_economic_sector
            FROM latest_cls
            WHERE rn = 1
            ORDER BY ric ASC
            """,
            [*runtime_identity_params, as_of],
        )
        rows_df = pd.DataFrame(fundamentals_rows)
        if classification_rows:
            rows_df = rows_df.merge(
                pd.DataFrame(classification_rows),
                on="ric",
                how="left",
            )
        else:
            rows_df["trbc_business_sector"] = None
            rows_df["trbc_industry_group"] = None
            rows_df["trbc_economic_sector_short"] = ""
            rows_df["trbc_economic_sector"] = None
        rows_df["trbc_business_sector"] = rows_df.get("trbc_business_sector").where(
            rows_df.get("trbc_business_sector").notna(),
            None,
        )
        rows_df["trbc_industry_group"] = rows_df.get("trbc_industry_group").where(
            rows_df.get("trbc_industry_group").notna(),
            None,
        )
        rows_df["trbc_economic_sector"] = rows_df.get("trbc_economic_sector").where(
            rows_df.get("trbc_economic_sector").notna(),
            None,
        )
        rows_df["trbc_economic_sector_short"] = rows_df.get("trbc_economic_sector_short").fillna("")
        return rows_df
    if _registry_table_available(
        missing_tables_fn=missing_tables_fn,
        fetch_rows_fn=fetch_rows_fn,
    ):
        return pd.DataFrame()
    raise RuntimeError(
        "Latest fundamentals require registry-first runtime tables."
    )


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
        ticker_filter = f" AND sm.ticker IN ({placeholders})"
        params = clean

    missing = missing_tables_fn("security_prices_eod")
    if missing:
        raise RuntimeError(
            f"Missing required canonical table(s): {', '.join(sorted(missing))}"
        )

    rows: list[dict[str, Any]] = []
    if _prefer_runtime_registry(
        missing_tables_fn=missing_tables_fn,
        fetch_rows_fn=fetch_rows_fn,
        tickers=clean,
        require_taxonomy=False,
    ):
        rows = fetch_rows_fn(
            f"""
            WITH runtime_registry AS (
                SELECT
                    reg.ric AS ric,
                    UPPER(TRIM(reg.ticker)) AS ticker
                FROM security_registry reg
                LEFT JOIN security_policy_current pol
                  ON pol.ric = reg.ric
                WHERE COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') <> 'disabled'
                  AND reg.ticker IS NOT NULL
                  AND TRIM(reg.ticker) <> ''
                  AND pol.ric IS NOT NULL
                  AND COALESCE(pol.price_ingest_enabled, 0) = 1
                  {ticker_filter.replace("sm.ticker", "UPPER(TRIM(reg.ticker))")}
            ),
            latest AS (
                SELECT
                    p.ric,
                    p.date,
                    p.close,
                    ROW_NUMBER() OVER (
                        PARTITION BY p.ric
                        ORDER BY p.date DESC
                    ) AS rn
                FROM security_prices_eod p
                JOIN runtime_registry rr
                  ON rr.ric = p.ric
            )
            SELECT rr.ric AS ric, rr.ticker AS ticker, p.date, CAST(p.close AS REAL) AS close
            FROM latest p
            JOIN runtime_registry rr
              ON rr.ric = p.ric
            WHERE p.rn = 1
            ORDER BY rr.ric ASC
            """,
            params,
        )
        if not rows and clean and _requested_price_duplicate_registry_ticker_exists(
            tickers=clean,
            fetch_rows_fn=fetch_rows_fn,
        ):
            rows = fetch_rows_fn(
                f"""
                WITH runtime_registry AS (
                    SELECT
                        reg.ric AS ric,
                        UPPER(TRIM(reg.ticker)) AS ticker
                    FROM security_registry reg
                    LEFT JOIN security_policy_current pol
                      ON pol.ric = reg.ric
                    LEFT JOIN security_master_compat_current compat
                      ON compat.ric = reg.ric
                    LEFT JOIN (
                        SELECT
                            UPPER(TRIM(reg2.ticker)) AS ticker,
                            MAX(CASE WHEN COALESCE(pol2.price_ingest_enabled, 0) = 1 THEN 1 ELSE 0 END) AS ticker_price_ingest_enabled
                        FROM security_registry reg2
                        LEFT JOIN security_policy_current pol2
                          ON pol2.ric = reg2.ric
                        WHERE reg2.ticker IS NOT NULL
                          AND TRIM(reg2.ticker) <> ''
                        GROUP BY UPPER(TRIM(reg2.ticker))
                    ) ticker_policy
                      ON ticker_policy.ticker = UPPER(TRIM(reg.ticker))
                    WHERE COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') <> 'disabled'
                      AND reg.ticker IS NOT NULL
                      AND TRIM(reg.ticker) <> ''
                      AND UPPER(TRIM(reg.ticker)) IN ({",".join("?" for _ in clean)})
                      AND compat.ric IS NOT NULL
                      AND COALESCE(ticker_policy.ticker_price_ingest_enabled, 0) = 1
                ),
                latest AS (
                    SELECT
                        p.ric,
                        p.date,
                        p.close,
                        ROW_NUMBER() OVER (
                            PARTITION BY p.ric
                            ORDER BY p.date DESC
                        ) AS rn
                    FROM security_prices_eod p
                    JOIN runtime_registry rr
                      ON rr.ric = p.ric
                )
                SELECT rr.ric AS ric, rr.ticker AS ticker, p.date, CAST(p.close AS REAL) AS close
                FROM latest p
                JOIN runtime_registry rr
                  ON rr.ric = p.ric
                WHERE p.rn = 1
                ORDER BY rr.ric ASC
                """,
                params,
            )
    elif _registry_table_available(
        missing_tables_fn=missing_tables_fn,
        fetch_rows_fn=fetch_rows_fn,
    ):
        return pd.DataFrame()
    else:
        raise RuntimeError(
            "Latest prices require registry-first runtime tables."
        )
    return pd.DataFrame(rows) if rows else pd.DataFrame()
