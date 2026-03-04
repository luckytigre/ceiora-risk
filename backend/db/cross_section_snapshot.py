"""Canonical cross-section snapshot builder keyed by (ticker, as_of_date)."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from db.trbc_schema import ensure_trbc_naming

TABLE = "universe_cross_section_snapshot"


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


def _table_column_list(conn: sqlite3.Connection, table: str) -> list[str]:
    if not _table_exists(conn, table):
        return []
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(r[1]) for r in rows]


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cols = _table_columns(conn, table)
    if column in cols:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def _create_cross_section_snapshot_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            ticker TEXT NOT NULL,
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
            PRIMARY KEY (ticker, as_of_date)
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_asof ON {TABLE}(as_of_date)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_ticker ON {TABLE}(ticker)")


def _migrate_drop_price_exchange(conn: sqlite3.Connection) -> None:
    if "price_exchange" not in _table_columns(conn, TABLE):
        return
    legacy = f"{TABLE}__legacy_pre_no_price_exchange"
    conn.execute(f"DROP TABLE IF EXISTS {legacy}")
    conn.execute(f"ALTER TABLE {TABLE} RENAME TO {legacy}")
    _create_cross_section_snapshot_table(conn)
    new_cols = _table_column_list(conn, TABLE)
    legacy_cols = set(_table_column_list(conn, legacy))
    keep_cols = [c for c in new_cols if c in legacy_cols]
    if keep_cols:
        cols_csv = ", ".join(keep_cols)
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {TABLE} ({cols_csv})
            SELECT {cols_csv}
            FROM {legacy}
            """
        )
    conn.execute(f"DROP TABLE IF EXISTS {legacy}")


def ensure_cross_section_snapshot_table(conn: sqlite3.Connection) -> None:
    _create_cross_section_snapshot_table(conn)
    for col, ddl in [
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
        _ensure_column(conn, TABLE, col, ddl)
    _migrate_drop_price_exchange(conn)


def _load_base_cross_sections(
    conn: sqlite3.Connection,
    *,
    start_date: str | None,
    end_date: str | None,
    tickers: list[str] | None,
    mode: str,
) -> pd.DataFrame:
    source_table = "barra_raw_cross_section_history"
    source_cols = _table_columns(conn, source_table)
    if "ticker" not in source_cols or "as_of_date" not in source_cols:
        return pd.DataFrame(columns=["ticker", "as_of_date"])

    clauses: list[str] = []
    params: list[Any] = []
    if start_date:
        clauses.append("e.as_of_date >= ?")
        params.append(str(start_date))
    if end_date:
        clauses.append("e.as_of_date <= ?")
        params.append(str(end_date))
    if tickers:
        clean = [str(t).upper().strip() for t in tickers if str(t).strip()]
        if clean:
            placeholders = ",".join("?" for _ in clean)
            clauses.append(f"UPPER(e.ticker) IN ({placeholders})")
            params.extend(clean)

    clauses.extend(
        [
            "UPPER(sm.ticker) = UPPER(e.ticker)",
            "COALESCE(sm.classification_ok, 0) = 1",
            "COALESCE(sm.is_equity_eligible, 0) = 1",
        ]
    )
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    if str(mode).strip().lower() == "full":
        df = pd.read_sql_query(
            f"""
            SELECT DISTINCT UPPER(e.ticker) AS ticker, e.as_of_date
            FROM {source_table} e
            JOIN security_master sm
              ON UPPER(sm.ticker) = UPPER(e.ticker)
            {where_sql}
            ORDER BY UPPER(e.ticker), e.as_of_date
            """,
            conn,
            params=params,
        )
    else:
        df = pd.read_sql_query(
            f"""
            WITH ranked AS (
                SELECT
                    UPPER(e.ticker) AS ticker,
                    e.as_of_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(e.ticker)
                        ORDER BY e.as_of_date DESC
                    ) AS rn
                FROM {source_table} e
                JOIN security_master sm
                  ON UPPER(sm.ticker) = UPPER(e.ticker)
                {where_sql}
            )
            SELECT ticker, as_of_date
            FROM ranked
            WHERE rn = 1
            ORDER BY ticker, as_of_date
            """,
            conn,
            params=params,
        )
    if df.empty:
        return df
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["as_of_date"] = df["as_of_date"].astype(str)
    return df


def _merge_asof_by_ticker(
    base: pd.DataFrame,
    events: pd.DataFrame,
    *,
    left_date_col: str,
    right_date_col: str,
) -> pd.DataFrame:
    if events.empty:
        return base
    merged_parts: list[pd.DataFrame] = []
    events_by_ticker = {str(t): grp.copy() for t, grp in events.groupby("ticker", sort=False)}
    for ticker, left_grp in base.groupby("ticker", sort=False):
        left_sorted = left_grp.sort_values(left_date_col).reset_index(drop=True)
        right_grp = events_by_ticker.get(str(ticker))
        if right_grp is None or right_grp.empty:
            merged_parts.append(left_sorted)
            continue
        right_sorted = right_grp.sort_values(right_date_col).reset_index(drop=True)
        right_sorted = right_sorted.drop(columns=["ticker"], errors="ignore")
        out = pd.merge_asof(
            left_sorted,
            right_sorted,
            left_on=left_date_col,
            right_on=right_date_col,
            direction="backward",
            allow_exact_matches=True,
        )
        merged_parts.append(out)
    if not merged_parts:
        return base
    return pd.concat(merged_parts, ignore_index=True)


def _sanitize_num(df: pd.DataFrame, cols: list[str]) -> None:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")


def _coalesce_columns(df: pd.DataFrame, target: str, candidates: list[str]) -> None:
    vals = None
    for col in candidates:
        if col not in df.columns:
            continue
        cur = df[col]
        vals = cur if vals is None else vals.combine_first(cur)
    if vals is None:
        if target not in df.columns:
            df[target] = None
        return
    df[target] = vals


def rebuild_cross_section_snapshot(
    data_db: Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    tickers: list[str] | None = None,
    mode: str = "current",
) -> dict[str, Any]:
    conn = sqlite3.connect(str(data_db))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        mode_norm = str(mode or "current").strip().lower()
        if mode_norm not in {"current", "full"}:
            mode_norm = "current"

        ensure_trbc_naming(conn)
        ensure_cross_section_snapshot_table(conn)

        base = _load_base_cross_sections(
            conn,
            start_date=start_date,
            end_date=end_date,
            tickers=tickers,
            mode=mode_norm,
        )
        if base.empty:
            return {"status": "no-op", "rows_upserted": 0, "table": TABLE}

        base["as_of_date_dt"] = pd.to_datetime(base["as_of_date"], errors="coerce")
        base = base.dropna(subset=["as_of_date_dt"])
        if base.empty:
            return {"status": "no-op", "rows_upserted": 0, "table": TABLE}

        max_asof = str(base["as_of_date"].max())
        base_tickers = sorted(set(base["ticker"].astype(str).str.upper().tolist()))
        ticker_clause = ""
        ticker_params: list[Any] = []
        if base_tickers:
            placeholders = ",".join("?" for _ in base_tickers)
            ticker_clause = f" AND UPPER(sm.ticker) IN ({placeholders})"
            ticker_params = base_tickers

        # Fundamental events (canonical SID-keyed PIT source).
        fundamental_cols = [
            "market_cap",
            "shares_outstanding",
            "dividend_yield",
            "common_name",
            "book_value",
            "forward_eps",
            "trailing_eps",
            "total_debt",
            "cash_and_equivalents",
            "long_term_debt",
            "free_cash_flow",
            "gross_profit",
            "net_income",
            "operating_cashflow",
            "capital_expenditures",
            "shares_basic",
            "shares_diluted",
            "free_float_shares",
            "free_float_percent",
            "revenue",
            "ebitda",
            "ebit",
            "total_assets",
            "total_liabilities",
            "return_on_equity",
            "operating_margins",
            "fundamental_period_end_date",
            "report_currency",
            "fiscal_year",
            "period_type",
        ]
        fsel = ", ".join(
            [
                "UPPER(sm.ticker) AS ticker",
                "f.as_of_date AS fetch_date",
                "CAST(f.market_cap AS REAL) AS market_cap",
                "CAST(f.shares_outstanding AS REAL) AS shares_outstanding",
                "CAST(f.dividend_yield AS REAL) AS dividend_yield",
                "f.common_name AS common_name",
                "CAST(f.book_value_per_share AS REAL) AS book_value",
                "CAST(f.forward_eps AS REAL) AS forward_eps",
                "CAST(f.trailing_eps AS REAL) AS trailing_eps",
                "CAST(f.total_debt AS REAL) AS total_debt",
                "CAST(f.cash_and_equivalents AS REAL) AS cash_and_equivalents",
                "CAST(f.long_term_debt AS REAL) AS long_term_debt",
                "NULL AS free_cash_flow",
                "NULL AS gross_profit",
                "NULL AS net_income",
                "CAST(f.operating_cashflow AS REAL) AS operating_cashflow",
                "CAST(f.capital_expenditures AS REAL) AS capital_expenditures",
                "NULL AS shares_basic",
                "NULL AS shares_diluted",
                "NULL AS free_float_shares",
                "NULL AS free_float_percent",
                "CAST(f.revenue AS REAL) AS revenue",
                "CAST(f.ebitda AS REAL) AS ebitda",
                "CAST(f.ebit AS REAL) AS ebit",
                "CAST(f.total_assets AS REAL) AS total_assets",
                "NULL AS total_liabilities",
                "CAST(f.roe_pct AS REAL) AS return_on_equity",
                "CAST(f.operating_margin_pct AS REAL) AS operating_margins",
                "f.period_end_date AS fundamental_period_end_date",
                "f.report_currency AS report_currency",
                "f.fiscal_year AS fiscal_year",
                "f.period_type AS period_type",
                "f.source AS source",
                "f.job_run_id AS job_run_id",
                "f.updated_at AS updated_at",
            ]
        )
        if mode_norm == "full":
            fundamentals = pd.read_sql_query(
                f"""
                SELECT {fsel}
                FROM security_fundamentals_pit f
                JOIN security_master sm
                  ON sm.ric = f.ric
                WHERE f.as_of_date <= ?
                  {ticker_clause}
                ORDER BY UPPER(sm.ticker), f.as_of_date, f.updated_at
                """,
                conn,
                params=[max_asof, *ticker_params],
            )
        else:
            fundamentals = pd.read_sql_query(
                f"""
                WITH latest_date AS (
                    SELECT ric, MAX(as_of_date) AS max_as_of_date
                    FROM security_fundamentals_pit
                    WHERE as_of_date <= ?
                    GROUP BY ric
                ),
                ranked AS (
                    SELECT
                        {fsel},
                        ROW_NUMBER() OVER (
                            PARTITION BY UPPER(sm.ticker)
                            ORDER BY f.stat_date DESC, f.updated_at DESC
                        ) AS rn
                    FROM security_fundamentals_pit f
                    JOIN latest_date ld
                      ON ld.ric = f.ric
                     AND ld.max_as_of_date = f.as_of_date
                    JOIN security_master sm
                      ON sm.ric = f.ric
                    WHERE 1=1
                      {ticker_clause}
                )
                SELECT *
                FROM ranked
                WHERE rn = 1
                ORDER BY ticker ASC
                """,
                conn,
                params=[max_asof, *ticker_params],
            )
        if not fundamentals.empty:
            fundamentals["ticker"] = fundamentals["ticker"].astype(str).str.upper()
            fundamentals["fetch_date_dt"] = pd.to_datetime(fundamentals["fetch_date"], errors="coerce")
            fundamentals = fundamentals.dropna(subset=["fetch_date_dt"])
            fundamentals = (
                fundamentals.sort_values(["ticker", "fetch_date", "updated_at"])
                .drop_duplicates(subset=["ticker", "fetch_date"], keep="last")
            )
            fundamentals = fundamentals.rename(
                columns={
                    "source": "fundamental_source",
                    "job_run_id": "fundamental_job_run_id",
                }
            )

        # TRBC point-in-time events from canonical PIT source.
        trbc_cols = [
            "trbc_economic_sector",
            "trbc_business_sector",
            "trbc_industry_group",
            "trbc_industry",
            "trbc_activity",
        ]
        hsel = ", ".join(
            [
                "UPPER(sm.ticker) AS ticker",
                "h.as_of_date",
                *[f"h.{c}" for c in trbc_cols],
                "h.source AS source",
                "h.job_run_id AS job_run_id",
                "h.updated_at AS updated_at",
            ]
        )
        trbc_hist = pd.DataFrame()
        if _table_exists(conn, "security_classification_pit"):
            if mode_norm == "full":
                trbc_hist = pd.read_sql_query(
                    f"""
                    SELECT {hsel}
                    FROM security_classification_pit h
                    JOIN security_master sm
                      ON sm.ric = h.ric
                    WHERE h.as_of_date <= ?
                      {ticker_clause}
                    ORDER BY UPPER(sm.ticker), h.as_of_date, h.updated_at
                    """,
                    conn,
                    params=[max_asof, *ticker_params],
                )
            else:
                trbc_hist = pd.read_sql_query(
                    f"""
                    WITH latest_date AS (
                        SELECT ric, MAX(as_of_date) AS max_as_of_date
                        FROM security_classification_pit
                        WHERE as_of_date <= ?
                        GROUP BY ric
                    ),
                    ranked AS (
                        SELECT
                            {hsel},
                            ROW_NUMBER() OVER (
                                PARTITION BY UPPER(sm.ticker)
                                ORDER BY h.updated_at DESC
                            ) AS rn
                        FROM security_classification_pit h
                        JOIN latest_date ld
                          ON ld.ric = h.ric
                         AND ld.max_as_of_date = h.as_of_date
                        JOIN security_master sm
                          ON sm.ric = h.ric
                        WHERE 1=1
                          {ticker_clause}
                    )
                    SELECT *
                    FROM ranked
                    WHERE rn = 1
                    ORDER BY ticker ASC
                    """,
                    conn,
                    params=[max_asof, *ticker_params],
                )
        if not trbc_hist.empty:
            trbc_hist["ticker"] = trbc_hist["ticker"].astype(str).str.upper()
            trbc_hist["trbc_effective_date"] = trbc_hist["as_of_date"].astype(str)
            trbc_hist["trbc_effective_date_dt"] = pd.to_datetime(trbc_hist["trbc_effective_date"], errors="coerce")
            trbc_hist = trbc_hist.dropna(subset=["trbc_effective_date_dt"])
            trbc_hist = (
                trbc_hist.sort_values(["ticker", "trbc_effective_date", "updated_at"])
                .drop_duplicates(subset=["ticker", "trbc_effective_date"], keep="last")
            )
            trbc_hist = trbc_hist.rename(
                columns={
                    "source": "trbc_source",
                    "job_run_id": "trbc_job_run_id",
                }
            )

        # Price events
        if mode_norm == "full":
            prices = pd.read_sql_query(
                f"""
                SELECT
                    UPPER(sm.ticker) AS ticker,
                    p.date,
                    p.close,
                    p.currency,
                    p.source,
                    p.updated_at
                FROM security_prices_eod p
                JOIN security_master sm
                  ON sm.ric = p.ric
                WHERE p.date <= ?
                  {ticker_clause}
                ORDER BY UPPER(sm.ticker), p.date
                """,
                conn,
                params=[max_asof, *ticker_params],
            )
        else:
            prices = pd.read_sql_query(
                f"""
                WITH latest_date AS (
                    SELECT ric, MAX(date) AS max_date
                    FROM security_prices_eod
                    WHERE date <= ?
                    GROUP BY ric
                ),
                ranked AS (
                    SELECT
                        UPPER(sm.ticker) AS ticker,
                        p.date,
                        p.close,
                        p.currency,
                        p.source,
                        p.updated_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY UPPER(sm.ticker)
                            ORDER BY p.updated_at DESC
                        ) AS rn
                    FROM security_prices_eod p
                    JOIN latest_date ld
                      ON ld.ric = p.ric
                     AND ld.max_date = p.date
                    JOIN security_master sm
                      ON sm.ric = p.ric
                    WHERE 1=1
                      {ticker_clause}
                )
                SELECT ticker, date, close, currency, source, updated_at
                FROM ranked
                WHERE rn = 1
                ORDER BY ticker ASC
                """,
                conn,
                params=[max_asof, *ticker_params],
            )
        if not prices.empty:
            prices["ticker"] = prices["ticker"].astype(str).str.upper()
            prices["price_date"] = prices["date"].astype(str)
            prices["price_date_dt"] = pd.to_datetime(prices["price_date"], errors="coerce")
            prices = prices.dropna(subset=["price_date_dt"])
            prices = (
                prices.sort_values(["ticker", "price_date", "updated_at"])
                .drop_duplicates(subset=["ticker", "price_date"], keep="last")
            )
            prices = prices.rename(
                columns={
                    "close": "price_close",
                    "currency": "price_currency",
                    "source": "price_source",
                }
            )

        out = base.copy()
        if not fundamentals.empty:
            fmerge_cols = [
                "ticker",
                "fetch_date",
                "fetch_date_dt",
                *[c for c in fundamental_cols if c in fundamentals.columns],
                "fundamental_source",
                "fundamental_job_run_id",
            ]
            out = _merge_asof_by_ticker(
                out,
                fundamentals[fmerge_cols],
                left_date_col="as_of_date_dt",
                right_date_col="fetch_date_dt",
            )
            out = out.rename(columns={"fetch_date": "fundamental_fetch_date"})

        if not trbc_hist.empty:
            hmerge_cols = [
                "ticker",
                "trbc_effective_date",
                "trbc_effective_date_dt",
                *[c for c in trbc_cols if c in trbc_hist.columns],
                "trbc_source",
                "trbc_job_run_id",
            ]
            out = _merge_asof_by_ticker(
                out,
                trbc_hist[hmerge_cols],
                left_date_col="as_of_date_dt",
                right_date_col="trbc_effective_date_dt",
            )

        if not prices.empty:
            pmerge_cols = [
                "ticker",
                "price_date",
                "price_date_dt",
                "price_close",
                "price_currency",
                "price_source",
            ]
            out = _merge_asof_by_ticker(
                out,
                prices[pmerge_cols],
                left_date_col="as_of_date_dt",
                right_date_col="price_date_dt",
            )

        # Merge collisions between fundamental and TRBC-history fields.
        for col in [
            "trbc_economic_sector",
            "trbc_business_sector",
            "trbc_industry_group",
            "trbc_industry",
            "trbc_activity",
        ]:
            _coalesce_columns(out, col, [f"{col}_y", col, f"{col}_x"])

        # Fill compatibility sector alias
        if "trbc_economic_sector_short" not in out.columns:
            out["trbc_economic_sector_short"] = ""
        if "trbc_sector" in out.columns:
            out["trbc_economic_sector_short"] = out["trbc_economic_sector_short"].fillna(out["trbc_sector"])
        if "trbc_economic_sector" in out.columns:
            out["trbc_economic_sector_short"] = out["trbc_economic_sector_short"].fillna(out["trbc_economic_sector"])

        _sanitize_num(
            out,
            [
                "market_cap",
                "shares_outstanding",
                "dividend_yield",
                "book_value",
                "forward_eps",
                "trailing_eps",
                "total_debt",
                "cash_and_equivalents",
                "long_term_debt",
                "free_cash_flow",
                "gross_profit",
                "net_income",
                "operating_cashflow",
                "capital_expenditures",
                "shares_basic",
                "shares_diluted",
                "free_float_shares",
                "free_float_percent",
                "revenue",
                "ebitda",
                "ebit",
                "total_assets",
                "total_liabilities",
                "return_on_equity",
                "operating_margins",
                "fiscal_year",
                "price_close",
            ],
        )

        now_iso = datetime.now(timezone.utc).isoformat()
        job_run_id = f"cross_snapshot_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        out["updated_at"] = now_iso
        out["snapshot_job_run_id"] = job_run_id

        for dcol in [
            "as_of_date",
            "fundamental_fetch_date",
            "fundamental_period_end_date",
            "trbc_effective_date",
            "price_date",
        ]:
            if dcol in out.columns:
                out[dcol] = out[dcol].astype("object")

        target_cols = [
            "ticker",
            "as_of_date",
            "fundamental_fetch_date",
            "fundamental_period_end_date",
            "market_cap",
            "shares_outstanding",
            "dividend_yield",
            "common_name",
            "book_value",
            "forward_eps",
            "trailing_eps",
            "total_debt",
            "cash_and_equivalents",
            "long_term_debt",
            "free_cash_flow",
            "gross_profit",
            "net_income",
            "operating_cashflow",
            "capital_expenditures",
            "shares_basic",
            "shares_diluted",
            "free_float_shares",
            "free_float_percent",
            "revenue",
            "ebitda",
            "ebit",
            "total_assets",
            "total_liabilities",
            "return_on_equity",
            "operating_margins",
            "report_currency",
            "fiscal_year",
            "period_type",
            "trbc_economic_sector_short",
            "trbc_economic_sector",
            "trbc_business_sector",
            "trbc_industry_group",
            "trbc_industry",
            "trbc_activity",
            "trbc_effective_date",
            "price_date",
            "price_close",
            "price_currency",
            "fundamental_source",
            "trbc_source",
            "price_source",
            "fundamental_job_run_id",
            "trbc_job_run_id",
            "snapshot_job_run_id",
            "updated_at",
        ]
        for col in target_cols:
            if col not in out.columns:
                out[col] = None

        payload = out[target_cols].where(pd.notna(out[target_cols]), None)
        if mode_norm != "full":
            if tickers:
                placeholders = ",".join("?" for _ in base_tickers)
                conn.execute(
                    f"DELETE FROM {TABLE} WHERE UPPER(ticker) IN ({placeholders})",
                    base_tickers,
                )
            else:
                conn.execute(f"DELETE FROM {TABLE}")
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {TABLE}
            ({", ".join(target_cols)})
            VALUES ({", ".join(['?'] * len(target_cols))})
            """,
            payload.itertuples(index=False, name=None),
        )
        conn.commit()
        return {
            "status": "ok",
            "table": TABLE,
            "rows_upserted": int(len(payload)),
            "job_run_id": job_run_id,
            "max_asof": max_asof,
            "mode": mode_norm,
        }
    finally:
        conn.close()
