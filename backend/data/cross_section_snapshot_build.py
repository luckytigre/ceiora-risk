"""Build helpers for the canonical cross-section snapshot payload."""

from __future__ import annotations

from datetime import datetime, timezone
import sqlite3
from typing import Any

import pandas as pd

from backend.data.cross_section_snapshot_schema import TABLE, table_columns, table_exists
from backend.universe.security_master_sync import load_default_source_universe_rows


def load_base_cross_sections(
    conn: sqlite3.Connection,
    *,
    start_date: str | None,
    end_date: str | None,
    tickers: list[str] | None,
    mode: str,
) -> pd.DataFrame:
    source_table = "barra_raw_cross_section_history"
    source_cols = table_columns(conn, source_table)
    if "ric" not in source_cols or "as_of_date" not in source_cols:
        return pd.DataFrame(columns=["ric", "ticker", "as_of_date"])

    clauses: list[str] = []
    params: list[Any] = []
    if start_date:
        clauses.append("e.as_of_date >= ?")
        params.append(str(start_date))
    if end_date:
        clauses.append("e.as_of_date <= ?")
        params.append(str(end_date))
    requested_tickers = [str(ticker).strip().upper() for ticker in (tickers or []) if str(ticker).strip()]
    if requested_tickers and "ticker" in source_cols:
        placeholders = ",".join("?" for _ in requested_tickers)
        clauses.append(f"UPPER(e.ticker) IN ({placeholders})")
        params.extend(requested_tickers)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    if str(mode).strip().lower() == "full":
        df = pd.read_sql_query(
            f"""
            SELECT DISTINCT UPPER(e.ric) AS ric, UPPER(COALESCE(e.ticker, '')) AS ticker, e.as_of_date
            FROM {source_table} e
            {where_sql}
            ORDER BY UPPER(e.ric), e.as_of_date
            """,
            conn,
            params=params,
        )
    else:
        df = pd.read_sql_query(
            f"""
            SELECT
                UPPER(e.ric) AS ric,
                UPPER(COALESCE(MAX(e.ticker), '')) AS ticker,
                MAX(e.as_of_date) AS as_of_date
            FROM {source_table} e
            {where_sql}
            GROUP BY UPPER(e.ric)
            ORDER BY UPPER(e.ric), as_of_date
            """,
            conn,
            params=params,
        )
    if df.empty:
        return df
    df["ric"] = df["ric"].astype(str).str.upper()
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["as_of_date"] = df["as_of_date"].astype(str)
    source_universe_rows = load_default_source_universe_rows(conn, include_pending_seed=False)
    if source_universe_rows:
        source_universe = pd.DataFrame(source_universe_rows)
        source_universe["ric"] = source_universe["ric"].astype(str).str.upper()
        source_universe["ticker"] = source_universe["ticker"].astype(str).str.upper()
        df = df.drop(columns=["ticker"], errors="ignore").merge(
            source_universe,
            on="ric",
            how="inner",
        )
    if requested_tickers:
        df = df[df["ticker"].astype(str).str.upper().isin(requested_tickers)]
    return df


def merge_asof_by_ric(
    base: pd.DataFrame,
    events: pd.DataFrame,
    *,
    left_date_col: str,
    right_date_col: str,
) -> pd.DataFrame:
    if events.empty:
        return base
    merged_parts: list[pd.DataFrame] = []
    events_by_ric = {str(t): grp.copy() for t, grp in events.groupby("ric", sort=False)}
    for ric, left_grp in base.groupby("ric", sort=False):
        left_sorted = left_grp.sort_values(left_date_col).reset_index(drop=True)
        right_grp = events_by_ric.get(str(ric))
        if right_grp is None or right_grp.empty:
            merged_parts.append(left_sorted)
            continue
        right_sorted = right_grp.sort_values(right_date_col).reset_index(drop=True)
        right_sorted = right_sorted.drop(columns=["ric", "ticker"], errors="ignore")
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


def sanitize_num(df: pd.DataFrame, cols: list[str]) -> None:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")


def coalesce_columns(df: pd.DataFrame, target: str, candidates: list[str]) -> None:
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


def build_snapshot_payload(
    conn: sqlite3.Connection,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    tickers: list[str] | None = None,
    mode: str = "current",
) -> dict[str, Any]:
    mode_norm = str(mode or "current").strip().lower()
    if mode_norm not in {"current", "full"}:
        mode_norm = "current"

    base = load_base_cross_sections(
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
    base_rics = sorted(set(base["ric"].astype(str).str.upper().tolist()))
    if not base_rics:
        return {"status": "no-op", "rows_upserted": 0, "table": TABLE}

    identity = base[["ric", "ticker"]].drop_duplicates(subset=["ric"], keep="last")

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
    trbc_cols = [
        "trbc_economic_sector",
        "trbc_business_sector",
        "trbc_industry_group",
        "trbc_industry",
        "trbc_activity",
    ]

    fundamentals = pd.DataFrame()
    if table_exists(conn, "security_fundamentals_pit"):
        fundamental_placeholders = ",".join("?" for _ in base_rics)
        fundamental_ric_clause = f" AND UPPER(f.ric) IN ({fundamental_placeholders})"
        fundamental_params: list[Any] = [max_asof, *base_rics]
        fundamental_select = ", ".join(
            [
                "UPPER(f.ric) AS ric",
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
                SELECT {fundamental_select}
                FROM security_fundamentals_pit f
                WHERE f.as_of_date <= ?{fundamental_ric_clause}
                ORDER BY UPPER(f.ric), f.as_of_date, f.updated_at
                """,
                conn,
                params=fundamental_params,
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
                        {fundamental_select},
                        ROW_NUMBER() OVER (
                            PARTITION BY UPPER(f.ric)
                            ORDER BY f.stat_date DESC, f.updated_at DESC
                        ) AS rn
                    FROM security_fundamentals_pit f
                    JOIN latest_date ld
                      ON ld.ric = f.ric
                     AND ld.max_as_of_date = f.as_of_date
                    WHERE 1=1{fundamental_ric_clause}
                )
                SELECT *
                FROM ranked
                WHERE rn = 1
                ORDER BY ric ASC
                """,
                conn,
                params=fundamental_params,
            )
        if not fundamentals.empty:
            fundamentals["ric"] = fundamentals["ric"].astype(str).str.upper()
            fundamentals = fundamentals.merge(identity, on="ric", how="left")
            fundamentals["ticker"] = fundamentals["ticker"].astype(str).str.upper()
            fundamentals["fetch_date_dt"] = pd.to_datetime(fundamentals["fetch_date"], errors="coerce")
            fundamentals = fundamentals.dropna(subset=["fetch_date_dt"])
            fundamentals = (
                fundamentals.sort_values(["ric", "fetch_date", "updated_at"])
                .drop_duplicates(subset=["ric", "fetch_date"], keep="last")
            )
            fundamentals = fundamentals.rename(
                columns={
                    "source": "fundamental_source",
                    "job_run_id": "fundamental_job_run_id",
                }
            )

    trbc_hist = pd.DataFrame()
    if table_exists(conn, "security_classification_pit"):
        trbc_placeholders = ",".join("?" for _ in base_rics)
        trbc_ric_clause = f" AND UPPER(h.ric) IN ({trbc_placeholders})"
        trbc_params: list[Any] = [max_asof, *base_rics]
        trbc_select = ", ".join(
            [
                "UPPER(h.ric) AS ric",
                "h.as_of_date",
                *[f"h.{col}" for col in trbc_cols],
                "h.source AS source",
                "h.job_run_id AS job_run_id",
                "h.updated_at AS updated_at",
            ]
        )
        if mode_norm == "full":
            trbc_hist = pd.read_sql_query(
                f"""
                SELECT {trbc_select}
                FROM security_classification_pit h
                WHERE h.as_of_date <= ?{trbc_ric_clause}
                ORDER BY UPPER(h.ric), h.as_of_date, h.updated_at
                """,
                conn,
                params=trbc_params,
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
                        {trbc_select},
                        ROW_NUMBER() OVER (
                            PARTITION BY UPPER(h.ric)
                            ORDER BY h.updated_at DESC
                        ) AS rn
                    FROM security_classification_pit h
                    JOIN latest_date ld
                      ON ld.ric = h.ric
                     AND ld.max_as_of_date = h.as_of_date
                    WHERE 1=1{trbc_ric_clause}
                )
                SELECT *
                FROM ranked
                WHERE rn = 1
                ORDER BY ric ASC
                """,
                conn,
                params=trbc_params,
            )
        if not trbc_hist.empty:
            trbc_hist["ric"] = trbc_hist["ric"].astype(str).str.upper()
            trbc_hist = trbc_hist.merge(identity, on="ric", how="left")
            trbc_hist["ticker"] = trbc_hist["ticker"].astype(str).str.upper()
            trbc_hist["trbc_effective_date"] = trbc_hist["as_of_date"].astype(str)
            trbc_hist["trbc_effective_date_dt"] = pd.to_datetime(
                trbc_hist["trbc_effective_date"],
                errors="coerce",
            )
            trbc_hist = trbc_hist.dropna(subset=["trbc_effective_date_dt"])
            trbc_hist = (
                trbc_hist.sort_values(["ric", "trbc_effective_date", "updated_at"])
                .drop_duplicates(subset=["ric", "trbc_effective_date"], keep="last")
            )
            trbc_hist = trbc_hist.rename(
                columns={
                    "source": "trbc_source",
                    "job_run_id": "trbc_job_run_id",
                }
            )

    prices = pd.DataFrame()
    if table_exists(conn, "security_prices_eod"):
        price_placeholders = ",".join("?" for _ in base_rics)
        price_ric_clause = f" AND UPPER(p.ric) IN ({price_placeholders})"
        price_params: list[Any] = [max_asof, *base_rics]
        if mode_norm == "full":
            prices = pd.read_sql_query(
                f"""
                SELECT
                    UPPER(p.ric) AS ric,
                    p.date,
                    p.close,
                    p.currency,
                    p.source,
                    p.updated_at
                FROM security_prices_eod p
                WHERE p.date <= ?{price_ric_clause}
                ORDER BY UPPER(p.ric), p.date, p.updated_at
                """,
                conn,
                params=price_params,
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
                        UPPER(p.ric) AS ric,
                        p.date,
                        p.close,
                        p.currency,
                        p.source,
                        p.updated_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY UPPER(p.ric)
                            ORDER BY p.updated_at DESC
                        ) AS rn
                    FROM security_prices_eod p
                    JOIN latest_date ld
                      ON ld.ric = p.ric
                     AND ld.max_date = p.date
                    WHERE 1=1{price_ric_clause}
                )
                SELECT ric, date, close, currency, source, updated_at
                FROM ranked
                WHERE rn = 1
                ORDER BY ric ASC
                """,
                conn,
                params=price_params,
            )
        if not prices.empty:
            prices["ric"] = prices["ric"].astype(str).str.upper()
            prices = prices.merge(identity, on="ric", how="left")
            prices["ticker"] = prices["ticker"].astype(str).str.upper()
            prices["price_date"] = prices["date"].astype(str)
            prices["price_date_dt"] = pd.to_datetime(prices["price_date"], errors="coerce")
            prices = prices.dropna(subset=["price_date_dt"])
            prices = (
                prices.sort_values(["ric", "price_date", "updated_at"])
                .drop_duplicates(subset=["ric", "price_date"], keep="last")
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
        fundamental_merge_cols = [
            "ric",
            "ticker",
            "fetch_date",
            "fetch_date_dt",
            *[col for col in fundamental_cols if col in fundamentals.columns],
            "fundamental_source",
            "fundamental_job_run_id",
        ]
        out = merge_asof_by_ric(
            out,
            fundamentals[fundamental_merge_cols],
            left_date_col="as_of_date_dt",
            right_date_col="fetch_date_dt",
        )
        out = out.rename(columns={"fetch_date": "fundamental_fetch_date"})

    if not trbc_hist.empty:
        trbc_merge_cols = [
            "ric",
            "ticker",
            "trbc_effective_date",
            "trbc_effective_date_dt",
            *[col for col in trbc_cols if col in trbc_hist.columns],
            "trbc_source",
            "trbc_job_run_id",
        ]
        out = merge_asof_by_ric(
            out,
            trbc_hist[trbc_merge_cols],
            left_date_col="as_of_date_dt",
            right_date_col="trbc_effective_date_dt",
        )

    if not prices.empty:
        price_merge_cols = [
            "ric",
            "ticker",
            "price_date",
            "price_date_dt",
            "price_close",
            "price_currency",
            "price_source",
        ]
        out = merge_asof_by_ric(
            out,
            prices[price_merge_cols],
            left_date_col="as_of_date_dt",
            right_date_col="price_date_dt",
        )

    for col in trbc_cols:
        coalesce_columns(out, col, [f"{col}_y", col, f"{col}_x"])

    if "trbc_economic_sector_short" not in out.columns:
        out["trbc_economic_sector_short"] = ""
    if "trbc_sector" in out.columns:
        out["trbc_economic_sector_short"] = out["trbc_economic_sector_short"].fillna(out["trbc_sector"])
    if "trbc_economic_sector" in out.columns:
        out["trbc_economic_sector_short"] = out["trbc_economic_sector_short"].fillna(
            out["trbc_economic_sector"]
        )

    sanitize_num(
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
        "ric",
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
    return {
        "status": "ok",
        "table": TABLE,
        "rows_upserted": int(len(payload)),
        "job_run_id": job_run_id,
        "max_asof": max_asof,
        "mode": mode_norm,
        "payload": payload,
        "base_tickers": base_tickers,
    }
