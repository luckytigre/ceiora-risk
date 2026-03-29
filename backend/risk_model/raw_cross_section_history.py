"""Build in-project cUSE raw cross-section history and style scores."""

from __future__ import annotations

import logging
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from backend.risk_model.descriptors import assemble_full_style_scores
from backend.risk_model.eligibility import structural_eligibility_for_snapshot
from backend.risk_model.risk_attribution import STYLE_COLUMN_TO_LABEL
from backend.data.trbc_schema import (
    ensure_trbc_naming,
    pick_trbc_business_sector_column,
    pick_trbc_industry_column,
)
from backend.trading_calendar import filter_xnys_sessions
from backend.universe.runtime_rows import load_security_runtime_rows
from backend.universe.security_master_sync import load_default_source_universe_rows

logger = logging.getLogger(__name__)

TABLE = "barra_raw_cross_section_history"
SECURITY_MASTER_TABLE = "security_master"
PRICES_TABLE = "security_prices_eod"
FUNDAMENTALS_TABLE = "security_fundamentals_pit"
CLASSIFICATION_TABLE = "security_classification_pit"
MODEL_VERSION = "inproj-v1"
DESCRIPTOR_VERSION = "raw-cross-section-v3-no-value-factor"

_SCORE_COLS = list(STYLE_COLUMN_TO_LABEL.keys())
_FACTOR_TO_SCORE_COL = {v: k for k, v in STYLE_COLUMN_TO_LABEL.items()}

_RAW_DESCRIPTOR_COLS = [
    "beta_raw",
    "momentum_raw",
    "size_raw",
    "nonlinear_size_raw",
    "st_reversal_raw",
    "resid_vol_raw",
    "turnover_1m_raw",
    "turnover_12m_raw",
    "log_avg_dollar_volume_20d_raw",
    "book_to_price_raw",
    "forward_ep_raw",
    "cash_earnings_yield_raw",
    "trailing_ep_raw",
    "debt_to_equity_raw",
    "debt_to_assets_raw",
    "book_leverage_raw",
    "sales_growth_raw",
    "eps_growth_raw",
    "roe_raw",
    "gross_profitability_raw",
    "asset_growth_raw",
    "dividend_yield_raw",
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


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def _create_raw_cross_section_history_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            ric TEXT NOT NULL,
            ticker TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            market_cap REAL,
            price_close REAL,
            price_volume REAL,
            shares_outstanding REAL,
            trbc_economic_sector_short TEXT,
            trbc_business_sector TEXT,
            beta_raw REAL,
            momentum_raw REAL,
            size_raw REAL,
            nonlinear_size_raw REAL,
            st_reversal_raw REAL,
            resid_vol_raw REAL,
            turnover_1m_raw REAL,
            turnover_12m_raw REAL,
            log_avg_dollar_volume_20d_raw REAL,
            book_to_price_raw REAL,
            forward_ep_raw REAL,
            cash_earnings_yield_raw REAL,
            trailing_ep_raw REAL,
            debt_to_equity_raw REAL,
            debt_to_assets_raw REAL,
            book_leverage_raw REAL,
            sales_growth_raw REAL,
            eps_growth_raw REAL,
            roe_raw REAL,
            gross_profitability_raw REAL,
            asset_growth_raw REAL,
            dividend_yield_raw REAL,
            beta_score REAL,
            momentum_score REAL,
            size_score REAL,
            nonlinear_size_score REAL,
            short_term_reversal_score REAL,
            resid_vol_score REAL,
            liquidity_score REAL,
            book_to_price_score REAL,
            earnings_yield_score REAL,
            leverage_score REAL,
            growth_score REAL,
            profitability_score REAL,
            investment_score REAL,
            dividend_yield_score REAL,
            confidence_band TEXT,
            fallback_depth TEXT,
            idio_var_daily REAL,
            coverage_degraded TEXT,
            barra_model_version TEXT,
            descriptor_schema_version TEXT,
            assumption_set_version TEXT,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT,
            PRIMARY KEY (ric, as_of_date)
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_asof ON {TABLE}(as_of_date)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_ticker ON {TABLE}(ticker)")
    conn.execute(f"DROP INDEX IF EXISTS idx_{TABLE}_ric")


def ensure_raw_cross_section_history_table(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, TABLE):
        _create_raw_cross_section_history_table(conn)
        return
    cols = _table_columns(conn, TABLE)
    expected = {"ric", "ticker", "as_of_date"}
    pk_cols = [
        str(r[1])
        for r in conn.execute(f"PRAGMA table_info({TABLE})").fetchall()
        if int(r[5] or 0) > 0
    ]
    if expected.issubset(cols) and pk_cols == ["ric", "as_of_date"] and "trbc_business_sector" in cols:
        _create_raw_cross_section_history_table(conn)
        if "value_score" in cols:
            try:
                conn.execute(f"ALTER TABLE {TABLE} DROP COLUMN value_score")
            except sqlite3.OperationalError:
                logger.warning(
                    "Unable to drop legacy value_score column from %s in place; recreating table without it",
                    TABLE,
                )
                conn.execute(f"DROP TABLE IF EXISTS {TABLE}")
                _create_raw_cross_section_history_table(conn)
        conn.execute(f"DROP INDEX IF EXISTS idx_{TABLE}_ric")
        return

    logger.warning(
        "Recreating %s due to schema mismatch (expected PK=(ric, as_of_date), found=%s)",
        TABLE,
        pk_cols,
    )
    conn.execute(f"DROP TABLE IF EXISTS {TABLE}")
    _create_raw_cross_section_history_table(conn)


def _dedupe_prices(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["ric"] = out["ric"].astype(str).str.upper()
    out["ticker"] = out["ticker"].astype(str).str.upper()
    out["date"] = out["date"].astype(str)
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out["volume"] = pd.to_numeric(out.get("volume"), errors="coerce")
    out["updated_at"] = pd.to_datetime(out.get("updated_at"), errors="coerce")
    out["source"] = out.get("source", pd.Series(index=out.index, dtype="object")).astype(str).str.lower()
    out["source_priority"] = np.where(out["source"] == "lseg_toolkit", 1, 0)
    out = out.sort_values(
        ["ric", "date", "source_priority", "updated_at"],
        ascending=[True, True, False, False],
    )
    out = out.drop_duplicates(subset=["ric", "date"], keep="first")
    return out[["ric", "ticker", "date", "close", "volume"]]


def _merge_asof_by_ric(
    base: pd.DataFrame,
    events: pd.DataFrame,
    *,
    left_date_col: str,
    right_date_col: str,
) -> pd.DataFrame:
    if base.empty or events.empty:
        return base
    merged_parts: list[pd.DataFrame] = []
    events_by_ric = {str(ric): grp.copy() for ric, grp in events.groupby("ric", sort=False)}
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


def _compute_price_features(prices: pd.DataFrame) -> pd.DataFrame:
    if prices.empty:
        return prices
    out = prices.copy().sort_values(["ric", "date"])
    g = out.groupby("ric", sort=False)
    close = pd.to_numeric(out["close"], errors="coerce")
    shift_21 = g["close"].shift(21)
    shift_252 = g["close"].shift(252)
    out["ret_1d"] = g["close"].pct_change(fill_method=None)
    out["momentum_raw"] = (shift_21 / shift_252) - 1.0
    out["st_reversal_raw"] = -((close / shift_21) - 1.0)
    out["resid_vol_raw"] = g["ret_1d"].rolling(63, min_periods=20).std().reset_index(level=0, drop=True)
    out["dollar_volume"] = out["close"] * out["volume"]
    out["avg_dollar_volume_20d"] = (
        g["dollar_volume"]
        .rolling(20, min_periods=10)
        .mean()
        .reset_index(level=0, drop=True)
    )
    out["avg_volume_21d"] = g["volume"].rolling(21, min_periods=10).mean().reset_index(level=0, drop=True)
    out["avg_volume_252d"] = g["volume"].rolling(252, min_periods=60).mean().reset_index(level=0, drop=True)

    valid_market = (
        np.isfinite(pd.to_numeric(out["ret_1d"], errors="coerce").to_numpy(dtype=float))
        & np.isfinite(close.to_numpy(dtype=float))
        & (close.to_numpy(dtype=float) >= 5.0)
    )
    market_ret = (
        out.loc[valid_market]
        .groupby("date", sort=False)["ret_1d"]
        .median()
        .rename("market_ret")
    )
    out = out.merge(market_ret, on="date", how="left")

    def _rolling_beta(grp: pd.DataFrame) -> pd.Series:
        ri = pd.to_numeric(grp["ret_1d"], errors="coerce")
        rm = pd.to_numeric(grp["market_ret"], errors="coerce")
        cov = ri.rolling(252, min_periods=60).cov(rm)
        var = rm.rolling(252, min_periods=60).var()
        beta = cov / var.replace(0.0, np.nan)
        return beta

    beta_parts: list[pd.Series] = []
    for _, grp in out.groupby("ric", sort=False):
        beta = _rolling_beta(grp)
        beta.index = grp.index
        beta_parts.append(beta)
    out["beta_raw"] = (
        pd.concat(beta_parts).sort_index()
        if beta_parts
        else pd.Series(dtype=float)
    )
    out["log_avg_dollar_volume_20d_raw"] = np.log(np.clip(out["avg_dollar_volume_20d"], 0.0, None) + 1.0)
    return out


def _load_dates(conn: sqlite3.Connection, *, start_date: str | None, end_date: str | None) -> list[str]:
    clauses = []
    params: list[Any] = []
    if start_date:
        clauses.append("date >= ?")
        params.append(str(start_date))
    if end_date:
        clauses.append("date <= ?")
        params.append(str(end_date))
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = conn.execute(
        f"""
        SELECT DISTINCT date
        FROM {PRICES_TABLE}
        {where_sql}
        ORDER BY date
        """,
        params,
    ).fetchall()
    out = [str(r[0]) for r in rows if r and r[0]]
    return filter_xnys_sessions(out)


def _resolve_target_dates(
    conn: sqlite3.Connection,
    *,
    start_date: str | None,
    end_date: str | None,
    frequency: str,
) -> list[str]:
    trading_dates = _load_dates(conn, start_date=start_date, end_date=end_date)
    if not trading_dates:
        return []
    if frequency == "daily":
        dates = set(trading_dates)
    elif frequency == "weekly":
        ts = pd.to_datetime(pd.Series(trading_dates), errors="coerce")
        dates = {str(d.date()) for d in ts[ts.dt.weekday == 4] if pd.notna(d)}
        dates.add(trading_dates[-1])
    elif frequency == "latest":
        dates = {trading_dates[-1]}
    else:
        dates = {trading_dates[-1]}
    out = sorted(d for d in dates if d and (start_date is None or d >= str(start_date)) and (end_date is None or d <= str(end_date)))
    return out


def _normalize_text(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .replace({"nan": "", "None": "", "Unmapped": "", "unmapped": ""})
    )


def _load_runtime_identity(conn: sqlite3.Connection) -> pd.DataFrame:
    runtime_rows = load_security_runtime_rows(
        conn,
        include_disabled=False,
        allow_empty_registry_fallback=False,
    )
    if not runtime_rows:
        return pd.DataFrame(columns=["ric", "ticker"])
    runtime_identity = pd.DataFrame(
        [
            {
                "ric": row.get("ric"),
                "ticker": row.get("ticker"),
            }
            for row in runtime_rows
        ]
    )
    if runtime_identity.empty:
        return pd.DataFrame(columns=["ric", "ticker"])
    runtime_identity["ric"] = runtime_identity["ric"].astype(str).str.upper()
    runtime_identity["ticker"] = runtime_identity["ticker"].astype(str).str.upper()
    return runtime_identity[["ric", "ticker"]].drop_duplicates(subset=["ric"], keep="last")


def rebuild_raw_cross_section_history(
    data_db: Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    frequency: str = "weekly",
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Rebuild in-project raw/scores table from source-of-truth datasets."""
    stage_t0 = time.perf_counter()
    conn = sqlite3.connect(str(data_db))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    ensure_trbc_naming(conn)
    ensure_raw_cross_section_history_table(conn)

    try:
        dates = _resolve_target_dates(
            conn,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
        )
        logger.info(
            "Raw cross-section rebuild target dates resolved: frequency=%s count=%s start=%s end=%s",
            frequency,
            len(dates),
            dates[0] if dates else None,
            dates[-1] if dates else None,
        )
        if progress_callback is not None:
            progress_callback(
                {
                    "message": f"Resolved {len(dates)} target dates for raw cross-section rebuild",
                    "items_processed": 0,
                    "items_total": int(len(dates)),
                    "unit": "dates",
                    "progress_pct": 0.0,
                    "progress_kind": "dates",
                }
            )
        if not dates:
            return {"status": "no-dates", "rows_upserted": 0, "table": TABLE}

        min_for_roll = (
            pd.to_datetime(dates[0], errors="coerce") - timedelta(days=420)
        ).date().isoformat()
        max_date = dates[-1]

        runtime_identity = _load_runtime_identity(conn)
        if runtime_identity.empty:
            return {"status": "no-runtime-identity", "rows_upserted": 0, "table": TABLE}
        default_source_rows = load_default_source_universe_rows(conn, include_pending_seed=False)
        if default_source_rows:
            default_source_rics = {
                str(row.get("ric") or "").strip().upper()
                for row in default_source_rows
                if str(row.get("ric") or "").strip()
            }
            runtime_identity = runtime_identity[
                runtime_identity["ric"].astype(str).str.upper().isin(default_source_rics)
            ]
        if runtime_identity.empty:
            return {"status": "no-runtime-identity", "rows_upserted": 0, "table": TABLE}
        runtime_union_identity = runtime_identity[["ric", "ticker"]].drop_duplicates(subset=["ric"], keep="last")
        runtime_rics = runtime_union_identity["ric"].astype(str).tolist()
        runtime_placeholders = ",".join("?" for _ in runtime_rics)

        prices_raw = pd.read_sql_query(
            f"""
            SELECT UPPER(p.ric) AS ric, p.date, p.close, p.volume, p.source, p.updated_at
            FROM {PRICES_TABLE} p
            WHERE p.date >= ? AND p.date <= ?
              AND UPPER(p.ric) IN ({runtime_placeholders})
            ORDER BY UPPER(p.ric), p.date
            """,
            conn,
            params=(min_for_roll, max_date, *runtime_rics),
        )
        if not prices_raw.empty:
            prices_raw["ric"] = prices_raw["ric"].astype(str).str.upper()
            prices_raw = prices_raw.merge(runtime_union_identity, on="ric", how="inner")
        prices = _dedupe_prices(prices_raw)
        prices = _compute_price_features(prices)
        logger.info(
            "Loaded price history for raw cross-section: rows=%s rics=%s",
            len(prices),
            prices["ric"].nunique() if not prices.empty else 0,
        )
        if progress_callback is not None:
            progress_callback(
                {
                    "message": f"Loaded price history for {prices['ric'].nunique() if not prices.empty else 0} securities",
                    "items_processed": 1,
                    "items_total": 4,
                    "unit": "setup_steps",
                    "progress_pct": 25.0,
                    "progress_kind": "setup",
                }
            )
        if prices.empty:
            return {"status": "no-prices", "rows_upserted": 0, "table": TABLE}

        target_set = set(dates)
        base = prices.loc[prices["date"].isin(target_set)].copy()
        base = base.rename(columns={"date": "as_of_date", "close": "price_close", "volume": "price_volume"})
        base["as_of_date_dt"] = pd.to_datetime(base["as_of_date"], errors="coerce")
        base = base.dropna(subset=["as_of_date_dt"])
        base = base.merge(runtime_union_identity, on=["ric", "ticker"], how="inner")
        if base.empty:
            return {"status": "no-base", "rows_upserted": 0, "table": TABLE}

        fcols = _table_columns(conn, FUNDAMENTALS_TABLE)
        fkeep = [
            "market_cap",
            "shares_outstanding",
            "dividend_yield",
            "book_value_per_share",
            "forward_eps",
            "trailing_eps",
            "total_debt",
            "operating_cashflow",
            "revenue",
            "total_assets",
            "roe_pct",
            "operating_margin_pct",
            "common_name",
        ]
        fkeep = [c for c in fkeep if c in fcols]
        fnum_cols = [c for c in fkeep if c != "common_name"]
        if not fkeep:
            fundamentals = pd.DataFrame()
        else:
            fselect_cols = ", ".join(f"f.{c}" for c in fkeep)
            fundamentals = pd.read_sql_query(
                f"""
                SELECT UPPER(f.ric) AS ric, f.as_of_date AS fetch_date, {fselect_cols}
                FROM {FUNDAMENTALS_TABLE} f
                WHERE f.as_of_date <= ?
                  AND UPPER(f.ric) IN ({runtime_placeholders})
                ORDER BY UPPER(f.ric), f.as_of_date
                """,
                conn,
                params=(max_date, *runtime_rics),
            )
        if not fundamentals.empty:
            fundamentals["ric"] = fundamentals["ric"].astype(str).str.upper()
            fundamentals = fundamentals.merge(runtime_union_identity, on="ric", how="left")
            fundamentals["ticker"] = fundamentals["ticker"].astype(str).str.upper()
            fundamentals["fetch_date"] = fundamentals["fetch_date"].astype(str)
            fundamentals["fetch_date_dt"] = pd.to_datetime(fundamentals["fetch_date"], errors="coerce")
            fundamentals = fundamentals.dropna(subset=["fetch_date_dt"])
            for col in fnum_cols:
                fundamentals[col] = pd.to_numeric(fundamentals[col], errors="coerce")
            if "book_value_per_share" in fundamentals.columns:
                fundamentals["book_value"] = fundamentals["book_value_per_share"]
            if "roe_pct" in fundamentals.columns:
                fundamentals["return_on_equity"] = fundamentals["roe_pct"]
            if "operating_margin_pct" in fundamentals.columns:
                fundamentals["operating_margins"] = fundamentals["operating_margin_pct"]
            fundamentals = fundamentals.sort_values(["ric", "fetch_date"]).drop_duplicates(
                subset=["ric", "fetch_date"], keep="last"
            )
            fundamentals["sales_growth_raw"] = fundamentals.groupby("ric", sort=False)["revenue"].pct_change(4, fill_method=None)
            fundamentals["eps_growth_raw"] = fundamentals.groupby("ric", sort=False)["trailing_eps"].pct_change(4, fill_method=None)
            fundamentals["asset_growth_raw"] = fundamentals.groupby("ric", sort=False)["total_assets"].pct_change(4, fill_method=None)
        logger.info(
            "Loaded fundamentals for raw cross-section: rows=%s rics=%s",
            len(fundamentals),
            fundamentals["ric"].nunique() if not fundamentals.empty else 0,
        )
        if progress_callback is not None:
            progress_callback(
                {
                    "message": f"Loaded PIT fundamentals for {fundamentals['ric'].nunique() if not fundamentals.empty else 0} securities",
                    "items_processed": 2,
                    "items_total": 4,
                    "unit": "setup_steps",
                    "progress_pct": 50.0,
                    "progress_kind": "setup",
                }
            )

        trbc_cols = _table_columns(conn, CLASSIFICATION_TABLE)
        trbc_ind_col = pick_trbc_industry_column(trbc_cols)
        trbc_biz_col = pick_trbc_business_sector_column(trbc_cols)
        trbc_sec_col = "trbc_economic_sector"
        trbc = pd.DataFrame()
        if trbc_biz_col and trbc_sec_col in trbc_cols:
            trbc = pd.read_sql_query(
                f"""
                SELECT
                    UPPER(c.ric) AS ric,
                    c.as_of_date,
                    c.{trbc_sec_col} AS trbc_economic_sector_short,
                    c.{trbc_biz_col} AS trbc_business_sector,
                    {f"c.{trbc_ind_col}" if trbc_ind_col else "NULL"} AS trbc_industry_group,
                    COALESCE(UPPER(TRIM(c.hq_country_code)), '') AS hq_country_code
                FROM {CLASSIFICATION_TABLE} c
                WHERE c.as_of_date <= ?
                  AND UPPER(c.ric) IN ({runtime_placeholders})
                ORDER BY UPPER(c.ric), c.as_of_date
                """,
                conn,
                params=(max_date, *runtime_rics),
            )
            if not trbc.empty:
                trbc["ric"] = trbc["ric"].astype(str).str.upper()
                trbc = trbc.merge(runtime_union_identity, on="ric", how="left")
                trbc["ticker"] = trbc["ticker"].astype(str).str.upper()
                trbc["as_of_date"] = trbc["as_of_date"].astype(str)
                trbc["as_of_date_dt"] = pd.to_datetime(trbc["as_of_date"], errors="coerce")
                trbc = trbc.dropna(subset=["as_of_date_dt"])
                trbc["trbc_economic_sector_short"] = _normalize_text(trbc["trbc_economic_sector_short"])
                trbc["trbc_industry_group"] = _normalize_text(trbc.get("trbc_industry_group"))
                trbc["trbc_business_sector"] = _normalize_text(trbc["trbc_business_sector"])
                trbc["hq_country_code"] = _normalize_text(trbc["hq_country_code"]).str.upper()
                trbc = trbc.sort_values(["ric", "as_of_date"]).drop_duplicates(
                    subset=["ric", "as_of_date"], keep="last"
                )
        logger.info(
            "Loaded TRBC PIT classification for raw cross-section: rows=%s rics=%s biz_col=%s ind_col=%s",
            len(trbc),
            trbc["ric"].nunique() if not trbc.empty else 0,
            trbc_biz_col,
            trbc_ind_col,
        )
        if progress_callback is not None:
            progress_callback(
                {
                    "message": f"Loaded TRBC classification for {trbc['ric'].nunique() if not trbc.empty else 0} securities",
                    "items_processed": 3,
                    "items_total": 4,
                    "unit": "setup_steps",
                    "progress_pct": 75.0,
                    "progress_kind": "setup",
                }
            )

        out = base.copy()
        if not fundamentals.empty:
            fmerge_cols = [
                "ric",
                "ticker",
                "fetch_date_dt",
                "market_cap",
                "shares_outstanding",
                "dividend_yield",
                "book_value",
                "forward_eps",
                "trailing_eps",
                "total_debt",
                "operating_cashflow",
                "revenue",
                "total_assets",
                "return_on_equity",
                "operating_margins",
                "net_income",
                "common_name",
                "sales_growth_raw",
                "eps_growth_raw",
                "asset_growth_raw",
            ]
            fmerge_cols = [c for c in fmerge_cols if c in fundamentals.columns]
            out = _merge_asof_by_ric(
                out,
                fundamentals[fmerge_cols],
                left_date_col="as_of_date_dt",
                right_date_col="fetch_date_dt",
            )
        if not trbc.empty:
            out = _merge_asof_by_ric(
                out,
                trbc[
                    [
                        "ric",
                        "ticker",
                        "as_of_date_dt",
                        "trbc_economic_sector_short",
                        "trbc_business_sector",
                        "trbc_industry_group",
                        "hq_country_code",
                    ]
                ],
                left_date_col="as_of_date_dt",
                right_date_col="as_of_date_dt",
            )

        out["ric"] = out["ric"].astype(str).str.upper()
        out["ticker"] = out["ticker"].astype(str).str.upper()
        out["trbc_economic_sector_short"] = _normalize_text(out.get("trbc_economic_sector_short", pd.Series(index=out.index)))
        out["trbc_business_sector"] = _normalize_text(out.get("trbc_business_sector", pd.Series(index=out.index)))
        out["trbc_industry_group"] = _normalize_text(out.get("trbc_industry_group", pd.Series(index=out.index)))
        out["hq_country_code"] = _normalize_text(out.get("hq_country_code", pd.Series(index=out.index))).str.upper()

        # Rebuild beta against a lagged-cap market proxy after PIT market caps are merged.
        out = out.sort_values(["ric", "as_of_date"]).reset_index(drop=True)
        out["ret_1d"] = pd.to_numeric(out.get("ret_1d"), errors="coerce")
        out["market_cap"] = pd.to_numeric(out.get("market_cap"), errors="coerce")
        out["price_close"] = pd.to_numeric(out.get("price_close"), errors="coerce")
        out["lagged_market_cap"] = out.groupby("ric", sort=False)["market_cap"].shift(1)
        valid_beta_proxy = (
            np.isfinite(out["ret_1d"].to_numpy(dtype=float))
            & np.isfinite(out["lagged_market_cap"].to_numpy(dtype=float))
            & (out["lagged_market_cap"].to_numpy(dtype=float) > 0.0)
            & np.isfinite(out["price_close"].to_numpy(dtype=float))
            & (out["price_close"].to_numpy(dtype=float) >= 5.0)
        )
        proxy_df = out.loc[valid_beta_proxy, ["as_of_date", "ret_1d", "lagged_market_cap"]].copy()
        if not proxy_df.empty:
            proxy_df["weighted_ret"] = proxy_df["ret_1d"] * proxy_df["lagged_market_cap"]
            market_proxy = proxy_df.groupby("as_of_date", sort=False).agg(
                total_weighted_ret=("weighted_ret", "sum"),
                total_cap=("lagged_market_cap", "sum"),
            )
            market_proxy["beta_market_ret"] = market_proxy["total_weighted_ret"] / market_proxy["total_cap"].replace(0.0, np.nan)
            out = out.merge(
                market_proxy[["beta_market_ret"]].reset_index(),
                on="as_of_date",
                how="left",
            )

            def _rolling_beta_from_proxy(grp: pd.DataFrame) -> pd.Series:
                ri = pd.to_numeric(grp["ret_1d"], errors="coerce")
                rm = pd.to_numeric(grp["beta_market_ret"], errors="coerce")
                cov = ri.rolling(252, min_periods=60).cov(rm)
                var = rm.rolling(252, min_periods=60).var()
                return cov / var.replace(0.0, np.nan)

            beta_parts: list[pd.Series] = []
            for _, grp in out.groupby("ric", sort=False):
                beta = _rolling_beta_from_proxy(grp)
                beta.index = grp.index
                beta_parts.append(beta)
            out["beta_raw"] = (
                pd.concat(beta_parts).sort_index()
                if beta_parts
                else pd.Series(dtype=float)
            )

        market_cap = pd.to_numeric(out.get("market_cap"), errors="coerce")
        shares_out = pd.to_numeric(out.get("shares_outstanding"), errors="coerce")
        price_close = pd.to_numeric(out.get("price_close"), errors="coerce")
        book_value = pd.to_numeric(out.get("book_value"), errors="coerce")
        total_debt = pd.to_numeric(out.get("total_debt"), errors="coerce")
        operating_cashflow = pd.to_numeric(out.get("operating_cashflow"), errors="coerce")
        total_assets = pd.to_numeric(out.get("total_assets"), errors="coerce")
        gross_profit = pd.to_numeric(out.get("gross_profit"), errors="coerce")
        operating_margins = pd.to_numeric(out.get("operating_margins"), errors="coerce")
        dividend_yield = pd.to_numeric(out.get("dividend_yield"), errors="coerce")
        return_on_equity = pd.to_numeric(out.get("return_on_equity"), errors="coerce")
        avg_volume_21d = pd.to_numeric(out.get("avg_volume_21d"), errors="coerce")
        avg_volume_252d = pd.to_numeric(out.get("avg_volume_252d"), errors="coerce")
        log_adv = pd.to_numeric(out.get("log_avg_dollar_volume_20d_raw"), errors="coerce")

        book_equity = book_value * shares_out
        book_equity = book_equity.where(np.isfinite(book_equity) & (book_equity > 0), np.nan)

        out["size_raw"] = np.log(np.clip(market_cap, 1.0, None))
        out["nonlinear_size_raw"] = out["size_raw"] ** 3
        out["book_to_price_raw"] = book_value / price_close.replace(0.0, np.nan)
        out["forward_ep_raw"] = pd.to_numeric(out.get("forward_eps"), errors="coerce") / price_close.replace(0.0, np.nan)
        out["trailing_ep_raw"] = pd.to_numeric(out.get("trailing_eps"), errors="coerce") / price_close.replace(0.0, np.nan)
        out["cash_earnings_yield_raw"] = operating_cashflow / market_cap.replace(0.0, np.nan)
        out["debt_to_equity_raw"] = total_debt / book_equity
        out["debt_to_assets_raw"] = total_debt / total_assets.replace(0.0, np.nan)
        out["book_leverage_raw"] = total_assets / book_equity
        out["roe_raw"] = return_on_equity
        gross_profitability = gross_profit / total_assets.replace(0.0, np.nan)
        out["gross_profitability_raw"] = gross_profitability.where(
            gross_profitability.notna(),
            operating_margins,
        )
        out["dividend_yield_raw"] = dividend_yield
        out["turnover_1m_raw"] = avg_volume_21d / shares_out.replace(0.0, np.nan)
        out["turnover_12m_raw"] = avg_volume_252d / shares_out.replace(0.0, np.nan)
        out["log_avg_dollar_volume_20d_raw"] = log_adv

        for col in ["beta_raw", "momentum_raw", "st_reversal_raw", "resid_vol_raw", "sales_growth_raw", "eps_growth_raw", "asset_growth_raw"]:
            out[col] = pd.to_numeric(out.get(col), errors="coerce")

        required_desc = sorted(set(_RAW_DESCRIPTOR_COLS))
        for col in required_desc:
            if col not in out.columns:
                out[col] = np.nan

        for sc in _SCORE_COLS:
            out[sc] = np.nan

        # Compute style scores cross-sectionally by as_of_date.
        out = out.sort_values(["as_of_date", "ric"]).reset_index(drop=True)
        date_groups = list(out.groupby("as_of_date", sort=False).groups.items())
        total_groups = len(date_groups)
        logger.info("Computing style scores across %s cross-sections", total_groups)
        if progress_callback is not None:
            progress_callback(
                {
                    "message": f"Computing style scores across {total_groups} cross-sections",
                    "items_processed": 0,
                    "items_total": int(total_groups),
                    "unit": "cross_sections",
                    "progress_pct": 0.0,
                    "progress_kind": "cross_sections",
                }
            )
        for group_i, (as_of, idx) in enumerate(date_groups, start=1):
            loc = list(idx)
            grp = out.loc[loc].copy()
            valid = (
                pd.to_numeric(grp["market_cap"], errors="coerce").gt(0.0)
                & grp["trbc_business_sector"].astype(str).str.strip().ne("")
            )
            if int(valid.sum()) < 30:
                continue
            sub = grp.loc[valid, ["ric", "market_cap", "trbc_business_sector", *required_desc]].copy()
            for col in required_desc:
                sub[col] = pd.to_numeric(sub[col], errors="coerce")
                med = float(sub[col].median()) if sub[col].notna().any() else 0.0
                if not np.isfinite(med):
                    med = 0.0
                sub[col] = sub[col].fillna(med)
            raw = sub.set_index("ric")[["market_cap", *required_desc]]
            industries = sub.set_index("ric")["trbc_business_sector"].astype(str)
            ind_dummies = pd.get_dummies(industries, dtype=float)
            scores = assemble_full_style_scores(
                raw_descriptors=raw,
                industry_exposures=ind_dummies,
            )
            for factor_name, score_col in _FACTOR_TO_SCORE_COL.items():
                if factor_name in scores.columns and score_col in out.columns:
                    fill_series = scores[factor_name].rename("v")
                    merged = grp[["ric"]].merge(
                        fill_series.reset_index(),
                        on="ric",
                        how="left",
                    )["v"]
                    out.loc[loc, score_col] = merged.to_numpy()
            if group_i % 250 == 0 or group_i == total_groups:
                logger.info(
                    "Style-score progress %s/%s cross-sections (as_of=%s)",
                    group_i,
                    total_groups,
                    as_of,
                )
                if progress_callback is not None:
                    progress_callback(
                        {
                            "message": f"Computed style scores through {as_of}",
                            "items_processed": int(group_i),
                            "items_total": int(total_groups),
                            "unit": "cross_sections",
                            "progress_pct": round((float(group_i) / max(1.0, float(total_groups))) * 100.0, 1),
                            "current_as_of": str(as_of),
                            "progress_kind": "cross_sections",
                        }
                    )

        keep_mask = pd.Series(False, index=out.index, dtype=bool)
        eligibility_groups = list(out.groupby("as_of_date", sort=False).groups.items())
        total_eligibility_groups = len(eligibility_groups)
        for group_i, (as_of, idx) in enumerate(eligibility_groups, start=1):
            grp = out.loc[idx]
            exposure_snapshot = grp.set_index("ric")[_SCORE_COLS].copy()
            required_style_cols = [
                column
                for column in _SCORE_COLS
                if column in exposure_snapshot.columns and exposure_snapshot[column].notna().any()
            ]
            market_caps = pd.to_numeric(grp.set_index("ric")["market_cap"], errors="coerce")
            economic_sectors = grp.set_index("ric")["trbc_economic_sector_short"]
            business_sectors = grp.set_index("ric")["trbc_business_sector"]
            industries = grp.set_index("ric")["trbc_industry_group"]
            countries = grp.set_index("ric")["hq_country_code"]
            eligibility_df = structural_eligibility_for_snapshot(
                exposure_snapshot=exposure_snapshot,
                market_caps=market_caps,
                trbc_economic_sector_shorts=economic_sectors,
                trbc_business_sectors=business_sectors,
                trbc_industries=industries,
                hq_country_codes=countries,
                required_style_cols=required_style_cols,
            )
            if eligibility_df.empty:
                if progress_callback is not None and (group_i % 250 == 0 or group_i == total_eligibility_groups):
                    progress_callback(
                        {
                            "message": f"Evaluated structural eligibility through {as_of}",
                            "items_processed": int(group_i),
                            "items_total": int(total_eligibility_groups),
                            "unit": "cross_sections",
                            "progress_pct": round((float(group_i) / max(1.0, float(total_eligibility_groups))) * 100.0, 1),
                            "current_as_of": str(as_of),
                            "progress_kind": "eligibility",
                        }
                    )
                continue
            keep_rics = {
                str(ric).upper()
                for ric in eligibility_df.index[
                    eligibility_df["is_structural_eligible"].astype(bool)
                ]
            }
            if keep_rics:
                keep_mask.loc[idx] = grp["ric"].astype(str).str.upper().isin(keep_rics).to_numpy()
            if progress_callback is not None and (group_i % 250 == 0 or group_i == total_eligibility_groups):
                progress_callback(
                    {
                        "message": f"Evaluated structural eligibility through {as_of}",
                        "items_processed": int(group_i),
                        "items_total": int(total_eligibility_groups),
                        "unit": "cross_sections",
                        "progress_pct": round((float(group_i) / max(1.0, float(total_eligibility_groups))) * 100.0, 1),
                        "current_as_of": str(as_of),
                        "progress_kind": "eligibility",
                    }
                )
        out = out.loc[keep_mask.to_numpy()].copy()
        if out.empty:
            for d in sorted(set(dates)):
                conn.execute(f"DELETE FROM {TABLE} WHERE as_of_date = ?", (d,))
            conn.commit()
            return {"status": "no-structural-eligible-rows", "rows_upserted": 0, "table": TABLE}

        # Quality metadata
        present = out[required_desc].notna().sum(axis=1).astype(float)
        ratio = present / float(len(required_desc))
        out["confidence_band"] = np.where(ratio >= 0.9, "high", np.where(ratio >= 0.75, "medium", "low"))
        out["fallback_depth"] = (len(required_desc) - present).astype(int).astype(str)
        out["coverage_degraded"] = np.where(ratio < 0.75, "1", "0")

        assumption_set_version = f"{frequency}-v1"
        now_iso = datetime.now(timezone.utc).isoformat()
        job_run_id = f"raw_cross_section_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        out["idio_var_daily"] = np.nan
        out["barra_model_version"] = MODEL_VERSION
        out["descriptor_schema_version"] = DESCRIPTOR_VERSION
        out["assumption_set_version"] = assumption_set_version
        out["source"] = "in_project_builder"
        out["job_run_id"] = job_run_id
        out["updated_at"] = now_iso

        target_cols = [
            "ric",
            "ticker",
            "as_of_date",
            "market_cap",
            "price_close",
            "price_volume",
            "shares_outstanding",
            "trbc_economic_sector_short",
            "trbc_business_sector",
            *_RAW_DESCRIPTOR_COLS,
            *_SCORE_COLS,
            "confidence_band",
            "fallback_depth",
            "idio_var_daily",
            "coverage_degraded",
            "barra_model_version",
            "descriptor_schema_version",
            "assumption_set_version",
            "source",
            "job_run_id",
            "updated_at",
        ]
        payload = out[target_cols].where(pd.notna(out[target_cols]), None).copy()
        delete_dates = sorted(set(payload["as_of_date"].astype(str).tolist()))
        logger.info(
            "Persisting raw cross-section payload: rows=%s dates=%s",
            len(payload),
            len(delete_dates),
        )
        if progress_callback is not None:
            progress_callback(
                {
                    "message": f"Persisting {len(payload)} raw cross-section rows",
                    "items_processed": 4,
                    "items_total": 4,
                    "unit": "setup_steps",
                    "progress_pct": 100.0,
                    "rows_upserted": int(len(payload)),
                    "dates_processed": int(len(delete_dates)),
                    "progress_kind": "persist",
                }
            )
        for d in delete_dates:
            conn.execute(f"DELETE FROM {TABLE} WHERE as_of_date = ?", (d,))
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {TABLE}
            ({", ".join(target_cols)})
            VALUES ({", ".join(["?"] * len(target_cols))})
            """,
            payload.itertuples(index=False, name=None),
        )
        conn.commit()
        elapsed = time.perf_counter() - stage_t0
        logger.info("Raw cross-section rebuild completed in %.1fs", elapsed)
        return {
            "status": "ok",
            "table": TABLE,
            "rows_upserted": int(len(payload)),
            "dates_processed": int(len(delete_dates)),
            "assumption_set_version": assumption_set_version,
            "job_run_id": job_run_id,
        }
    finally:
        conn.close()
