"""Build in-project Barra raw cross-section history and style scores."""

from __future__ import annotations

import logging
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.barra.descriptors import assemble_full_style_scores
from backend.barra.risk_attribution import STYLE_COLUMN_TO_LABEL
from backend.db.trbc_schema import (
    ensure_trbc_naming,
    pick_trbc_business_sector_column,
    pick_trbc_industry_column,
)
from backend.trading_calendar import filter_xnys_sessions

logger = logging.getLogger(__name__)

TABLE = "barra_raw_cross_section_history"
SECURITY_MASTER_TABLE = "security_master"
PRICES_TABLE = "security_prices_eod"
FUNDAMENTALS_TABLE = "security_fundamentals_pit"
CLASSIFICATION_TABLE = "security_classification_pit"
MODEL_VERSION = "inproj-v1"
DESCRIPTOR_VERSION = "raw-cross-section-v1"

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
            trbc_industry_group TEXT,
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
            value_score REAL,
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
    if expected.issubset(cols) and pk_cols == ["ric", "as_of_date"]:
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

    market_ret = out.groupby("date", sort=False)["ret_1d"].mean().rename("market_ret")
    out = out.merge(market_ret, on="date", how="left")

    def _rolling_beta(grp: pd.DataFrame) -> pd.Series:
        ri = pd.to_numeric(grp["ret_1d"], errors="coerce")
        rm = pd.to_numeric(grp["market_ret"], errors="coerce")
        cov = ri.rolling(252, min_periods=60).cov(rm)
        var = rm.rolling(252, min_periods=60).var()
        beta = cov / var.replace(0.0, np.nan)
        return beta

    out["beta_raw"] = (
        out.groupby("ric", sort=False, group_keys=False)[["ret_1d", "market_ret"]]
        .apply(_rolling_beta)
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


def rebuild_raw_cross_section_history(
    data_db: Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    frequency: str = "weekly",
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
        if not dates:
            return {"status": "no-dates", "rows_upserted": 0, "table": TABLE}

        min_for_roll = (
            pd.to_datetime(dates[0], errors="coerce") - timedelta(days=420)
        ).date().isoformat()
        max_date = dates[-1]

        prices_raw = pd.read_sql_query(
            f"""
            SELECT UPPER(p.ric) AS ric, UPPER(sm.ticker) AS ticker, p.date, p.close, p.volume, p.source, p.updated_at
            FROM {PRICES_TABLE} p
            JOIN {SECURITY_MASTER_TABLE} sm
              ON sm.ric = p.ric
            WHERE p.date >= ? AND p.date <= ?
            ORDER BY UPPER(p.ric), p.date
            """,
            conn,
            params=(min_for_roll, max_date),
        )
        prices = _dedupe_prices(prices_raw)
        prices = _compute_price_features(prices)
        logger.info(
            "Loaded price history for raw cross-section: rows=%s rics=%s",
            len(prices),
            prices["ric"].nunique() if not prices.empty else 0,
        )
        if prices.empty:
            return {"status": "no-prices", "rows_upserted": 0, "table": TABLE}

        target_set = set(dates)
        base = prices.loc[prices["date"].isin(target_set)].copy()
        base = base.rename(columns={"date": "as_of_date", "close": "price_close", "volume": "price_volume"})
        base["as_of_date_dt"] = pd.to_datetime(base["as_of_date"], errors="coerce")
        base = base.dropna(subset=["as_of_date_dt"])
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
                SELECT UPPER(f.ric) AS ric, UPPER(sm.ticker) AS ticker, f.as_of_date AS fetch_date, {fselect_cols}
                FROM {FUNDAMENTALS_TABLE} f
                JOIN {SECURITY_MASTER_TABLE} sm
                  ON sm.ric = f.ric
                WHERE f.as_of_date <= ?
                ORDER BY UPPER(f.ric), f.as_of_date
                """,
                conn,
                params=(max_date,),
            )
        if not fundamentals.empty:
            fundamentals["ric"] = fundamentals["ric"].astype(str).str.upper()
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
                    UPPER(sm.ticker) AS ticker,
                    c.as_of_date,
                    c.{trbc_sec_col} AS trbc_economic_sector_short,
                    c.{trbc_biz_col} AS trbc_industry_group,
                    {f"c.{trbc_ind_col}" if trbc_ind_col else "NULL"} AS trbc_industry_group_l3
                FROM {CLASSIFICATION_TABLE} c
                JOIN {SECURITY_MASTER_TABLE} sm
                  ON sm.ric = c.ric
                WHERE c.as_of_date <= ?
                ORDER BY UPPER(c.ric), c.as_of_date
                """,
                conn,
                params=(max_date,),
            )
            if not trbc.empty:
                trbc["ric"] = trbc["ric"].astype(str).str.upper()
                trbc["ticker"] = trbc["ticker"].astype(str).str.upper()
                trbc["as_of_date"] = trbc["as_of_date"].astype(str)
                trbc["as_of_date_dt"] = pd.to_datetime(trbc["as_of_date"], errors="coerce")
                trbc = trbc.dropna(subset=["as_of_date_dt"])
                trbc["trbc_economic_sector_short"] = _normalize_text(trbc["trbc_economic_sector_short"])
                trbc["trbc_industry_group_l3"] = _normalize_text(trbc.get("trbc_industry_group_l3"))
                trbc["trbc_industry_group"] = _normalize_text(trbc["trbc_industry_group"])
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
                trbc[["ric", "ticker", "as_of_date_dt", "trbc_economic_sector_short", "trbc_industry_group"]],
                left_date_col="as_of_date_dt",
                right_date_col="as_of_date_dt",
            )

        out["ric"] = out["ric"].astype(str).str.upper()
        out["ticker"] = out["ticker"].astype(str).str.upper()
        out["trbc_economic_sector_short"] = _normalize_text(out.get("trbc_economic_sector_short", pd.Series(index=out.index)))
        out["trbc_industry_group"] = _normalize_text(out.get("trbc_industry_group", pd.Series(index=out.index)))

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
        for group_i, (as_of, idx) in enumerate(date_groups, start=1):
            loc = list(idx)
            grp = out.loc[loc].copy()
            valid = (
                pd.to_numeric(grp["market_cap"], errors="coerce").gt(0.0)
                & grp["trbc_industry_group"].astype(str).str.strip().ne("")
            )
            if int(valid.sum()) < 30:
                continue
            sub = grp.loc[valid, ["ric", "market_cap", "trbc_industry_group", *required_desc]].copy()
            for col in required_desc:
                sub[col] = pd.to_numeric(sub[col], errors="coerce")
                med = float(sub[col].median()) if sub[col].notna().any() else 0.0
                if not np.isfinite(med):
                    med = 0.0
                sub[col] = sub[col].fillna(med)
            raw = sub.set_index("ric")[["market_cap", *required_desc]]
            industries = sub.set_index("ric")["trbc_industry_group"].astype(str)
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
            "trbc_industry_group",
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
