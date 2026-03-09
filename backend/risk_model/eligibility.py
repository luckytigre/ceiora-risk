"""Centralized universe eligibility logic for Barra cross-sections."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from backend.risk_model.risk_attribution import STYLE_COLUMN_TO_LABEL
from backend.data.trbc_schema import (
    ensure_trbc_naming,
    pick_trbc_business_sector_column,
    pick_trbc_industry_column,
)

NON_EQUITY_ECONOMIC_SECTORS = {
    "Exchange Traded Fund",
    "Digital Asset",
}


@dataclass(frozen=True)
class EligibilityContext:
    exposure_dates: list[str]
    exposure_snapshots: dict[str, pd.DataFrame]
    market_cap_panel: pd.DataFrame
    trbc_economic_sector_short_panel: pd.DataFrame
    trbc_business_sector_panel: pd.DataFrame
    trbc_industry_panel: pd.DataFrame
    hq_country_code_panel: pd.DataFrame
    dates: list[str]


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


def _normalize_text_series(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .replace({"nan": "", "None": "", "Unmapped": "", "unmapped": ""})
    )


def _pick_trbc_economic_sector_short_column(columns: Iterable[str]) -> str | None:
    cols = set(columns)
    for col in ("trbc_economic_sector_short", "trbc_sector", "trbc_economic_sector", "sector"):
        if col in cols:
            return col
    return None


def most_recent_date(sorted_dates: list[str], target: str) -> str | None:
    """Binary search for max(sorted_dates) <= target."""
    if not sorted_dates:
        return None
    lo, hi = 0, len(sorted_dates) - 1
    out: str | None = None
    while lo <= hi:
        mid = (lo + hi) // 2
        cur = sorted_dates[mid]
        if cur <= target:
            out = cur
            lo = mid + 1
        else:
            hi = mid - 1
    return out


def load_trading_dates(data_db: Path) -> list[str]:
    conn = sqlite3.connect(str(data_db))
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT date
            FROM security_prices_eod
            WHERE date IS NOT NULL
            ORDER BY date
            """
        ).fetchall()
    finally:
        conn.close()
    return [str(r[0]) for r in rows if r and r[0]]


def load_exposure_snapshots(
    data_db: Path,
    *,
    dates: list[str] | None = None,
) -> tuple[list[str], dict[str, pd.DataFrame]]:
    """Load exposure snapshots keyed by as_of_date (RIC-indexed)."""
    conn = sqlite3.connect(str(data_db))
    try:
        ensure_trbc_naming(conn)
        source_table = "barra_raw_cross_section_history"
        cols = _table_columns(conn, source_table)
        if not {"ric", "as_of_date"}.issubset(cols):
            return [], {}
        style_cols = [c for c in STYLE_COLUMN_TO_LABEL.keys() if c in cols]
        industry_col = pick_trbc_industry_column(cols)
        industry_select = f"{industry_col} AS trbc_industry_group" if industry_col else "NULL AS trbc_industry_group"
        requested_dates = sorted({str(d) for d in (dates or []) if str(d).strip()})
        params: list[str] = []
        lower_bound: str | None = None
        upper_bound: str | None = None
        if requested_dates:
            upper_bound = requested_dates[-1]
            snapshot_rows = conn.execute(
                f"""
                SELECT DISTINCT as_of_date
                FROM {source_table}
                WHERE as_of_date <= ?
                ORDER BY as_of_date
                """,
                (upper_bound,),
            ).fetchall()
            snapshot_dates = [str(row[0]) for row in snapshot_rows if row and row[0]]
            lower_bound = most_recent_date(snapshot_dates, requested_dates[0])
            if lower_bound is None:
                return [], {}
            params.extend([lower_bound, upper_bound])
            where_clause = "WHERE as_of_date >= ? AND as_of_date <= ?"
        else:
            where_clause = ""
        df = pd.read_sql_query(
            f"""
            SELECT UPPER(ric) AS ric, UPPER(ticker) AS ticker, as_of_date, {", ".join(style_cols)}, {industry_select}
            FROM {source_table}
            {where_clause}
            ORDER BY as_of_date, ric
            """,
            conn,
            params=params,
        )
    finally:
        conn.close()
    if df.empty:
        return [], {}

    df["ric"] = df["ric"].astype(str).str.upper()
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["as_of_date"] = df["as_of_date"].astype(str)
    if "trbc_industry_group" in df.columns:
        df["trbc_industry_group"] = _normalize_text_series(df["trbc_industry_group"])
    for col in style_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    snapshots: dict[str, pd.DataFrame] = {}
    for as_of, grp in df.groupby("as_of_date", sort=True):
        snap = grp.drop_duplicates(subset=["ric"], keep="last").set_index("ric")
        keep_cols = [*style_cols]
        if "ticker" in snap.columns:
            keep_cols.append("ticker")
        if "trbc_industry_group" in snap.columns:
            keep_cols.append("trbc_industry_group")
        snapshots[str(as_of)] = snap[keep_cols].copy()
    return sorted(snapshots.keys()), snapshots


def _load_market_cap_panel(data_db: Path, dates: list[str]) -> pd.DataFrame:
    if not dates:
        return pd.DataFrame()
    conn = sqlite3.connect(str(data_db))
    try:
        df = pd.read_sql_query(
            """
            SELECT UPPER(f.ric) AS ric, f.as_of_date AS fetch_date, f.market_cap
            FROM security_fundamentals_pit f
            WHERE f.as_of_date <= ?
            ORDER BY f.as_of_date, UPPER(f.ric)
            """,
            conn,
            params=(dates[-1],),
        )
    finally:
        conn.close()
    if df.empty:
        return pd.DataFrame(index=dates)
    df["ric"] = df["ric"].astype(str).str.upper()
    df["fetch_date"] = df["fetch_date"].astype(str)
    df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")
    wide = (
        df.dropna(subset=["ric", "fetch_date"])
        .drop_duplicates(subset=["ric", "fetch_date"], keep="last")
        .pivot(index="fetch_date", columns="ric", values="market_cap")
        .sort_index()
    )
    full_index = sorted(set(wide.index.astype(str)).union(set(dates)))
    wide = wide.reindex(full_index).ffill()
    return wide.reindex(dates)


def _load_trbc_classification_panel(
    data_db: Path,
    dates: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not dates:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    conn = sqlite3.connect(str(data_db))
    try:
        ensure_trbc_naming(conn)
        parts: list[pd.DataFrame] = []

        # Historical TRBC table (single source of truth for point-in-time classes).
        if _table_exists(conn, "security_classification_pit"):
            hcols = _table_columns(conn, "security_classification_pit")
            h_biz_col = pick_trbc_business_sector_column(hcols)
            h_ind_col = pick_trbc_industry_column(hcols)
            h_sec_col = _pick_trbc_economic_sector_short_column(hcols)
            if h_biz_col and h_sec_col:
                ind_expr = f"h.{h_ind_col}" if h_ind_col else "NULL"
                hist = pd.read_sql_query(
                    f"""
                    SELECT UPPER(h.ric) AS ric,
                           h.as_of_date AS ref_date,
                           h.{h_sec_col} AS trbc_economic_sector_short,
                           h.{h_biz_col} AS trbc_business_sector,
                           {ind_expr} AS trbc_industry_group,
                           COALESCE(UPPER(TRIM(h.hq_country_code)), '') AS hq_country_code
                    FROM security_classification_pit h
                    WHERE h.as_of_date <= ?
                    """,
                    conn,
                    params=(dates[-1],),
                )
                if not hist.empty:
                    hist["priority"] = 2
                    parts.append(hist)
    finally:
        conn.close()

    if not parts:
        empty = pd.DataFrame(index=dates)
        return empty, empty.copy(), empty.copy(), empty.copy()

    df = pd.concat(parts, ignore_index=True)
    df["ric"] = df["ric"].astype(str).str.upper()
    df["ref_date"] = df["ref_date"].astype(str)
    df["trbc_economic_sector_short"] = _normalize_text_series(df["trbc_economic_sector_short"])
    df["trbc_business_sector"] = _normalize_text_series(df["trbc_business_sector"])
    df["trbc_industry_group"] = _normalize_text_series(df["trbc_industry_group"])
    df["hq_country_code"] = _normalize_text_series(df["hq_country_code"]).str.upper()
    df["trbc_economic_sector_short"] = df["trbc_economic_sector_short"].replace({"": np.nan})
    df["trbc_business_sector"] = df["trbc_business_sector"].replace({"": np.nan})
    df["trbc_industry_group"] = df["trbc_industry_group"].replace({"": np.nan})
    df["hq_country_code"] = df["hq_country_code"].replace({"": np.nan})
    df = df.dropna(subset=["ric", "ref_date"])
    df = (
        df.sort_values(["ref_date", "priority"])
        .drop_duplicates(subset=["ric", "ref_date"], keep="last")
    )

    sec = (
        df.pivot(index="ref_date", columns="ric", values="trbc_economic_sector_short")
        .sort_index()
    )
    biz = (
        df.pivot(index="ref_date", columns="ric", values="trbc_business_sector")
        .sort_index()
    )
    ind = (
        df.pivot(index="ref_date", columns="ric", values="trbc_industry_group")
        .sort_index()
    )
    country = (
        df.pivot(index="ref_date", columns="ric", values="hq_country_code")
        .sort_index()
    )
    sec = sec.astype("string")
    biz = biz.astype("string")
    ind = ind.astype("string")
    country = country.astype("string")
    full_index = sorted(set(sec.index.astype(str)).union(set(dates)))
    with pd.option_context("future.no_silent_downcasting", True):
        sec = sec.reindex(full_index).ffill().reindex(dates)
    full_index = sorted(set(biz.index.astype(str)).union(set(dates)))
    with pd.option_context("future.no_silent_downcasting", True):
        biz = biz.reindex(full_index).ffill().reindex(dates)
    full_index = sorted(set(ind.index.astype(str)).union(set(dates)))
    with pd.option_context("future.no_silent_downcasting", True):
        ind = ind.reindex(full_index).ffill().reindex(dates)
    full_index = sorted(set(country.index.astype(str)).union(set(dates)))
    with pd.option_context("future.no_silent_downcasting", True):
        country = country.reindex(full_index).ffill().reindex(dates)
    return sec, biz, ind, country


def _load_panels_from_cross_section_snapshot(
    data_db: Path,
    dates: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not dates:
        empty = pd.DataFrame()
        return empty, empty, empty, empty
    conn = sqlite3.connect(str(data_db))
    try:
        if not _table_exists(conn, "universe_cross_section_snapshot"):
            empty = pd.DataFrame()
            return empty, empty, empty, empty
        cols = _table_columns(conn, "universe_cross_section_snapshot")
        sector_expr_parts: list[str] = []
        if "trbc_economic_sector_short" in cols:
            sector_expr_parts.append("NULLIF(trbc_economic_sector_short, '')")
        if "trbc_sector" in cols:
            sector_expr_parts.append("NULLIF(trbc_sector, '')")
        if "trbc_economic_sector" in cols:
            sector_expr_parts.append("NULLIF(trbc_economic_sector, '')")
        sector_expr = (
            f"COALESCE({', '.join(sector_expr_parts)}, '') AS trbc_economic_sector_short"
            if sector_expr_parts
            else "'' AS trbc_economic_sector_short"
        )
        business_expr_parts: list[str] = []
        if "trbc_business_sector" in cols:
            business_expr_parts.append("NULLIF(trbc_business_sector, '')")
        if "business_sector" in cols:
            business_expr_parts.append("NULLIF(business_sector, '')")
        business_expr = (
            f"COALESCE({', '.join(business_expr_parts)}, '') AS trbc_business_sector"
            if business_expr_parts
            else "'' AS trbc_business_sector"
        )
        df = pd.read_sql_query(
            f"""
            SELECT
                ticker,
                as_of_date AS ref_date,
                market_cap,
                {sector_expr},
                {business_expr},
                trbc_industry_group
            FROM universe_cross_section_snapshot
            WHERE as_of_date <= ?
            ORDER BY as_of_date, ticker
            """,
            conn,
            params=(dates[-1],),
        )
    finally:
        conn.close()
    if df.empty:
        empty = pd.DataFrame()
        return empty, empty, empty, empty

    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["ref_date"] = df["ref_date"].astype(str)
    df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")
    df["trbc_economic_sector_short"] = _normalize_text_series(df["trbc_economic_sector_short"]).replace({"": np.nan})
    df["trbc_business_sector"] = _normalize_text_series(df["trbc_business_sector"]).replace({"": np.nan})
    df["trbc_industry_group"] = _normalize_text_series(df["trbc_industry_group"]).replace({"": np.nan})

    market = df.pivot(index="ref_date", columns="ticker", values="market_cap").sort_index()
    sec = df.pivot(index="ref_date", columns="ticker", values="trbc_economic_sector_short").sort_index()
    biz = df.pivot(index="ref_date", columns="ticker", values="trbc_business_sector").sort_index()
    ind = df.pivot(index="ref_date", columns="ticker", values="trbc_industry_group").sort_index()

    def _ffill_to_dates(panel: pd.DataFrame) -> pd.DataFrame:
        full_index = sorted(set(panel.index.astype(str)).union(set(dates)))
        # Opt into pandas' future behavior now to avoid silent downcasting warnings.
        with pd.option_context("future.no_silent_downcasting", True):
            filled = panel.reindex(full_index).ffill()
        return filled.reindex(dates)

    market = _ffill_to_dates(market)
    sec = _ffill_to_dates(sec)
    biz = _ffill_to_dates(biz)
    ind = _ffill_to_dates(ind)
    return market, sec, biz, ind


def build_eligibility_context(
    data_db: Path,
    *,
    dates: list[str] | None = None,
) -> EligibilityContext:
    exposure_dates, snapshots = load_exposure_snapshots(data_db, dates=dates)
    if dates is None:
        trading_dates = load_trading_dates(data_db)
        # Include exposure dates so non-trading snapshot dates still resolve.
        merged_dates = sorted(set(trading_dates).union(exposure_dates))
    else:
        merged_dates = sorted(set(str(d) for d in dates))

    market_cap_panel = _load_market_cap_panel(data_db, merged_dates)
    sector_panel, business_sector_panel, industry_panel, country_panel = _load_trbc_classification_panel(data_db, merged_dates)
    if market_cap_panel.empty or sector_panel.empty or business_sector_panel.empty or industry_panel.empty:
        snap_market, snap_sector, snap_business, snap_industry = _load_panels_from_cross_section_snapshot(
            data_db,
            merged_dates,
        )
        if market_cap_panel.empty:
            market_cap_panel = snap_market
        if sector_panel.empty:
            sector_panel = snap_sector
        if business_sector_panel.empty:
            business_sector_panel = snap_business
        if industry_panel.empty:
            industry_panel = snap_industry
    return EligibilityContext(
        exposure_dates=exposure_dates,
        exposure_snapshots=snapshots,
        market_cap_panel=market_cap_panel,
        trbc_economic_sector_short_panel=sector_panel,
        trbc_business_sector_panel=business_sector_panel,
        trbc_industry_panel=industry_panel,
        hq_country_code_panel=country_panel,
        dates=merged_dates,
    )


def _panel_row(panel: pd.DataFrame, date_key: str) -> pd.Series:
    if panel.empty:
        return pd.Series(dtype=object)
    if date_key in panel.index:
        return panel.loc[date_key]
    prev = most_recent_date([str(d) for d in panel.index.astype(str).tolist()], date_key)
    if prev is None:
        return pd.Series(dtype=object)
    return panel.loc[prev]


def structural_eligibility_for_snapshot(
    *,
    exposure_snapshot: pd.DataFrame,
    market_caps: pd.Series,
    trbc_economic_sector_shorts: pd.Series,
    trbc_business_sectors: pd.Series,
    trbc_industries: pd.Series,
    hq_country_codes: pd.Series,
    required_style_cols: list[str] | None = None,
    non_equity_sectors: set[str] | None = None,
) -> pd.DataFrame:
    """Return per-security structural eligibility booleans and reasons."""
    if exposure_snapshot is None or exposure_snapshot.empty:
        return pd.DataFrame()

    non_equity = set(non_equity_sectors or NON_EQUITY_ECONOMIC_SECTORS)
    style_cols = required_style_cols or list(STYLE_COLUMN_TO_LABEL.keys())
    idx = pd.Index(exposure_snapshot.index.astype(str).str.upper(), name="ric")
    frame = pd.DataFrame(index=idx)

    if style_cols and all(c in exposure_snapshot.columns for c in style_cols):
        s = exposure_snapshot.reindex(columns=style_cols)
        finite = s.apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan)
        frame["has_all_style"] = finite.notna().all(axis=1)
    else:
        frame["has_all_style"] = False

    caps = pd.to_numeric(market_caps, errors="coerce").reindex(idx)
    frame["has_market_cap"] = caps.notna() & np.isfinite(caps) & (caps > 0.0)

    sec = _normalize_text_series(pd.Series(trbc_economic_sector_shorts, index=trbc_economic_sector_shorts.index)).reindex(idx)
    biz = _normalize_text_series(pd.Series(trbc_business_sectors, index=trbc_business_sectors.index)).reindex(idx)
    ind = _normalize_text_series(pd.Series(trbc_industries, index=trbc_industries.index)).reindex(idx)
    country = _normalize_text_series(pd.Series(hq_country_codes, index=hq_country_codes.index)).str.upper().reindex(idx)
    frame["trbc_economic_sector_short"] = sec.fillna("")
    frame["trbc_business_sector"] = biz.fillna("")
    frame["trbc_industry_group"] = ind.fillna("")
    frame["hq_country_code"] = country.fillna("")
    frame["has_trbc_economic_sector_short"] = frame["trbc_economic_sector_short"].str.len() > 0
    frame["has_trbc_business_sector"] = frame["trbc_business_sector"].str.len() > 0
    frame["has_trbc_industry"] = frame["trbc_industry_group"].str.len() > 0
    frame["has_hq_country_code"] = frame["hq_country_code"].str.len() > 0
    frame["is_non_equity"] = frame["trbc_economic_sector_short"].isin(non_equity)

    frame["is_structural_eligible"] = (
        frame["has_all_style"]
        & frame["has_market_cap"]
        & frame["has_trbc_economic_sector_short"]
        & frame["has_trbc_business_sector"]
        & frame["has_hq_country_code"]
        & ~frame["is_non_equity"]
    )

    def _reasons(row: pd.Series) -> str:
        out: list[str] = []
        if not bool(row.get("has_all_style", False)):
            out.append("missing_style")
        if not bool(row.get("has_market_cap", False)):
            out.append("missing_market_cap")
        if not bool(row.get("has_trbc_economic_sector_short", False)):
            out.append("missing_trbc_economic_sector_short")
        # Keep legacy reason token for downstream compatibility, but gate on L2 business sector.
        if not bool(row.get("has_trbc_business_sector", False)):
            out.append("missing_trbc_industry")
        if not bool(row.get("has_hq_country_code", False)):
            out.append("missing_country")
        if bool(row.get("is_non_equity", False)):
            out.append("non_equity")
        return "|".join(out)

    frame["exclusion_reason"] = frame.apply(_reasons, axis=1)
    frame["market_cap"] = caps.reindex(idx)
    return frame


def structural_eligibility_for_date(
    context: EligibilityContext,
    date_key: str,
) -> tuple[str | None, pd.DataFrame]:
    """Resolve exposure snapshot <= date and compute structural eligibility."""
    if not context.exposure_dates:
        return None, pd.DataFrame()
    exp_date = most_recent_date(context.exposure_dates, str(date_key))
    if exp_date is None:
        return None, pd.DataFrame()
    snap = context.exposure_snapshots.get(exp_date)
    if snap is None or snap.empty:
        return exp_date, pd.DataFrame()

    mcap_row = _panel_row(context.market_cap_panel, str(date_key))
    sector_row = _panel_row(context.trbc_economic_sector_short_panel, str(date_key))
    business_sector_row = _panel_row(context.trbc_business_sector_panel, str(date_key))
    industry_row = _panel_row(context.trbc_industry_panel, str(date_key))
    country_row = _panel_row(context.hq_country_code_panel, str(date_key))
    elig = structural_eligibility_for_snapshot(
        exposure_snapshot=snap,
        market_caps=mcap_row,
        trbc_economic_sector_shorts=sector_row,
        trbc_business_sectors=business_sector_row,
        trbc_industries=industry_row,
        hq_country_codes=country_row,
    )
    return exp_date, elig
