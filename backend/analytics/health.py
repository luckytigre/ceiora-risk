"""Model health diagnostics for the Health dashboard."""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from barra.daily_factor_returns import load_specific_residuals
from barra.descriptors import FULL_STYLE_FACTORS, FULL_STYLE_ORTH_RULES, canonicalize_style_scores
from barra.risk_attribution import STYLE_COLUMN_TO_LABEL
from db.sqlite import cache_get
from db.trbc_schema import ensure_trbc_naming, pick_trbc_industry_column

ANNUALIZATION = 252.0


def _to_date_str(dt: Any) -> str:
    return str(pd.to_datetime(dt).date())


def _hist(values: np.ndarray, *, bins: int = 40, lo: float | None = None, hi: float | None = None) -> dict[str, list[float | int]]:
    x = np.asarray(values, dtype=float)
    x = x[np.isfinite(x)]
    if x.size == 0:
        return {"centers": [], "counts": []}
    if lo is None:
        lo = float(np.nanpercentile(x, 1))
    if hi is None:
        hi = float(np.nanpercentile(x, 99))
    if not np.isfinite(lo):
        lo = float(np.nanmin(x))
    if not np.isfinite(hi):
        hi = float(np.nanmax(x))
    if hi <= lo:
        lo = float(lo) - 1.0
        hi = float(hi) + 1.0
    counts, edges = np.histogram(x, bins=bins, range=(lo, hi))
    centers = ((edges[:-1] + edges[1:]) * 0.5).tolist()
    return {
        "centers": [float(c) for c in centers],
        "counts": [int(c) for c in counts.tolist()],
    }


def _load_daily_factor_returns(cache_db: Path, *, years: int = 10) -> pd.DataFrame:
    conn = sqlite3.connect(str(cache_db))
    try:
        latest_row = conn.execute("SELECT MAX(date) FROM daily_factor_returns").fetchone()
        latest = latest_row[0] if latest_row and latest_row[0] else None
        if latest is None:
            return pd.DataFrame()
        latest_dt = date.fromisoformat(str(latest))
        start_dt = latest_dt - timedelta(days=365 * years)
        df = pd.read_sql_query(
            """
            SELECT date, factor_name, factor_return, r_squared, residual_vol
            FROM daily_factor_returns
            WHERE date >= ?
            ORDER BY date, factor_name
            """,
            conn,
            params=(start_dt.isoformat(),),
        )
    finally:
        conn.close()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["factor_name"] = df["factor_name"].astype(str)
    df["factor_return"] = pd.to_numeric(df["factor_return"], errors="coerce").fillna(0.0)
    df["r_squared"] = pd.to_numeric(df["r_squared"], errors="coerce").fillna(0.0)
    df["residual_vol"] = pd.to_numeric(df["residual_vol"], errors="coerce").fillna(0.0)
    return df.dropna(subset=["date"])


def _week_end_sample_dates(dates: pd.Series) -> set[pd.Timestamp]:
    d = pd.to_datetime(dates, errors="coerce").dropna().dt.normalize()
    if d.empty:
        return set()
    tmp = pd.DataFrame({"date": d})
    anchors = tmp.groupby(tmp["date"].dt.to_period("W-FRI"))["date"].max()
    return set(pd.to_datetime(anchors.astype(str), errors="coerce").dropna().tolist())


def _find_most_recent(sorted_dates: list[str], target: str) -> str | None:
    if not sorted_dates:
        return None
    lo, hi = 0, len(sorted_dates) - 1
    result: str | None = None
    while lo <= hi:
        mid = (lo + hi) // 2
        cur = sorted_dates[mid]
        if cur <= target:
            result = cur
            lo = mid + 1
        else:
            hi = mid - 1
    return result


def _load_style_exposure_snapshots(data_db: Path) -> tuple[list[str], dict[str, pd.DataFrame]]:
    conn = sqlite3.connect(str(data_db))
    try:
        cols = [str(r[1]) for r in conn.execute("PRAGMA table_info(barra_exposures)").fetchall()]
        style_cols_present = [c for c in STYLE_COLUMN_TO_LABEL.keys() if c in cols]
        if not style_cols_present:
            return [], {}
        df = pd.read_sql_query(
            f"""
            SELECT ticker, as_of_date, {", ".join(style_cols_present)}
            FROM barra_exposures
            ORDER BY as_of_date, ticker
            """,
            conn,
        )
    finally:
        conn.close()
    if df.empty:
        return [], {}
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["as_of_date"] = df["as_of_date"].astype(str)
    for c in style_cols_present:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    snapshots: dict[str, pd.DataFrame] = {}
    for as_of, g in df.groupby("as_of_date", sort=True):
        snap = (
            g.drop_duplicates(subset=["ticker"], keep="last")
            .set_index("ticker")[style_cols_present]
            .rename(columns=STYLE_COLUMN_TO_LABEL)
            .fillna(0.0)
        )
        snapshots[str(as_of)] = snap
    exposure_dates = sorted(snapshots.keys())
    return exposure_dates, snapshots


def _compute_incremental_r2_by_block(
    data_db: Path,
    cache_db: Path,
    df_ret: pd.DataFrame,
    *,
    years: int = 10,
    sample_dates: set[str] | None = None,
) -> list[dict[str, float | str]]:
    """Compute daily block-level incremental R² using cached residuals.

    Method:
    - Full-model R² comes directly from `daily_factor_returns`.
    - Reconstruct style fitted return per stock/date from style factor returns and
      canonicalized style exposures.
    - Industry-only residual = full residual + style_fitted.
    - Convert to weighted R² using the same sqrt(mcap) weighting convention.
    """
    if df_ret.empty:
        return []

    latest_dt = pd.to_datetime(df_ret["date"].max(), errors="coerce")
    if pd.isna(latest_dt):
        return []
    start_dt = latest_dt - pd.Timedelta(days=365 * years)

    full_r2_by_date = (
        df_ret.groupby("date")["r_squared"]
        .mean()
        .sort_index()
    )

    style_names = sorted(set(FULL_STYLE_FACTORS.keys()))
    style_beta = (
        df_ret[df_ret["factor_name"].isin(style_names)]
        .pivot_table(index="date", columns="factor_name", values="factor_return", aggfunc="last")
        .sort_index()
        .fillna(0.0)
    )

    residuals = load_specific_residuals(cache_db, lookback_days=0)
    if residuals.empty:
        return []
    residuals["date"] = pd.to_datetime(residuals["date"], errors="coerce")
    residuals = residuals.dropna(subset=["date"])
    residuals = residuals[(residuals["date"] >= start_dt) & (residuals["date"] <= latest_dt)].copy()
    if residuals.empty:
        return []
    residuals["date_str"] = residuals["date"].dt.date.astype(str)
    if sample_dates:
        residuals = residuals[residuals["date_str"].isin(sample_dates)].copy()
        if residuals.empty:
            return []
    residuals["ticker"] = residuals["ticker"].astype(str).str.upper()
    residuals["residual"] = pd.to_numeric(residuals["residual"], errors="coerce")
    residuals["market_cap"] = pd.to_numeric(residuals["market_cap"], errors="coerce")
    residuals["trbc_industry_group"] = residuals["trbc_industry_group"].fillna("Unmapped").astype(str)

    exposure_dates, exposure_snaps = _load_style_exposure_snapshots(data_db)
    if not exposure_dates or not exposure_snaps:
        return []

    out: list[dict[str, float | str]] = []
    for dt, g in residuals.groupby("date", sort=True):
        dstr = _to_date_str(dt)
        exp_date = _find_most_recent(exposure_dates, dstr)
        if exp_date is None:
            continue
        snap = exposure_snaps.get(exp_date)
        if snap is None or snap.empty:
            continue
        r2_full = float(full_r2_by_date.get(dt, np.nan))
        if not np.isfinite(r2_full):
            continue

        tickers = g["ticker"].astype(str)
        raw_style = snap.reindex(tickers).fillna(0.0)
        if raw_style.empty:
            continue
        caps = pd.to_numeric(g["market_cap"], errors="coerce").astype(float)
        resid_full = pd.to_numeric(g["residual"], errors="coerce").astype(float)
        valid = np.isfinite(caps.to_numpy()) & (caps.to_numpy() > 0) & np.isfinite(resid_full.to_numpy())
        if int(valid.sum()) < 20:
            continue

        valid_idx = g.index[valid]
        valid_tickers = tickers.loc[valid_idx]
        caps_valid = caps.loc[valid_idx]
        resid_full_valid = resid_full.loc[valid_idx].to_numpy(dtype=float)
        inds = g.loc[valid_idx, "trbc_industry_group"].fillna("Unmapped").astype(str)
        ind_dummies = pd.get_dummies(inds, dtype=float)

        style_scores = raw_style.loc[valid_tickers].copy()
        style_scores.index = valid_idx
        style_scores = style_scores.fillna(0.0)
        canonical = canonicalize_style_scores(
            style_scores=style_scores,
            market_caps=pd.Series(caps_valid.to_numpy(dtype=float), index=valid_idx, dtype=float),
            orth_rules=FULL_STYLE_ORTH_RULES,
            industry_exposures=ind_dummies,
        ).fillna(0.0)

        if canonical.empty:
            continue

        beta_row = style_beta.loc[dt] if dt in style_beta.index else pd.Series(dtype=float)
        beta_vec = np.array([float(beta_row.get(f, 0.0) or 0.0) for f in canonical.columns], dtype=float)
        style_fit = canonical.to_numpy(dtype=float) @ beta_vec
        resid_ind_only = resid_full_valid + style_fit

        w = np.sqrt(np.clip(caps_valid.to_numpy(dtype=float), 0.0, None))
        wsum = float(np.sum(w))
        if wsum <= 0:
            continue
        ww = w / wsum
        sse_full = float(np.sum(ww * (resid_full_valid ** 2)))
        if sse_full <= 0:
            r2_ind = r2_full
        else:
            denom = max(1e-10, 1.0 - r2_full)
            sst = sse_full / denom
            sse_ind = float(np.sum(ww * (resid_ind_only ** 2)))
            r2_ind = 1.0 - (sse_ind / sst if sst > 0 else 0.0)
        r2_ind = float(np.clip(r2_ind, 0.0, 1.0))
        r2_full = float(np.clip(r2_full, 0.0, 1.0))
        r2_style_inc = float(np.clip(r2_full - r2_ind, 0.0, 1.0))

        out.append({
            "date": dstr,
            "r2_full": r2_full,
            "r2_industry": r2_ind,
            "r2_style_incremental": r2_style_inc,
        })

    if not out:
        return out
    block_df = pd.DataFrame(out).sort_values("date")
    # Week-end series: use weekly analogs of 60d/252d windows.
    # 60 trading days ~ 12 weeks, 252 trading days ~ 52 weeks.
    block_df["roll60_full"] = block_df["r2_full"].rolling(window=12, min_periods=4).mean()
    block_df["roll60_industry"] = block_df["r2_industry"].rolling(window=12, min_periods=4).mean()
    block_df["roll60_style_incremental"] = block_df["r2_style_incremental"].rolling(window=12, min_periods=4).mean()

    rows: list[dict[str, float | str]] = []
    for _, r in block_df.iterrows():
        rows.append({
            "date": str(r["date"]),
            "r2_full": float(r["r2_full"]),
            "r2_industry": float(r["r2_industry"]),
            "r2_style_incremental": float(r["r2_style_incremental"]),
            "roll60_full": float(r["roll60_full"]) if np.isfinite(r["roll60_full"]) else float(r["r2_full"]),
            "roll60_industry": float(r["roll60_industry"]) if np.isfinite(r["roll60_industry"]) else float(r["r2_industry"]),
            "roll60_style_incremental": float(r["roll60_style_incremental"]) if np.isfinite(r["roll60_style_incremental"]) else float(r["r2_style_incremental"]),
        })
    return rows


def _load_latest_exposure_snapshot(data_db: Path) -> tuple[str | None, pd.DataFrame]:
    conn = sqlite3.connect(str(data_db))
    try:
        ensure_trbc_naming(conn)
        latest_row = conn.execute("SELECT MAX(as_of_date) FROM barra_exposures").fetchone()
        as_of = str(latest_row[0]) if latest_row and latest_row[0] else None
        if as_of is None:
            return None, pd.DataFrame()
        cols = [str(r[1]) for r in conn.execute("PRAGMA table_info(barra_exposures)").fetchall()]
        industry_col = pick_trbc_industry_column(cols) or "trbc_industry_group"
        style_cols = [c for c in STYLE_COLUMN_TO_LABEL.keys() if c in cols]
        select_cols = ["ticker", "as_of_date", industry_col, *style_cols]
        sql = f"""
        SELECT {", ".join(select_cols)}
        FROM barra_exposures
        WHERE as_of_date = ?
        ORDER BY ticker
        """
        df = pd.read_sql_query(sql, conn, params=(as_of,))
        if industry_col != "trbc_industry_group" and industry_col in df.columns:
            df = df.rename(columns={industry_col: "trbc_industry_group"})
    finally:
        conn.close()
    return as_of, df


def _load_market_caps_for_snapshot_dates(
    data_db: Path,
    snapshot_dates: list[str],
) -> dict[str, pd.Series]:
    dates = sorted({str(d) for d in snapshot_dates if str(d).strip()})
    if not dates:
        return {}

    conn = sqlite3.connect(str(data_db))
    try:
        mcap_df = pd.read_sql_query(
            """
            SELECT ticker, fetch_date, market_cap
            FROM fundamental_snapshots
            WHERE fetch_date <= ?
            ORDER BY fetch_date, ticker
            """,
            conn,
            params=(dates[-1],),
        )
    finally:
        conn.close()
    if mcap_df.empty:
        return {}

    mcap_df["ticker"] = mcap_df["ticker"].astype(str).str.upper()
    mcap_df["fetch_date"] = mcap_df["fetch_date"].astype(str)
    mcap_df["market_cap"] = pd.to_numeric(mcap_df["market_cap"], errors="coerce")

    panel = (
        mcap_df.dropna(subset=["ticker", "fetch_date"])
        .drop_duplicates(subset=["ticker", "fetch_date"], keep="last")
        .pivot(index="fetch_date", columns="ticker", values="market_cap")
        .sort_index()
    )
    if panel.empty:
        return {}

    panel = panel.reindex(dates).ffill()
    out: dict[str, pd.Series] = {}
    for d in dates:
        if d not in panel.index:
            continue
        out[d] = pd.to_numeric(panel.loc[d], errors="coerce").dropna().astype(float)
    return out


def _build_factor_exposure_matrix(
    snapshot_df: pd.DataFrame,
    *,
    market_caps: pd.Series | None = None,
    canonicalize_style: bool = False,
) -> pd.DataFrame:
    if snapshot_df.empty:
        return pd.DataFrame()
    df = snapshot_df.copy()
    df["ticker"] = df["ticker"].astype(str).str.upper()
    style_cols = [c for c in STYLE_COLUMN_TO_LABEL.keys() if c in df.columns]
    style = df[["ticker", *style_cols]].copy()
    style = style.drop_duplicates(subset=["ticker"], keep="last").set_index("ticker")
    style = style.apply(pd.to_numeric, errors="coerce").fillna(0.0)
    style = style.rename(columns=STYLE_COLUMN_TO_LABEL)

    industry_series = (
        df[["ticker", "trbc_industry_group"]]
        .copy()
        .drop_duplicates(subset=["ticker"], keep="last")
        .set_index("ticker")["trbc_industry_group"]
        .fillna("Unmapped")
        .astype(str)
    )
    industry = pd.get_dummies(industry_series, dtype=float)

    if canonicalize_style and not style.empty:
        caps = pd.to_numeric(market_caps, errors="coerce") if market_caps is not None else pd.Series(dtype=float)
        caps = caps.reindex(style.index)
        cap_vals = caps.to_numpy(dtype=float)
        finite_pos = cap_vals[np.isfinite(cap_vals) & (cap_vals > 0)]
        cap_fallback = float(np.nanmedian(finite_pos)) if finite_pos.size > 0 else 1.0
        if not np.isfinite(cap_fallback) or cap_fallback <= 0:
            cap_fallback = 1.0
        caps = caps.where(np.isfinite(caps) & (caps > 0), cap_fallback).astype(float)
        style = canonicalize_style_scores(
            style_scores=style,
            market_caps=caps,
            orth_rules=FULL_STYLE_ORTH_RULES,
            industry_exposures=industry,
        ).reindex(columns=style.columns, fill_value=0.0)

    return pd.concat([industry, style], axis=1).fillna(0.0)


def _compute_exposure_turnover(data_db: Path, factor_cols: list[str]) -> list[dict[str, float | str]]:
    if not factor_cols:
        return []
    conn = sqlite3.connect(str(data_db))
    try:
        ensure_trbc_naming(conn)
        cols = [str(r[1]) for r in conn.execute("PRAGMA table_info(barra_exposures)").fetchall()]
        industry_col = pick_trbc_industry_column(cols) or "trbc_industry_group"
        style_cols = [c for c in STYLE_COLUMN_TO_LABEL.keys() if c in cols]
        df = pd.read_sql_query(
            f"""
            SELECT ticker, as_of_date, {industry_col} AS trbc_industry_group, {", ".join(style_cols)}
            FROM barra_exposures
            ORDER BY as_of_date, ticker
            """,
            conn,
        )
    finally:
        conn.close()
    if df.empty:
        return []

    snapshot_dates = sorted(df["as_of_date"].astype(str).unique().tolist())
    caps_by_date = _load_market_caps_for_snapshot_dates(data_db, snapshot_dates)

    per_date: dict[str, pd.DataFrame] = {}
    for as_of, g in df.groupby("as_of_date", sort=True):
        d = str(as_of)
        m = _build_factor_exposure_matrix(
            g,
            market_caps=caps_by_date.get(d),
            canonicalize_style=True,
        )
        if m.empty:
            continue
        m = m.reindex(columns=factor_cols, fill_value=0.0)
        per_date[d] = m
    dates = sorted(per_date.keys())
    if len(dates) < 2:
        return []

    rows: list[dict[str, float | str]] = []
    prev_date = dates[0]
    prev = per_date[dates[0]]
    for d in dates[1:]:
        cur = per_date[d]
        idx = prev.index.union(cur.index)
        a = prev.reindex(index=idx, columns=factor_cols, fill_value=0.0).to_numpy(dtype=float)
        b = cur.reindex(index=idx, columns=factor_cols, fill_value=0.0).to_numpy(dtype=float)
        raw_turnover = float(np.mean(np.abs(b - a))) if a.size and b.size else 0.0
        try:
            delta_days = int((pd.to_datetime(d) - pd.to_datetime(prev_date)).days)
        except Exception:
            delta_days = 1
        delta_days = max(1, delta_days)
        turnover = raw_turnover / float(delta_days)
        rows.append({
            "date": d,
            "turnover": turnover,
            "raw_turnover": raw_turnover,
            "interval_days": float(delta_days),
        })
        prev = cur
        prev_date = d
    s = pd.Series([float(r["turnover"]) for r in rows], index=[str(r["date"]) for r in rows], dtype=float)
    roll = s.rolling(window=60, min_periods=3).mean()
    out: list[dict[str, float | str]] = []
    raw_map = {str(r["date"]): float(r["raw_turnover"]) for r in rows}
    interval_map = {str(r["date"]): float(r["interval_days"]) for r in rows}
    for d, t in s.items():
        out.append({
            "date": d,
            "turnover": float(t),
            "roll60": float(roll.loc[d]) if np.isfinite(roll.loc[d]) else float(t),
            "raw_turnover": raw_map.get(str(d), 0.0),
            "interval_days": interval_map.get(str(d), 1.0),
        })
    return out


def _load_prices(data_db: Path, tickers: list[str], lookback_days: int = 400) -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame()
    clean = sorted({str(t).upper() for t in tickers if str(t).strip()})
    if not clean:
        return pd.DataFrame()
    placeholders = ",".join("?" for _ in clean)
    conn = sqlite3.connect(str(data_db))
    try:
        latest_row = conn.execute("SELECT MAX(date) FROM prices_daily").fetchone()
        latest = str(latest_row[0]) if latest_row and latest_row[0] else None
        if latest is None:
            return pd.DataFrame()
        latest_dt = date.fromisoformat(latest)
        start = (latest_dt - timedelta(days=int(lookback_days * 1.8))).isoformat()
        df = pd.read_sql_query(
            f"""
            SELECT ticker, date, CAST(close AS REAL) AS close
            FROM prices_daily
            WHERE ticker IN ({placeholders})
              AND date >= ?
            ORDER BY date, ticker
            """,
            conn,
            params=(*clean, start),
        )
    finally:
        conn.close()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df.dropna(subset=["date", "close"])


def _portfolio_realized_vol(returns_pivot: pd.DataFrame, weights: dict[str, float], lookback_days: int = 60) -> float:
    if returns_pivot.empty or not weights:
        return 0.0
    tickers = [t for t in returns_pivot.columns if t in weights]
    if not tickers:
        return 0.0
    w = np.array([float(weights[t]) for t in tickers], dtype=float)
    r = returns_pivot[tickers].fillna(0.0).to_numpy(dtype=float)
    pr = r @ w
    if pr.size < 3:
        return 0.0
    pr = pr[-lookback_days:]
    vol = float(np.std(pr, ddof=1)) * np.sqrt(ANNUALIZATION)
    return vol if np.isfinite(vol) else 0.0


def _portfolio_forecast_vol(
    weights: dict[str, float],
    ticker_payload: dict[str, dict[str, Any]],
    factors: list[str],
    cov: np.ndarray,
) -> float:
    if not weights or cov.size == 0:
        return 0.0
    h = np.zeros(len(factors), dtype=float)
    spec = 0.0
    for t, w in weights.items():
        p = ticker_payload.get(str(t).upper()) or {}
        exps = p.get("exposures") or {}
        for i, f in enumerate(factors):
            h[i] += float(w) * float(exps.get(f, 0.0) or 0.0)
        sv = float(p.get("specific_var", 0.0) or 0.0)
        if np.isfinite(sv) and sv > 0:
            spec += (float(w) ** 2) * sv
    var = float(h.T @ cov @ h) + spec
    var = max(0.0, var if np.isfinite(var) else 0.0)
    return float(np.sqrt(var))


def compute_health_diagnostics(data_db: Path, cache_db: Path) -> dict[str, Any]:
    """Compute and return diagnostics payload for the Health tab."""
    df_ret = _load_daily_factor_returns(cache_db, years=10)
    if df_ret.empty:
        return {
            "status": "no-data",
            "notes": ["No daily_factor_returns history available."],
            "as_of": None,
        }

    style_names = set(FULL_STYLE_FACTORS.keys())
    risk_cache = cache_get("risk") or {}
    week_end_dates = _week_end_sample_dates(df_ret["date"])
    week_end_str = {_to_date_str(d) for d in week_end_dates}

    # SECTION 1 — Cross-sectional regression health
    r2_daily = (
        df_ret.groupby("date", as_index=False)["r_squared"]
        .mean()
        .sort_values("date")
        .rename(columns={"r_squared": "r2"})
    )
    if week_end_dates:
        r2_daily = r2_daily[r2_daily["date"].isin(week_end_dates)].copy()
    # Week-end series: use weekly analogs of 60d/252d windows.
    # 60 trading days ~ 12 weeks, 252 trading days ~ 52 weeks.
    r2_daily["roll60"] = r2_daily["r2"].rolling(window=12, min_periods=4).mean()
    r2_daily["roll252"] = r2_daily["r2"].rolling(window=52, min_periods=8).mean()

    t_proxy = df_ret.copy()
    t_proxy["t_stat"] = t_proxy["factor_return"] / t_proxy["residual_vol"].clip(lower=1e-8)
    t_proxy["is_style"] = t_proxy["factor_name"].isin(style_names)

    t_hist = _hist(t_proxy["t_stat"].to_numpy(dtype=float), bins=48, lo=-6.0, hi=6.0)
    hit_rate = (
        t_proxy.groupby("factor_name", as_index=False)["t_stat"]
        .apply(lambda s: float(np.mean(np.abs(s.to_numpy(dtype=float)) > 2.0) * 100.0))
        .rename(columns={"t_stat": "pct_days_abs_t_gt_2"})
        .sort_values("pct_days_abs_t_gt_2", ascending=False)
    )
    hit_rate["pct_days_abs_t_gt_2"] = hit_rate["pct_days_abs_t_gt_2"].round(2)

    incremental_r2_series = _compute_incremental_r2_by_block(
        data_db,
        cache_db,
        df_ret,
        years=10,
        sample_dates=week_end_str,
    )
    breadth_rows = []
    t_proxy_sampled = t_proxy[t_proxy["date"].dt.date.astype(str).isin(week_end_str)].copy() if week_end_str else t_proxy
    for dt, g in t_proxy_sampled.groupby("date", sort=True):
        style_t = np.abs(g.loc[g["is_style"], "t_stat"].to_numpy(dtype=float))
        ind_t = np.abs(g.loc[~g["is_style"], "t_stat"].to_numpy(dtype=float))
        style_mean = float(np.mean(style_t)) if style_t.size else 0.0
        ind_mean = float(np.mean(ind_t)) if ind_t.size else 0.0
        breadth_rows.append({
            "date": _to_date_str(dt),
            "industry_mean_abs_t": ind_mean,
            "style_mean_abs_t": style_mean,
        })

    industry_t_all = np.abs(t_proxy_sampled.loc[~t_proxy_sampled["is_style"], "t_stat"].to_numpy(dtype=float))
    style_t_all = np.abs(t_proxy_sampled.loc[t_proxy_sampled["is_style"], "t_stat"].to_numpy(dtype=float))
    breadth_summary = {
        "industry_mean_abs_t": float(np.mean(industry_t_all)) if industry_t_all.size else 0.0,
        "style_mean_abs_t": float(np.mean(style_t_all)) if style_t_all.size else 0.0,
    }

    portfolio_variance_split = {
        "industry_pct_total": float((risk_cache.get("risk_shares") or {}).get("industry", 0.0) or 0.0),
        "style_pct_total": float((risk_cache.get("risk_shares") or {}).get("style", 0.0) or 0.0),
        "idio_pct_total": float((risk_cache.get("risk_shares") or {}).get("idio", 0.0) or 0.0),
        "industry_pct_factor_only": float((risk_cache.get("component_shares") or {}).get("industry", 0.0) or 0.0) * 100.0,
        "style_pct_factor_only": float((risk_cache.get("component_shares") or {}).get("style", 0.0) or 0.0) * 100.0,
    }

    # SECTION 2 — Exposure diagnostics (latest cross-section)
    exp_as_of, snap = _load_latest_exposure_snapshot(data_db)
    exp_caps = _load_market_caps_for_snapshot_dates(data_db, [exp_as_of] if exp_as_of else [])
    exp_matrix = _build_factor_exposure_matrix(
        snap,
        market_caps=exp_caps.get(str(exp_as_of)) if exp_as_of else None,
        canonicalize_style=True,
    )
    factor_stats: list[dict[str, Any]] = []
    factor_hists: dict[str, dict[str, list[float | int]]] = {}
    exp_corr = {"factors": [], "correlation": []}
    turnover_series: list[dict[str, float | str]] = []
    if not exp_matrix.empty:
        for f in exp_matrix.columns:
            x = exp_matrix[f].to_numpy(dtype=float)
            finite = x[np.isfinite(x)]
            if finite.size == 0:
                continue
            factor_stats.append({
                "factor": str(f),
                "mean": float(np.mean(finite)),
                "std": float(np.std(finite, ddof=0)),
                "p1": float(np.percentile(finite, 1)),
                "p99": float(np.percentile(finite, 99)),
                "max_abs": float(np.max(np.abs(finite))),
            })
            factor_hists[str(f)] = _hist(finite, bins=30)

        factor_stats = sorted(factor_stats, key=lambda r: str(r["factor"]))
        corr_df = exp_matrix.corr().fillna(0.0)
        exp_corr = {
            "factors": [str(c) for c in corr_df.columns],
            "correlation": [[float(v) for v in row] for row in corr_df.to_numpy(dtype=float)],
        }
        turnover_series = _compute_exposure_turnover(data_db, list(exp_matrix.columns))

    # SECTION 3 — Factor return health
    piv = (
        df_ret.pivot_table(index="date", columns="factor_name", values="factor_return", aggfunc="last")
        .sort_index()
        .fillna(0.0)
    )
    factors = [str(c) for c in piv.columns]
    cumulative: dict[str, list[dict[str, float | str]]] = {}
    rolling_vol_60d: dict[str, list[dict[str, float | str]]] = {}
    return_dist: dict[str, dict[str, list[float | int]]] = {}
    if not piv.empty:
        for f in factors:
            s = piv[f].astype(float)
            cum = (1.0 + s).cumprod() - 1.0
            rv = s.rolling(window=60, min_periods=20).std(ddof=1) * np.sqrt(ANNUALIZATION)
            cumulative[f] = [{"date": _to_date_str(d), "value": float(v)} for d, v in cum.items()]
            rolling_vol_60d[f] = [{"date": _to_date_str(d), "value": float(v if np.isfinite(v) else 0.0)} for d, v in rv.items()]
            return_dist[f] = _hist(s.to_numpy(dtype=float), bins=40)

    ret_corr_df = piv.corr().fillna(0.0) if not piv.empty else pd.DataFrame()
    ret_corr = {
        "factors": [str(c) for c in ret_corr_df.columns],
        "correlation": [[float(v) for v in row] for row in ret_corr_df.to_numpy(dtype=float)],
    }

    # SECTION 4 — Covariance quality
    portfolio_cache = cache_get("portfolio") or {}
    universe_cache = cache_get("universe_loadings") or {}
    factor_details = {str(d.get("factor")): d for d in (risk_cache.get("factor_details") or [])}
    corr_obj = (risk_cache.get("cov_matrix") or {})
    cov_factors = [str(f) for f in (corr_obj.get("factors") or [])]
    corr_arr = np.array(corr_obj.get("correlation") or [], dtype=float)
    vols = np.array([float((factor_details.get(f) or {}).get("factor_vol", 0.0) or 0.0) for f in cov_factors], dtype=float)
    if corr_arr.size and vols.size == len(cov_factors):
        cov_mat = corr_arr * np.outer(vols, vols)
    else:
        cov_mat = np.zeros((0, 0), dtype=float)
    eigvals: list[float] = []
    if cov_mat.size:
        try:
            ev = np.linalg.eigvalsh(0.5 * (cov_mat + cov_mat.T))
            eigvals = [float(v) for v in np.sort(np.clip(ev, 0.0, None))[::-1]]
        except np.linalg.LinAlgError:
            eigvals = []

    # Forecast vs realized volatility
    positions = portfolio_cache.get("positions") or []
    ticker_payload: dict[str, dict[str, Any]] = {}
    for p in positions:
        t = str(p.get("ticker", "")).upper()
        if t:
            ticker_payload[t] = p
    by_ticker = (universe_cache.get("by_ticker") or {})
    for t, p in by_ticker.items():
        tt = str(t).upper()
        if tt not in ticker_payload:
            ticker_payload[tt] = p

    # Portfolio definitions
    w_current = {str(p.get("ticker", "")).upper(): float(p.get("weight", 0.0) or 0.0) for p in positions}
    long_only = [p for p in positions if float(p.get("market_value", 0.0) or 0.0) > 0]
    w_long_only = {str(p.get("ticker", "")).upper(): (1.0 / len(long_only) if long_only else 0.0) for p in long_only}
    shorts = [p for p in positions if float(p.get("market_value", 0.0) or 0.0) < 0]
    longs = long_only
    w_dneutral: dict[str, float] = {}
    if longs:
        for p in longs:
            w_dneutral[str(p.get("ticker", "")).upper()] = 1.0 / len(longs)
    if shorts:
        for p in shorts:
            w_dneutral[str(p.get("ticker", "")).upper()] = -1.0 / len(shorts)
    w_spy = {"SPY": 1.0}

    all_tickers = sorted({*w_current.keys(), *w_long_only.keys(), *w_dneutral.keys(), "SPY"})
    px = _load_prices(data_db, all_tickers, lookback_days=420)
    ret_piv = pd.DataFrame()
    if not px.empty:
        ret_piv = (
            px.pivot_table(index="date", columns="ticker", values="close", aggfunc="last")
            .sort_index()
            .pct_change(fill_method=None)
            .replace([np.inf, -np.inf], np.nan)
            .fillna(0.0)
        )

    fv = []
    for name, wmap in [
        ("Current Portfolio", w_current),
        ("Equal-Weight Long-Only", w_long_only),
        ("Equal-Weight Dollar-Neutral", w_dneutral),
        ("SPY Proxy", w_spy),
    ]:
        if not wmap:
            continue
        fvol = _portfolio_forecast_vol(wmap, ticker_payload, cov_factors, cov_mat) if cov_mat.size else 0.0
        rvol = _portfolio_realized_vol(ret_piv, wmap, lookback_days=60) if not ret_piv.empty else 0.0
        fv.append({
            "name": name,
            "forecast_vol": float(fvol),
            "realized_vol_60d": float(rvol),
        })

    # Rolling average factor vol
    avg_factor_vol_series: list[dict[str, float | str]] = []
    if not piv.empty:
        rv = piv.rolling(window=60, min_periods=20).std(ddof=1) * np.sqrt(ANNUALIZATION)
        avg = rv.mean(axis=1)
        avg_factor_vol_series = [{"date": _to_date_str(d), "value": float(v if np.isfinite(v) else 0.0)} for d, v in avg.items()]

    return {
        "status": "ok",
        "as_of": _to_date_str(df_ret["date"].max()),
        "notes": [
            "Daily factor t-stat uses a practical proxy: factor_return / residual_vol (not exact coefficient SE-based t-stat).",
            "Incremental block R² is reconstructed from cached full residuals plus canonicalized style-fitted returns.",
            "Computationally intensive Section 1 time-series are sampled at week-end over 10 years.",
            "Exposure turnover is normalized by elapsed calendar days between snapshots, then smoothed with a 60-observation rolling mean.",
        ],
        "section1": {
            "sampling": "weekly_week_end",
            "r2_series": [
                {
                    "date": _to_date_str(r["date"]),
                    "r2": float(r["r2"]),
                    "roll60": float(r["roll60"] if np.isfinite(r["roll60"]) else r["r2"]),
                    "roll252": float(r["roll252"] if np.isfinite(r["roll252"]) else r["r2"]),
                }
                for _, r in r2_daily.iterrows()
            ],
            "incremental_block_r2_series": incremental_r2_series,
            "t_stat_hist": t_hist,
            "pct_days_abs_t_gt_2": [
                {"factor": str(r["factor_name"]), "value": float(r["pct_days_abs_t_gt_2"])}
                for _, r in hit_rate.iterrows()
            ],
            "bucket_breadth_series": breadth_rows,
            "bucket_breadth_summary": breadth_summary,
            "portfolio_variance_split": portfolio_variance_split,
        },
        "section2": {
            "as_of": exp_as_of,
            "style_scores": "canonicalized",
            "factor_stats": factor_stats,
            "factor_histograms": factor_hists,
            "exposure_corr": exp_corr,
            "turnover_series": turnover_series,
        },
        "section3": {
            "factors": factors,
            "cumulative_returns": cumulative,
            "rolling_vol_60d": rolling_vol_60d,
            "return_corr": ret_corr,
            "return_dist": return_dist,
        },
        "section4": {
            "eigenvalues": eigvals,
            "condition_number": float(risk_cache.get("condition_number", 0.0) or 0.0),
            "forecast_vs_realized": fv,
            "rolling_avg_factor_vol": avg_factor_vol_series,
        },
    }
