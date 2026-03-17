"""Model health diagnostics for the Health dashboard."""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.risk_model.daily_factor_returns import load_specific_residuals
from backend.risk_model.descriptors import FULL_STYLE_FACTORS, FULL_STYLE_ORTH_RULES, canonicalize_style_scores
from backend.risk_model.eligibility import build_eligibility_context, structural_eligibility_for_date
from backend.risk_model.factor_catalog import (
    build_factor_catalog_for_factors,
    factor_name_to_id_map,
    serialize_factor_catalog,
)
from backend.risk_model.risk_attribution import STYLE_COLUMN_TO_LABEL
from backend.data.sqlite import cache_get

ANNUALIZATION = 252.0
HEALTH_CORE_COUNTRY_CODES = {"US"}


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
        cols = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(daily_factor_returns)").fetchall()
        }
        required_cols = {"date", "factor_name", "factor_return", "robust_se", "t_stat", "r_squared", "residual_vol"}
        if not required_cols.issubset(cols):
            return pd.DataFrame()
        latest_row = conn.execute("SELECT MAX(date) FROM daily_factor_returns").fetchone()
        latest = latest_row[0] if latest_row and latest_row[0] else None
        if latest is None:
            return pd.DataFrame()
        latest_dt = date.fromisoformat(str(latest))
        start_dt = latest_dt - timedelta(days=365 * years)
        df = pd.read_sql_query(
            """
            SELECT
                date,
                factor_name,
                factor_return,
                COALESCE(robust_se, 0.0) AS robust_se,
                COALESCE(t_stat, 0.0) AS t_stat,
                r_squared,
                residual_vol
            FROM daily_factor_returns
            WHERE date >= ?
            ORDER BY date, factor_name
            """,
            conn,
            params=(start_dt.isoformat(),),
        )
    except sqlite3.OperationalError:
        return pd.DataFrame()
    finally:
        conn.close()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["factor_name"] = df["factor_name"].astype(str)
    df["factor_return"] = pd.to_numeric(df["factor_return"], errors="coerce").fillna(0.0)
    df["robust_se"] = pd.to_numeric(df["robust_se"], errors="coerce").fillna(0.0)
    df["t_stat"] = pd.to_numeric(df["t_stat"], errors="coerce").fillna(0.0)
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


def _load_exposure_dates(data_db: Path) -> list[str]:
    conn = sqlite3.connect(str(data_db))
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT as_of_date
            FROM barra_raw_cross_section_history
            WHERE as_of_date IS NOT NULL
            ORDER BY as_of_date
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()
    return [str(row[0]) for row in rows if row and row[0]]


def _load_style_exposure_snapshots(
    data_db: Path,
    *,
    required_dates: set[str] | None = None,
) -> tuple[list[str], dict[str, pd.DataFrame]]:
    conn = sqlite3.connect(str(data_db))
    try:
        cols = [str(r[1]) for r in conn.execute("PRAGMA table_info(barra_raw_cross_section_history)").fetchall()]
        style_cols_present = [c for c in STYLE_COLUMN_TO_LABEL.keys() if c in cols]
        if not style_cols_present:
            return [], {}
        key_col = "ric" if "ric" in cols else ("ticker" if "ticker" in cols else None)
        if key_col is None:
            return [], {}
        exposure_dates = [
            str(row[0])
            for row in conn.execute(
                """
                SELECT DISTINCT as_of_date
                FROM barra_raw_cross_section_history
                ORDER BY as_of_date
                """
            ).fetchall()
            if row and row[0]
        ]
        if not exposure_dates:
            return [], {}

        required = {str(d).strip() for d in (required_dates or set()) if str(d).strip()}
        dates_to_load: list[str]
        if required:
            dates_to_load = [d for d in exposure_dates if d in required]
            if not dates_to_load:
                return exposure_dates, {}
            placeholders = ",".join("?" for _ in dates_to_load)
            df = pd.read_sql_query(
                f"""
                SELECT UPPER({key_col}) AS security_key, as_of_date, {", ".join(style_cols_present)}
                FROM barra_raw_cross_section_history
                WHERE as_of_date IN ({placeholders})
                ORDER BY as_of_date, UPPER({key_col})
                """,
                conn,
                params=tuple(dates_to_load),
            )
        else:
            df = pd.read_sql_query(
                f"""
                SELECT UPPER({key_col}) AS security_key, as_of_date, {", ".join(style_cols_present)}
                FROM barra_raw_cross_section_history
                ORDER BY as_of_date, UPPER({key_col})
                """,
                conn,
            )
    finally:
        conn.close()
    if df.empty:
        return exposure_dates, {}
    df["security_key"] = df["security_key"].astype(str).str.upper()
    df["as_of_date"] = df["as_of_date"].astype(str)
    for c in style_cols_present:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    snapshots: dict[str, pd.DataFrame] = {}
    for as_of, g in df.groupby("as_of_date", sort=True):
        snap = (
            g.drop_duplicates(subset=["security_key"], keep="last")
            .set_index("security_key")[style_cols_present]
            .rename(columns=STYLE_COLUMN_TO_LABEL)
            .fillna(0.0)
        )
        snapshots[str(as_of)] = snap
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
    - Structural-block residual = full residual + style_fitted.
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

    residuals = load_specific_residuals(cache_db, lookback_days=0, residual_kind="model")
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
    residuals["trbc_business_sector"] = (
        residuals["trbc_business_sector"]
        .fillna("")
        .astype(str)
        .str.strip()
    )

    exposure_dates = _load_exposure_dates(data_db)
    if not exposure_dates:
        return []
    required_exposure_dates = {
        exp_date
        for sample_date in sorted(residuals["date_str"].unique().tolist())
        for exp_date in [_find_most_recent(exposure_dates, str(sample_date))]
        if exp_date is not None
    }
    exposure_dates, exposure_snaps = _load_style_exposure_snapshots(
        data_db,
        required_dates=required_exposure_dates,
    )
    if not exposure_snaps:
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

        security_ids = (
            g["ric"].astype(str).str.upper()
            if "ric" in g.columns
            else g["ticker"].astype(str).str.upper()
        )
        raw_style = snap.reindex(security_ids)
        if raw_style.empty:
            continue
        caps = pd.to_numeric(g["market_cap"], errors="coerce").astype(float)
        resid_full = pd.to_numeric(g["residual"], errors="coerce").astype(float)
        valid = np.isfinite(caps.to_numpy()) & (caps.to_numpy() > 0) & np.isfinite(resid_full.to_numpy())
        if int(valid.sum()) < 20:
            continue

        valid_idx = g.index[valid]
        valid_security_ids = security_ids.loc[valid_idx]
        caps_valid = caps.loc[valid_idx]
        resid_full_valid = resid_full.loc[valid_idx].to_numpy(dtype=float)
        inds = g.loc[valid_idx, "trbc_business_sector"].fillna("").astype(str).str.strip()
        non_empty_ind = inds.str.len() > 0
        if int(non_empty_ind.sum()) < 20:
            continue
        valid_idx = valid_idx[non_empty_ind.to_numpy(dtype=bool)]
        valid_security_ids = security_ids.loc[valid_idx]
        caps_valid = caps.loc[valid_idx]
        resid_full_valid = resid_full.loc[valid_idx].to_numpy(dtype=float)
        inds = inds.loc[valid_idx]
        ind_dummies = pd.get_dummies(inds, dtype=float)

        style_scores = raw_style.loc[valid_security_ids].copy()
        style_scores.index = valid_idx
        style_scores = style_scores.replace([np.inf, -np.inf], np.nan)
        style_valid = style_scores.notna().all(axis=1)
        if int(style_valid.sum()) < 20:
            continue
        style_scores = style_scores.loc[style_valid]
        valid_idx = style_scores.index
        valid_security_ids = valid_security_ids.loc[valid_idx]
        caps_valid = caps_valid.loc[valid_idx]
        resid_full_valid = resid_full.loc[valid_idx].to_numpy(dtype=float)
        inds = inds.loc[valid_idx]
        ind_dummies = pd.get_dummies(inds, dtype=float)
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
        r2_ind = float(r2_ind)
        r2_full = float(r2_full)
        r2_style_inc = float(r2_full - r2_ind)

        out.append({
            "date": dstr,
            "r2_full": r2_full,
            "r2_structural": r2_ind,
            "r2_style_incremental": r2_style_inc,
        })

    if not out:
        return out
    block_df = pd.DataFrame(out).sort_values("date")
    # Week-end series: use weekly analogs of 60d/252d windows.
    # 60 trading days ~ 12 weeks, 252 trading days ~ 52 weeks.
    block_df["roll60_full"] = block_df["r2_full"].rolling(window=12, min_periods=4).mean()
    block_df["roll60_structural"] = block_df["r2_structural"].rolling(window=12, min_periods=4).mean()
    block_df["roll60_style_incremental"] = block_df["r2_style_incremental"].rolling(window=12, min_periods=4).mean()

    rows: list[dict[str, float | str]] = []
    for _, r in block_df.iterrows():
        rows.append({
            "date": str(r["date"]),
            "r2_full": float(r["r2_full"]),
            "r2_structural": float(r["r2_structural"]),
            "r2_style_incremental": float(r["r2_style_incremental"]),
            "roll60_full": float(r["roll60_full"]) if np.isfinite(r["roll60_full"]) else float(r["r2_full"]),
            "roll60_structural": float(r["roll60_structural"]) if np.isfinite(r["roll60_structural"]) else float(r["r2_structural"]),
            "roll60_style_incremental": float(r["roll60_style_incremental"]) if np.isfinite(r["roll60_style_incremental"]) else float(r["r2_style_incremental"]),
        })
    return rows


def _build_factor_exposure_matrix(
    snapshot_df: pd.DataFrame,
    *,
    eligibility: pd.DataFrame,
    core_country_codes: set[str] | None = None,
) -> pd.DataFrame:
    if snapshot_df.empty or eligibility.empty:
        return pd.DataFrame()
    df = snapshot_df.copy()
    key_col = "ric" if "ric" in df.columns else ("ticker" if "ticker" in df.columns else None)
    if key_col is None:
        return pd.DataFrame()
    df[key_col] = df[key_col].astype(str).str.upper()
    style_cols = [c for c in STYLE_COLUMN_TO_LABEL.keys() if c in df.columns]
    if not style_cols:
        return pd.DataFrame()

    eligible = eligibility[eligibility["is_structural_eligible"].astype(bool)].copy()
    if core_country_codes:
        allowed = {str(code).upper().strip() for code in core_country_codes if str(code).strip()}
        country = (
            eligible["hq_country_code"]
            .fillna("")
            .astype(str)
            .str.upper()
            .str.strip()
        )
        eligible = eligible[country.isin(allowed)].copy()
    if eligible.empty:
        return pd.DataFrame()
    eligible_idx = eligible.index.astype(str).str.upper()

    style = df[[key_col, *style_cols]].copy()
    style = style.drop_duplicates(subset=[key_col], keep="last").set_index(key_col)
    style = style.reindex(eligible_idx)
    style = style.apply(pd.to_numeric, errors="coerce")
    style = style.rename(columns=STYLE_COLUMN_TO_LABEL)
    caps = pd.to_numeric(eligible.reindex(style.index)["market_cap"], errors="coerce")
    industries = (
        eligible.reindex(style.index)["trbc_business_sector"]
        .fillna("")
        .astype(str)
        .str.strip()
    )
    valid = (
        np.isfinite(caps.to_numpy(dtype=float))
        & (caps.to_numpy(dtype=float) > 0.0)
        & style.notna().all(axis=1).to_numpy(dtype=bool)
        & (industries.str.len().to_numpy(dtype=float) > 0)
    )
    if int(valid.sum()) < 2:
        return pd.DataFrame()
    valid_idx = style.index[valid]
    style = style.loc[valid_idx]
    caps = caps.loc[valid_idx]
    industries = industries.loc[valid_idx]

    industry = pd.get_dummies(industries, dtype=float)
    if industry.empty:
        return pd.DataFrame()
    style = canonicalize_style_scores(
        style_scores=style,
        market_caps=caps,
        orth_rules=FULL_STYLE_ORTH_RULES,
        industry_exposures=industry,
    ).reindex(columns=style.columns, fill_value=0.0)

    return pd.concat([industry, style], axis=1).fillna(0.0)


def _compute_exposure_turnover(
    data_db: Path,
    factor_cols: list[str],
    *,
    core_country_codes: set[str] | None = None,
    sample_dates: list[str] | None = None,
) -> list[dict[str, float | str]]:
    if not factor_cols:
        return []
    exposure_dates = _load_exposure_dates(data_db)
    if not exposure_dates:
        return []

    per_date: dict[str, pd.DataFrame] = {}
    requested_dates = (
        sorted({str(d).strip() for d in (sample_dates or []) if str(d).strip()})
        if sample_dates
        else list(exposure_dates)
    )
    for requested_date in requested_dates:
        target_date = _find_most_recent(exposure_dates, requested_date)
        if target_date is None or target_date in per_date:
            continue
        ctx = build_eligibility_context(data_db, dates=[target_date])
        exp_date, eligibility = structural_eligibility_for_date(ctx, target_date)
        if exp_date is None or eligibility.empty:
            continue
        snap = ctx.exposure_snapshots.get(exp_date)
        if snap is None or snap.empty:
            continue
        snap_with_ric = snap.reset_index().rename(columns={"index": "ric"})
        m = _build_factor_exposure_matrix(
            snap_with_ric,
            eligibility=eligibility,
            core_country_codes=core_country_codes,
        )
        if m.empty:
            continue
        m = m.reindex(columns=factor_cols, fill_value=0.0)
        per_date[str(exp_date)] = m
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
        latest_row = conn.execute("SELECT MAX(date) FROM security_prices_eod").fetchone()
        latest = str(latest_row[0]) if latest_row and latest_row[0] else None
        if latest is None:
            return pd.DataFrame()
        latest_dt = date.fromisoformat(latest)
        start = (latest_dt - timedelta(days=int(lookback_days * 1.8))).isoformat()
        df = pd.read_sql_query(
            f"""
            SELECT UPPER(sm.ticker) AS ticker, p.date, CAST(p.close AS REAL) AS close
            FROM security_prices_eod p
            JOIN security_master sm
              ON sm.ric = p.ric
            WHERE UPPER(sm.ticker) IN ({placeholders})
              AND p.date >= ?
            ORDER BY p.date, UPPER(sm.ticker)
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


def _deserialize_full_covariance(payload: Any) -> pd.DataFrame:
    if not isinstance(payload, dict):
        return pd.DataFrame()
    factors = [str(f) for f in (payload.get("factors") or []) if str(f).strip()]
    matrix = payload.get("matrix") or []
    if not factors or not isinstance(matrix, list):
        return pd.DataFrame()
    try:
        arr = np.asarray(matrix, dtype=float)
    except Exception:
        return pd.DataFrame()
    if arr.ndim != 2 or arr.shape != (len(factors), len(factors)):
        return pd.DataFrame()
    return pd.DataFrame(arr, index=factors, columns=factors)


def _is_textish_type(col_type: str) -> bool:
    ctype = str(col_type or "").upper()
    return ("CHAR" in ctype) or ("TEXT" in ctype) or ("CLOB" in ctype)


def _valid_field_mask(values: pd.Series, col_type: str) -> pd.Series:
    if _is_textish_type(col_type):
        return values.notna() & values.astype(str).str.strip().ne("")
    return values.notna()


def _compute_table_field_coverage(
    conn: sqlite3.Connection,
    *,
    table: str,
    date_col: str,
    ticker_col: str,
    excluded_cols: set[str],
    label: str,
    base_df: pd.DataFrame | None = None,
    use_field_expected_tickers: bool = False,
    scope_note: str | None = None,
) -> dict[str, Any]:
    schema = conn.execute(f"PRAGMA table_info({table})").fetchall()
    if not schema:
        return {
            "label": label,
            "table": table,
            "scope_note": scope_note or "",
            "row_count": 0,
            "date_count": 0,
            "ticker_count": 0,
            "field_count": 0,
            "low_coverage_field_count": 0,
            "fields": [],
        }

    field_defs = [(str(r[1]), str(r[2] or "")) for r in schema if str(r[1]) not in excluded_cols]
    col_types = {str(r[1]): str(r[2] or "") for r in schema}
    if base_df is None:
        base_df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    df = base_df.copy()
    if df.empty:
        row_count = 0
    else:
        if ticker_col in df.columns:
            df[ticker_col] = df[ticker_col].astype(str).str.upper()
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date.astype(str)
            df = df[df[date_col].ne("NaT")]
        row_count = int(len(df))

    if row_count <= 0 or not field_defs:
        return {
            "label": label,
            "table": table,
            "scope_note": scope_note or "",
            "row_count": row_count,
            "date_count": 0,
            "ticker_count": 0,
            "field_count": len(field_defs),
            "low_coverage_field_count": 0,
            "fields": [],
        }

    ticker_den_all = df.groupby(ticker_col).size()
    all_tickers = [str(t) for t in ticker_den_all.index.tolist()]

    fields_out: list[dict[str, Any]] = []
    for field, _ in field_defs:
        col_type = col_types.get(field, "")
        values = df[field] if field in df.columns else pd.Series(index=df.index, dtype=object)
        valid_mask = _valid_field_mask(values, col_type)

        if use_field_expected_tickers:
            expected_tickers = set(df.loc[valid_mask, ticker_col].dropna().astype(str).str.upper().tolist())
            denom_mask = df[ticker_col].isin(expected_tickers)
        else:
            expected_tickers = set(all_tickers)
            denom_mask = pd.Series(True, index=df.index)

        denom_row_count = int(denom_mask.sum())
        valid_denom_mask = valid_mask & denom_mask
        non_null_rows = int(valid_denom_mask.sum())
        row_cov = (100.0 * non_null_rows / denom_row_count) if denom_row_count else 0.0

        date_den = df.loc[denom_mask].groupby(date_col).size()
        valid_by_date = df.loc[valid_denom_mask].groupby(date_col).size()
        date_cov_vals = []
        worst_date = None
        worst_date_cov = 100.0
        dates_below_80 = 0
        for d, denom_raw in date_den.items():
            denom = max(1, int(denom_raw))
            pct = 100.0 * float(valid_by_date.get(d, 0)) / float(denom)
            date_cov_vals.append(pct)
            if pct < worst_date_cov:
                worst_date_cov = pct
                worst_date = str(d)
            if pct < 80.0:
                dates_below_80 += 1
        avg_date_cov = float(np.mean(date_cov_vals)) if date_cov_vals else 0.0

        ticker_den = df.loc[denom_mask].groupby(ticker_col).size()
        valid_by_ticker = df.loc[valid_denom_mask].groupby(ticker_col).size()
        ticker_cov_vals = []
        tickers_below_80 = 0
        for t, denom_raw in ticker_den.items():
            denom = max(1, int(denom_raw))
            pct = 100.0 * float(valid_by_ticker.get(t, 0)) / float(denom)
            ticker_cov_vals.append(pct)
            if pct < 80.0:
                tickers_below_80 += 1
        avg_ticker_cov = float(np.mean(ticker_cov_vals)) if ticker_cov_vals else 0.0
        p10_ticker_cov = float(np.percentile(ticker_cov_vals, 10)) if ticker_cov_vals else 0.0

        coverage_score = (0.4 * row_cov) + (0.4 * avg_ticker_cov) + (0.2 * p10_ticker_cov)
        fields_out.append({
            "field": field,
            "data_type": col_type,
            "non_null_rows": non_null_rows,
            "total_rows": denom_row_count,
            "row_coverage_pct": row_cov,
            "avg_date_coverage_pct": avg_date_cov,
            "worst_date": worst_date,
            "worst_date_coverage_pct": float(worst_date_cov if np.isfinite(worst_date_cov) else 0.0),
            "dates_below_80_pct_count": int(dates_below_80),
            "avg_ticker_lifecycle_coverage_pct": avg_ticker_cov,
            "p10_ticker_lifecycle_coverage_pct": p10_ticker_cov,
            "tickers_below_80_pct_count": int(tickers_below_80),
            "expected_ticker_count": int(len(expected_tickers)),
            "coverage_score_pct": float(coverage_score),
        })

    fields_out = sorted(fields_out, key=lambda r: (float(r.get("coverage_score_pct", 0.0)), str(r.get("field", ""))))
    low_cov_count = sum(1 for r in fields_out if float(r.get("coverage_score_pct", 0.0)) < 80.0)

    return {
        "label": label,
        "table": table,
        "scope_note": scope_note or "",
        "row_count": row_count,
        "date_count": int(df[date_col].nunique()),
        "ticker_count": int(df[ticker_col].nunique()),
        "field_count": len(fields_out),
        "low_coverage_field_count": int(low_cov_count),
        "fields": fields_out,
    }


def _load_equity_price_bounds(conn: sqlite3.Connection) -> pd.DataFrame:
    try:
        bounds = pd.read_sql_query(
            """
            SELECT UPPER(sm.ticker) AS ticker, MIN(p.date) AS min_date, MAX(p.date) AS max_date
            FROM security_prices_eod p
            JOIN security_master sm
              ON sm.ric = p.ric
            GROUP BY UPPER(sm.ticker)
            """,
            conn,
        )
    except Exception:
        return pd.DataFrame(columns=["ticker", "min_date", "max_date"])
    if bounds.empty:
        return bounds
    bounds["ticker"] = bounds["ticker"].astype(str).str.upper()
    bounds["min_date"] = pd.to_datetime(bounds["min_date"], errors="coerce").dt.date.astype(str)
    bounds["max_date"] = pd.to_datetime(bounds["max_date"], errors="coerce").dt.date.astype(str)
    return bounds.dropna(subset=["ticker", "min_date", "max_date"])


def _load_equity_tickers(conn: sqlite3.Connection) -> set[str]:
    try:
        rows = conn.execute(
            """
            SELECT UPPER(ticker)
            FROM security_master
            WHERE COALESCE(classification_ok, 0) = 1
              AND COALESCE(is_equity_eligible, 0) = 1
            """
        ).fetchall()
    except Exception:
        return set()
    return {str(r[0]).upper() for r in rows if r and r[0]}


def _filter_active_equity_rows(
    df: pd.DataFrame,
    *,
    ticker_col: str,
    date_col: str,
    equity_tickers: set[str],
    price_bounds: pd.DataFrame,
) -> pd.DataFrame:
    if df.empty or ticker_col not in df.columns or date_col not in df.columns:
        return df.iloc[0:0].copy()

    out = df.copy()
    out[ticker_col] = out[ticker_col].astype(str).str.upper()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce").dt.date.astype(str)
    out = out[out[date_col].ne("NaT")].copy()

    if equity_tickers:
        out = out[out[ticker_col].isin(equity_tickers)].copy()
    if out.empty:
        return out

    if price_bounds.empty:
        return out.iloc[0:0].copy()

    bounds = price_bounds.rename(columns={"ticker": "__ticker", "min_date": "__min_date", "max_date": "__max_date"})
    out = out.merge(bounds, left_on=ticker_col, right_on="__ticker", how="inner")
    out = out[
        (out[date_col] >= out["__min_date"])
        & (out[date_col] <= out["__max_date"])
    ].copy()
    return out.drop(columns=["__ticker", "__min_date", "__max_date"], errors="ignore")


def compute_health_diagnostics(
    data_db: Path,
    cache_db: Path,
    *,
    risk_payload: dict[str, Any] | None = None,
    portfolio_payload: dict[str, Any] | None = None,
    universe_payload: dict[str, Any] | None = None,
    covariance_payload: dict[str, Any] | None = None,
    source_dates: dict[str, Any] | None = None,
    run_id: str | None = None,
    snapshot_id: str | None = None,
) -> dict[str, Any]:
    """Compute and return diagnostics payload for the Health tab."""
    df_ret = _load_daily_factor_returns(cache_db, years=10)
    if df_ret.empty:
        return {
            "status": "no-data",
            "notes": [
                "No compatible daily_factor_returns history available.",
                "Health diagnostics require persisted robust factor t-stats (`robust_se`, `t_stat`).",
                "Rebuild factor-return history with the current estimator/cache schema before using this page.",
            ],
            "as_of": None,
        }

    style_names = set(FULL_STYLE_FACTORS.keys())
    risk_cache = dict(risk_payload or (cache_get("risk") or {}))
    portfolio_cache = dict(portfolio_payload or (cache_get("portfolio") or {}))
    universe_cache = dict(universe_payload or (cache_get("universe_loadings") or {}))
    cov_payload = dict(covariance_payload or (cache_get("risk_engine_cov") or {}))
    model_method_version = str((risk_cache.get("risk_engine") or {}).get("method_version") or "")
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

    t_stat_df = df_ret.copy()
    t_stat_df["is_style"] = t_stat_df["factor_name"].isin(style_names)

    t_hist = _hist(t_stat_df["t_stat"].to_numpy(dtype=float), bins=48, lo=-6.0, hi=6.0)
    hit_rate = (
        t_stat_df.groupby("factor_name", as_index=False)["t_stat"]
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
    t_stat_sampled = t_stat_df[t_stat_df["date"].dt.date.astype(str).isin(week_end_str)].copy() if week_end_str else t_stat_df
    for dt, g in t_stat_sampled.groupby("date", sort=True):
        style_t = np.abs(g.loc[g["is_style"], "t_stat"].to_numpy(dtype=float))
        ind_t = np.abs(g.loc[~g["is_style"], "t_stat"].to_numpy(dtype=float))
        style_mean = float(np.mean(style_t)) if style_t.size else 0.0
        ind_mean = float(np.mean(ind_t)) if ind_t.size else 0.0
        breadth_rows.append({
            "date": _to_date_str(dt),
            "industry_mean_abs_t": ind_mean,
            "style_mean_abs_t": style_mean,
        })

    industry_t_all = np.abs(t_stat_sampled.loc[~t_stat_sampled["is_style"], "t_stat"].to_numpy(dtype=float))
    style_t_all = np.abs(t_stat_sampled.loc[t_stat_sampled["is_style"], "t_stat"].to_numpy(dtype=float))
    breadth_summary = {
        "industry_mean_abs_t": float(np.mean(industry_t_all)) if industry_t_all.size else 0.0,
        "style_mean_abs_t": float(np.mean(style_t_all)) if style_t_all.size else 0.0,
    }

    portfolio_variance_split = {
        "market_pct_total": float((risk_cache.get("risk_shares") or {}).get("market", 0.0) or 0.0),
        "industry_pct_total": float((risk_cache.get("risk_shares") or {}).get("industry", 0.0) or 0.0),
        "style_pct_total": float((risk_cache.get("risk_shares") or {}).get("style", 0.0) or 0.0),
        "idio_pct_total": float((risk_cache.get("risk_shares") or {}).get("idio", 0.0) or 0.0),
        "market_pct_factor_only": float((risk_cache.get("component_shares") or {}).get("market", 0.0) or 0.0) * 100.0,
        "industry_pct_factor_only": float((risk_cache.get("component_shares") or {}).get("industry", 0.0) or 0.0) * 100.0,
        "style_pct_factor_only": float((risk_cache.get("component_shares") or {}).get("style", 0.0) or 0.0) * 100.0,
    }

    # SECTION 2 — Exposure diagnostics (latest cross-section)
    exposure_dates = _load_exposure_dates(data_db)
    exp_as_of = exposure_dates[-1] if exposure_dates else None
    exp_matrix = pd.DataFrame()
    if exp_as_of is not None:
        elig_ctx = build_eligibility_context(data_db, dates=[exp_as_of])
        exp_date, exp_elig = structural_eligibility_for_date(elig_ctx, exp_as_of)
        snap = elig_ctx.exposure_snapshots.get(exp_date or "")
        if snap is not None and not snap.empty and not exp_elig.empty:
            snap_df = snap.reset_index().rename(columns={"index": "ticker"})
            exp_matrix = _build_factor_exposure_matrix(
                snap_df,
                eligibility=exp_elig,
                core_country_codes=HEALTH_CORE_COUNTRY_CODES,
            )
    health_factor_names = {str(name) for name in df_ret["factor_name"].astype(str).tolist()}
    if not exp_matrix.empty:
        health_factor_names.update(str(name) for name in exp_matrix.columns)
    health_factor_catalog = build_factor_catalog_for_factors(
        sorted(health_factor_names),
        method_version=model_method_version,
    )
    health_factor_name_to_id = factor_name_to_id_map(health_factor_catalog)

    def _health_factor_id(factor_name: Any) -> str:
        factor = str(factor_name or "").strip()
        return health_factor_name_to_id.get(factor, factor)

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
                "factor_id": _health_factor_id(f),
                "mean": float(np.mean(finite)),
                "std": float(np.std(finite, ddof=0)),
                "p1": float(np.percentile(finite, 1)),
                "p99": float(np.percentile(finite, 99)),
                "max_abs": float(np.max(np.abs(finite))),
            })
            factor_hists[str(f)] = _hist(finite, bins=30)

        factor_stats = sorted(factor_stats, key=lambda r: str(r["factor_id"]))
        corr_df = exp_matrix.corr().fillna(0.0)
        exp_corr = {
            "factors": [_health_factor_id(c) for c in corr_df.columns],
            "correlation": [[float(v) for v in row] for row in corr_df.to_numpy(dtype=float)],
        }
        turnover_series = _compute_exposure_turnover(
            data_db,
            list(exp_matrix.columns),
            core_country_codes=HEALTH_CORE_COUNTRY_CODES,
            sample_dates=sorted(week_end_str),
        )

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
            factor_id = _health_factor_id(f)
            cumulative[factor_id] = [{"date": _to_date_str(d), "value": float(v)} for d, v in cum.items()]
            rolling_vol_60d[factor_id] = [{"date": _to_date_str(d), "value": float(v if np.isfinite(v) else 0.0)} for d, v in rv.items()]
            return_dist[factor_id] = _hist(s.to_numpy(dtype=float), bins=40)

    ret_corr_df = piv.corr().fillna(0.0) if not piv.empty else pd.DataFrame()
    ret_corr = {
        "factors": [_health_factor_id(c) for c in ret_corr_df.columns],
        "correlation": [[float(v) for v in row] for row in ret_corr_df.to_numpy(dtype=float)],
    }

    # SECTION 4 — Covariance quality
    cov_df = _deserialize_full_covariance(cov_payload)
    cov_factors = [str(f) for f in cov_df.columns]
    cov_mat = cov_df.to_numpy(dtype=float) if not cov_df.empty else np.zeros((0, 0), dtype=float)
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

    # SECTION 5 — Source-of-truth data coverage (canonical PIT fundamentals + classification)
    conn = sqlite3.connect(str(data_db))
    try:
        equity_tickers = _load_equity_tickers(conn)
        price_bounds = _load_equity_price_bounds(conn)

        fundamentals_df = pd.read_sql_query(
            """
            WITH ranked AS (
                SELECT
                    UPPER(sm.ticker) AS ticker,
                    f.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY f.ric, f.as_of_date
                        ORDER BY f.stat_date DESC, f.updated_at DESC
                    ) AS rn
                FROM security_fundamentals_pit f
                JOIN security_master sm
                  ON sm.ric = f.ric
                WHERE COALESCE(sm.classification_ok, 0) = 1
                  AND COALESCE(sm.is_equity_eligible, 0) = 1
            )
            SELECT *
            FROM ranked
            WHERE rn = 1
            """,
            conn,
        )
        fundamentals_df = _filter_active_equity_rows(
            fundamentals_df,
            ticker_col="ticker",
            date_col="as_of_date",
            equity_tickers=equity_tickers,
            price_bounds=price_bounds,
        )
        fundamentals_coverage = _compute_table_field_coverage(
            conn,
            table="security_fundamentals_pit",
            date_col="as_of_date",
            ticker_col="ticker",
            excluded_cols={"ticker", "ric", "as_of_date", "stat_date", "source", "job_run_id", "updated_at"},
            label="Fundamentals PIT History",
            base_df=fundamentals_df,
            use_field_expected_tickers=True,
            scope_note=(
                "Denominator uses equity-eligible canonical names only and each ticker's "
                "observed price-history date range. Field coverage is scored against tickers "
                "that report the field at least once, so structural N/A does not count as missing."
            ),
        )

        trbc_df = pd.read_sql_query(
            """
            WITH ranked AS (
                SELECT
                    UPPER(sm.ticker) AS ticker,
                    c.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY c.ric, c.as_of_date
                        ORDER BY c.updated_at DESC
                    ) AS rn
                FROM security_classification_pit c
                JOIN security_master sm
                  ON sm.ric = c.ric
                WHERE COALESCE(sm.classification_ok, 0) = 1
                  AND COALESCE(sm.is_equity_eligible, 0) = 1
            )
            SELECT *
            FROM ranked
            WHERE rn = 1
            """,
            conn,
        )
        trbc_df = _filter_active_equity_rows(
            trbc_df,
            ticker_col="ticker",
            date_col="as_of_date",
            equity_tickers=equity_tickers,
            price_bounds=price_bounds,
        )
        trbc_coverage = _compute_table_field_coverage(
            conn,
            table="security_classification_pit",
            date_col="as_of_date",
            ticker_col="ticker",
            excluded_cols={"ticker", "ric", "as_of_date", "source", "job_run_id", "updated_at"},
            label="Classification PIT History",
            base_df=trbc_df,
            use_field_expected_tickers=False,
            scope_note=(
                "Denominator uses equity-eligible canonical names only and each ticker's "
                "observed price-history date range."
            ),
        )
    finally:
        conn.close()

    return {
        "status": "ok",
        "as_of": _to_date_str(df_ret["date"].max()),
        "run_id": str(run_id) if run_id else None,
        "snapshot_id": str(snapshot_id) if snapshot_id else None,
        "source_dates": dict(source_dates or {}),
        "factor_catalog": serialize_factor_catalog(health_factor_catalog),
        "notes": [
            "Daily factor t-stat uses stored heteroskedasticity-robust coefficient statistics from the estimator layer.",
            "Incremental block R² is reconstructed from cached full residuals plus canonicalized style-fitted returns.",
            "Computationally intensive Section 1 time-series are sampled at week-end over 10 years.",
            "Exposure turnover is normalized by elapsed calendar days between snapshots, then smoothed with a 60-observation rolling mean.",
            "Section 2 is anchored on the US-core structural universe used by the live estimator (no cap fill-ins, no unmapped industry bucket).",
            "Section 4 forecast-vs-realized uses the full model covariance from risk_engine_cov, not the style-only display correlation matrix.",
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
                {"factor_id": _health_factor_id(r["factor_name"]), "value": float(r["pct_days_abs_t_gt_2"])}
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
            "factor_histograms": {
                _health_factor_id(factor_name): hist
                for factor_name, hist in factor_hists.items()
            },
            "exposure_corr": exp_corr,
            "turnover_series": turnover_series,
        },
        "section3": {
            "factors": [_health_factor_id(f) for f in factors],
            "cumulative_returns": cumulative,
            "rolling_vol_60d": rolling_vol_60d,
            "return_corr": ret_corr,
            "return_dist": return_dist,
        },
        "section4": {
            "eigenvalues": eigvals,
            "forecast_vs_realized": fv,
            "rolling_avg_factor_vol": avg_factor_vol_series,
        },
        "section5": {
            "fundamentals": fundamentals_coverage,
            "trbc_history": trbc_coverage,
        },
    }
