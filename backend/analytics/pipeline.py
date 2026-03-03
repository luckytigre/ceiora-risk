"""Analytics pipeline: fetch → compute → cache."""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import config
from analytics.health import compute_health_diagnostics
from barra.covariance import build_factor_covariance_from_cache
from barra.daily_factor_returns import compute_daily_factor_returns
from barra.descriptors import FULL_STYLE_ORTH_RULES, canonicalize_style_scores
from barra.eligibility import build_eligibility_context, structural_eligibility_for_date
from barra.risk_attribution import (
    STYLE_COLUMN_TO_LABEL,
    portfolio_factor_exposure,
    risk_decomposition,
)
from barra.specific_risk import build_specific_risk_from_cache
from analytics.trbc_economic_sector_short import abbreviate_trbc_economic_sector_short
from cuse4.bootstrap import bootstrap_cuse4_source_tables
from cuse4.estu import build_and_persist_estu_membership
from db import postgres, sqlite
from db.cross_section_snapshot import rebuild_cross_section_snapshot
from portfolio.positions_store import get_position_meta, get_shares, get_tickers
from trading_calendar import previous_or_same_xnys_session

logger = logging.getLogger(__name__)

DATA_DB = Path(config.DATA_DB_PATH)
CACHE_DB = Path(config.SQLITE_PATH)
RISK_ENGINE_METHOD_VERSION = "v1_weekly_recompute_lagged_cross_section_2026_03_02"


def _finite_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    return out if np.isfinite(out) else float(default)


def _parse_iso_date(value: Any) -> date | None:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(str(value)).date()
    except (TypeError, ValueError):
        return None


def _serialize_covariance(cov: pd.DataFrame) -> dict[str, Any]:
    if cov is None or cov.empty:
        return {"factors": [], "matrix": []}
    factors = [str(c) for c in cov.columns]
    mat = cov.reindex(index=factors, columns=factors).to_numpy(dtype=float)
    return {
        "factors": factors,
        "matrix": [[_finite_float(v, 0.0) for v in row] for row in mat.tolist()],
    }


def _deserialize_covariance(payload: Any) -> pd.DataFrame:
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
    if arr.ndim != 2 or arr.shape[0] != len(factors) or arr.shape[1] != len(factors):
        return pd.DataFrame()
    return pd.DataFrame(arr, index=factors, columns=factors)


def _latest_factor_return_date(cache_db: Path) -> str | None:
    conn = sqlite3.connect(str(cache_db))
    try:
        row = conn.execute("SELECT MAX(date) FROM daily_factor_returns").fetchone()
    except sqlite3.OperationalError:
        row = None
    finally:
        conn.close()
    if not row or not row[0]:
        return None
    return str(row[0])


def _risk_recompute_due(meta: dict[str, Any], *, today_utc: date) -> tuple[bool, str]:
    if not meta:
        return True, "missing_meta"
    if str(meta.get("method_version") or "") != RISK_ENGINE_METHOD_VERSION:
        return True, "method_version_change"
    last_recompute = _parse_iso_date(meta.get("last_recompute_date"))
    if last_recompute is None:
        return True, "missing_last_recompute_date"
    interval = max(1, int(config.RISK_RECOMPUTE_INTERVAL_DAYS))
    if (today_utc - last_recompute).days >= interval:
        return True, f"interval_elapsed_{interval}d"
    return False, "within_interval"


def _build_model_sanity_report(
    *,
    risk_shares: dict[str, float],
    factor_details: list[dict[str, Any]],
    eligibility_summary: dict[str, Any],
) -> dict[str, Any]:
    warnings: list[str] = []

    regression_cov = _finite_float(eligibility_summary.get("regression_coverage"), 0.0)
    if regression_cov < 0.20:
        warnings.append(
            f"Low regression coverage on latest usable date: {regression_cov * 100.0:.1f}%."
        )

    drop_pct = _finite_float(eligibility_summary.get("drop_pct_from_prev"), 0.0)
    if drop_pct > 0.10:
        warnings.append(
            f"Eligible universe dropped {drop_pct * 100.0:.1f}% vs previous cross-section."
        )

    industry_pct = _finite_float(risk_shares.get("industry"), 0.0)
    style_pct = _finite_float(risk_shares.get("style"), 0.0)
    if industry_pct > 90.0:
        warnings.append(f"Industry risk share is highly concentrated at {industry_pct:.1f}% of total risk.")
    if style_pct > 90.0:
        warnings.append(f"Style risk share is highly concentrated at {style_pct:.1f}% of total risk.")

    sign_mismatch = 0
    for row in factor_details:
        exp = _finite_float(row.get("exposure"), 0.0)
        sens = _finite_float(row.get("sensitivity"), 0.0)
        if abs(exp) > 1e-12 and abs(sens) > 1e-12 and (exp * sens) < 0:
            sign_mismatch += 1
    if sign_mismatch > 0:
        warnings.append(
            f"{sign_mismatch} factors have exposure/sensitivity sign mismatch; expected same sign."
        )

    return {
        "status": "warn" if warnings else "ok",
        "warnings": warnings,
        "checks": {
            "factor_sign_mismatch_count": int(sign_mismatch),
            "latest_regression_coverage_pct": round(regression_cov * 100.0, 2),
            "latest_structural_eligible_n": int(eligibility_summary.get("structural_eligible_n", 0) or 0),
            "industry_risk_share_pct": round(industry_pct, 2),
            "style_risk_share_pct": round(style_pct, 2),
            "idio_risk_share_pct": round(_finite_float(risk_shares.get("idio"), 0.0), 2),
        },
    }


def _build_universe_ticker_loadings(
    exposures_df: pd.DataFrame,
    fundamentals_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    cov: pd.DataFrame,
    specific_risk_by_ticker: dict[str, dict[str, float | int | str]] | None = None,
) -> dict[str, Any]:
    """Build full-universe cached loadings/risk context keyed by ticker."""
    exposures_df = exposures_df.copy() if exposures_df is not None else pd.DataFrame()
    fundamentals_df = fundamentals_df.copy() if fundamentals_df is not None else pd.DataFrame()
    prices_df = prices_df.copy() if prices_df is not None else pd.DataFrame()

    if not exposures_df.empty:
        exposures_df["ticker"] = exposures_df["ticker"].astype(str).str.upper()
    if not fundamentals_df.empty:
        fundamentals_df["ticker"] = fundamentals_df["ticker"].astype(str).str.upper()
    if not prices_df.empty:
        prices_df["ticker"] = prices_df["ticker"].astype(str).str.upper()

    # Latest price map for whole universe
    price_map: dict[str, float] = {}
    if not prices_df.empty:
        for _, row in prices_df.iterrows():
            ticker = str(row.get("ticker", "")).upper()
            if ticker:
                price_map[ticker] = _finite_float(row.get("close"), 0.0)

    # Fundamentals maps for whole universe
    mcap_map: dict[str, float] = {}
    trbc_economic_sector_short_map: dict[str, str] = {}
    trbc_industry_map: dict[str, str] = {}
    name_map: dict[str, str] = {}
    if not fundamentals_df.empty:
        name_col = None
        for col in ("company_name", "common_name", "name", "Company Common Name"):
            if col in fundamentals_df.columns:
                name_col = col
                break

        for _, row in fundamentals_df.iterrows():
            ticker = str(row.get("ticker", "")).upper()
            if not ticker:
                continue
            mcap = _finite_float(row.get("market_cap"), np.nan)
            if np.isfinite(mcap) and mcap > 0:
                mcap_map[ticker] = mcap
            trbc_economic_sector_short = str(
                row.get("trbc_economic_sector_short")
                or row.get("trbc_sector")
                or ""
            ).strip()
            if trbc_economic_sector_short:
                trbc_economic_sector_short_map[ticker] = trbc_economic_sector_short
            trbc_industry = str(row.get("trbc_industry_group") or "").strip()
            if trbc_industry:
                trbc_industry_map[ticker] = trbc_industry
            if name_col:
                raw_name = row.get(name_col)
                if raw_name is not None:
                    s = str(raw_name).strip()
                    if s and s.lower() != "nan":
                        name_map[ticker] = s

    latest_asof = ""
    if "as_of_date" in exposures_df.columns and not exposures_df.empty:
        latest_asof = str(exposures_df["as_of_date"].astype(str).max())

    eligibility_df = pd.DataFrame()
    if latest_asof:
        elig_ctx = build_eligibility_context(DATA_DB, dates=[latest_asof])
        _, eligibility_df = structural_eligibility_for_date(elig_ctx, latest_asof)

    eligible_mask = eligibility_df.get("is_structural_eligible", pd.Series(dtype=bool)).astype(bool)
    eligible_tickers = set(eligibility_df.index[eligible_mask].astype(str))
    ineligible_reason = {
        str(t): str(eligibility_df.loc[t, "exclusion_reason"] or "")
        for t in eligibility_df.index
    }

    # Canonicalize style scores on the structurally eligible cross-section only.
    canonical_style_map: dict[str, dict[str, float]] = {}
    style_cols_present = [c for c in STYLE_COLUMN_TO_LABEL if c in exposures_df.columns] if not exposures_df.empty else []
    if not exposures_df.empty and style_cols_present and eligible_tickers:
        style_names = [STYLE_COLUMN_TO_LABEL[c] for c in style_cols_present]
        style_scores = exposures_df[["ticker", *style_cols_present]].copy()
        style_scores["ticker"] = style_scores["ticker"].astype(str).str.upper()
        style_scores = style_scores[style_scores["ticker"].isin(eligible_tickers)]
        style_scores = style_scores.drop_duplicates(subset=["ticker"], keep="last").set_index("ticker")
        style_scores.columns = style_names
        if not style_scores.empty:
            caps_from_elig = pd.to_numeric(
                eligibility_df.reindex(style_scores.index)["market_cap"],
                errors="coerce",
            )
            industries_from_elig = (
                eligibility_df.reindex(style_scores.index)["trbc_industry_group"]
                .fillna("")
                .astype(str)
            )
            valid = (
                style_scores.notna().all(axis=1).to_numpy(dtype=bool)
                & np.isfinite(style_scores.to_numpy(dtype=float)).all(axis=1)
                & np.isfinite(caps_from_elig.to_numpy(dtype=float))
                & (caps_from_elig.to_numpy(dtype=float) > 0.0)
                & (industries_from_elig.str.len().to_numpy(dtype=float) > 0)
            )
            if int(valid.sum()) > 0:
                valid_idx = style_scores.index[valid]
                style_scores = style_scores.loc[valid_idx]
                caps_from_elig = caps_from_elig.loc[valid_idx]
                industries_from_elig = industries_from_elig.loc[valid_idx]
                industry_dummies = pd.get_dummies(industries_from_elig, dtype=float)
                canonical_scores = canonicalize_style_scores(
                    style_scores=style_scores,
                    market_caps=caps_from_elig,
                    orth_rules=FULL_STYLE_ORTH_RULES,
                    industry_exposures=industry_dummies,
                )
                for ticker, row in canonical_scores.iterrows():
                    canonical_style_map[str(ticker).upper()] = {
                        factor: _finite_float(row.get(factor), 0.0)
                        for factor in canonical_scores.columns
                    }

    # Factor vol map from full-universe covariance
    factor_vol_map: dict[str, float] = {}
    if cov is not None and not cov.empty:
        for factor in cov.columns:
            factor_vol_map[str(factor)] = float(np.sqrt(max(0.0, _finite_float(cov.loc[factor, factor], 0.0))))

    all_tickers = sorted(
        {
            *exposures_df.get("ticker", pd.Series(dtype=str)).astype(str).str.upper().tolist(),
            *fundamentals_df.get("ticker", pd.Series(dtype=str)).astype(str).str.upper().tolist(),
            *prices_df.get("ticker", pd.Series(dtype=str)).astype(str).str.upper().tolist(),
        }
    )
    universe_by_ticker: dict[str, dict[str, Any]] = {}
    for ticker in all_tickers:
        if not ticker:
            continue

        eligible = bool(ticker in eligible_tickers)
        trbc_economic_sector_short = str(
            (
                eligibility_df.loc[ticker, "trbc_economic_sector_short"]
                if ticker in eligibility_df.index and "trbc_economic_sector_short" in eligibility_df.columns
                else (
                    eligibility_df.loc[ticker, "trbc_sector"]
                    if ticker in eligibility_df.index and "trbc_sector" in eligibility_df.columns
                    else ""
                )
            )
            or trbc_economic_sector_short_map.get(ticker, "")
        )
        trbc_industry_group = str(
            (eligibility_df.loc[ticker, "trbc_industry_group"] if ticker in eligibility_df.index else "")
            or trbc_industry_map.get(ticker, "")
        )
        market_cap = _finite_float(
            eligibility_df.loc[ticker, "market_cap"] if ticker in eligibility_df.index else mcap_map.get(ticker),
            np.nan,
        )

        exposures: dict[str, float] = {}
        if eligible and ticker in canonical_style_map:
            exposures.update(canonical_style_map[ticker])
            if trbc_industry_group:
                exposures[trbc_industry_group] = 1.0

        sensitivities = {
            factor: round(_finite_float(exposures.get(factor), 0.0) * _finite_float(vol, 0.0), 6)
            for factor, vol in factor_vol_map.items()
        }
        risk_loading = round(float(sum(abs(v) for v in sensitivities.values())), 6) if eligible else None
        spec = (specific_risk_by_ticker or {}).get(ticker, {})
        spec_var = _finite_float(spec.get("specific_var"), np.nan) if eligible else np.nan
        spec_vol = _finite_float(spec.get("specific_vol"), np.nan) if eligible else np.nan

        universe_by_ticker[ticker] = {
            "ticker": ticker,
            "name": name_map.get(ticker, ""),
            "trbc_economic_sector_short": trbc_economic_sector_short,
            "trbc_economic_sector_short_abbr": abbreviate_trbc_economic_sector_short(trbc_economic_sector_short),
            "trbc_industry_group": trbc_industry_group,
            "market_cap": round(float(market_cap), 2) if np.isfinite(market_cap) else None,
            "price": round(_finite_float(price_map.get(ticker), 0.0), 4),
            "exposures": exposures,
            "sensitivities": sensitivities,
            "risk_loading": risk_loading,
            "specific_var": round(spec_var, 8) if np.isfinite(spec_var) else None,
            "specific_vol": round(spec_vol, 6) if np.isfinite(spec_vol) else None,
            "eligible_for_model": eligible,
            "eligibility_reason": "" if eligible else ineligible_reason.get(ticker, "ineligible"),
            "model_warning": "" if eligible else "Ticker is ineligible for strict equity model; analytics shown as N/A.",
            "as_of_date": latest_asof,
        }

    # Lightweight search index for instant lookup
    search_index = [
        {
            "ticker": t,
            "name": d.get("name", ""),
            "trbc_economic_sector_short": d.get("trbc_economic_sector_short", ""),
            "trbc_economic_sector_short_abbr": d.get(
                "trbc_economic_sector_short_abbr",
                d.get("trbc_sector_abbr", ""),
            ),
            "trbc_industry_group": d.get("trbc_industry_group", ""),
            "risk_loading": d.get("risk_loading", 0.0),
            "specific_vol": d.get("specific_vol", None),
            "eligible_for_model": bool(d.get("eligible_for_model", False)),
            "eligibility_reason": str(d.get("eligibility_reason") or ""),
        }
        for t, d in universe_by_ticker.items()
    ]
    search_index.sort(key=lambda x: str(x["ticker"]))

    eligible_count = int(sum(1 for d in universe_by_ticker.values() if bool(d.get("eligible_for_model", False))))
    return {
        "ticker_count": len(universe_by_ticker),
        "eligible_ticker_count": eligible_count,
        "factor_count": len(factor_vol_map),
        "factors": sorted(factor_vol_map.keys()),
        "factor_vols": {k: round(v, 6) for k, v in factor_vol_map.items()},
        "index": search_index,
        "by_ticker": universe_by_ticker,
    }


def _build_positions_from_universe(universe_by_ticker: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], float]:
    """Project held positions from full-universe cached analytics."""
    shares_map = get_shares()
    tickers = get_tickers()

    positions: list[dict[str, Any]] = []
    total_value = 0.0
    for ticker in tickers:
        t = ticker.upper()
        base = universe_by_ticker.get(t, {})
        shares = _finite_float(shares_map.get(t, 0.0), 0.0)
        price = _finite_float(base.get("price"), 0.0)
        mv = shares * price
        total_value += mv
        meta = get_position_meta(t)
        positions.append({
            "ticker": t,
            "name": str(base.get("name") or ""),
            "long_short": "LONG" if shares >= 0 else "SHORT",
            "shares": shares,
            "price": round(price, 2),
            "market_value": round(mv, 2),
            "weight": 0.0,
            "trbc_economic_sector_short": str(
                base.get("trbc_economic_sector_short")
                or base.get("trbc_sector")
                or ""
            ),
            "trbc_economic_sector_short_abbr": str(
                base.get("trbc_economic_sector_short_abbr")
                or base.get("trbc_sector_abbr")
                or ""
            ),
            "account": meta["account"],
            "sleeve": meta["sleeve"],
            "source": meta["source"],
            "trbc_industry_group": str(base.get("trbc_industry_group") or ""),
            "exposures": dict(base.get("exposures") or {}),
            "specific_var": (
                _finite_float(base.get("specific_var"), 0.0)
                if np.isfinite(_finite_float(base.get("specific_var"), np.nan))
                else None
            ),
            "specific_vol": (
                _finite_float(base.get("specific_vol"), 0.0)
                if np.isfinite(_finite_float(base.get("specific_vol"), np.nan))
                else None
            ),
            "risk_contrib_pct": 0.0,
            "eligible_for_model": bool(base.get("eligible_for_model", False)),
            "eligibility_reason": str(base.get("eligibility_reason") or ""),
        })

    for pos in positions:
        mv = _finite_float(pos.get("market_value"), 0.0)
        pos["weight"] = round(mv / total_value, 6) if total_value != 0 else 0.0

    return positions, round(total_value, 2)


def _load_latest_factor_coverage(cache_db: Path) -> tuple[str | None, dict[str, dict[str, float | int]]]:
    """Load latest per-factor cross-section coverage stats from cache DB."""
    conn = sqlite3.connect(str(cache_db))
    try:
        latest_row = conn.execute("SELECT MAX(date) FROM daily_factor_returns").fetchone()
        latest = str(latest_row[0]) if latest_row and latest_row[0] else None
        if latest is None:
            return None, {}
        rows = conn.execute(
            """
            SELECT factor_name, cross_section_n, eligible_n, coverage
            FROM daily_factor_returns
            WHERE date = ?
            """,
            (latest,),
        ).fetchall()
    except sqlite3.OperationalError:
        # Backward-compat if cache schema doesn't have coverage columns yet.
        return None, {}
    finally:
        conn.close()

    out: dict[str, dict[str, float | int]] = {}
    for factor_name, cross_n, eligible_n, coverage in rows:
        out[str(factor_name)] = {
            "cross_section_n": int(cross_n or 0),
            "eligible_n": int(eligible_n or 0),
            "coverage_pct": float(coverage or 0.0),
        }
    return latest, out


def _load_latest_eligibility_summary(cache_db: Path) -> dict[str, Any]:
    conn = sqlite3.connect(str(cache_db))
    try:
        row = conn.execute(
            """
            SELECT date, exp_date, exposure_n, structural_eligible_n, regression_member_n,
                   structural_coverage, regression_coverage, drop_pct_from_prev, alert_level
            FROM daily_universe_eligibility_summary
            WHERE regression_member_n > 0
            ORDER BY date DESC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            row = conn.execute(
                """
                SELECT date, exp_date, exposure_n, structural_eligible_n, regression_member_n,
                       structural_coverage, regression_coverage, drop_pct_from_prev, alert_level
                FROM daily_universe_eligibility_summary
                ORDER BY date DESC
                LIMIT 1
                """
            ).fetchone()
    except sqlite3.OperationalError:
        row = None
    finally:
        conn.close()
    if not row:
        return {"status": "no-data"}
    return {
        "status": "ok",
        "date": str(row[0]),
        "exp_date": str(row[1]) if row[1] is not None else None,
        "exposure_n": int(row[2] or 0),
        "structural_eligible_n": int(row[3] or 0),
        "regression_member_n": int(row[4] or 0),
        "structural_coverage": float(row[5] or 0.0),
        "regression_coverage": float(row[6] or 0.0),
        "drop_pct_from_prev": float(row[7] or 0.0),
        "alert_level": str(row[8] or ""),
    }


def _compute_exposures_modes(
    positions: list[dict[str, Any]],
    cov,
    factor_details: list[dict],
    factor_coverage: dict[str, dict[str, float | int]] | None = None,
    coverage_date: str | None = None,
) -> dict[str, Any]:
    """Compute the 3-mode exposure data for all factors."""
    # Gather all factor names
    all_factors: set[str] = set()
    for pos in positions:
        all_factors.update(pos.get("exposures", {}).keys())

    factor_detail_map = {d["factor"]: d for d in factor_details}
    coverage_map = factor_coverage or {}
    exposure_map = {factor: portfolio_factor_exposure(positions, factor) for factor in all_factors}

    # Covariance-aware marginal scalar per factor: (F @ h)_f
    cov_adj_map: dict[str, float] = {}
    if cov is not None and not cov.empty:
        cov_factors = [str(c) for c in cov.columns if str(c).lower() != "market"]
        if cov_factors:
            h_vec = np.array([_finite_float(exposure_map.get(f), 0.0) for f in cov_factors], dtype=float)
            f_mat = cov.reindex(index=cov_factors, columns=cov_factors).to_numpy(dtype=float)
            fh_vec = f_mat @ h_vec
            cov_adj_map = {
                cov_factors[i]: _finite_float(fh_vec[i], 0.0)
                for i in range(len(cov_factors))
            }

    result: dict[str, list[dict]] = {"raw": [], "sensitivity": [], "risk_contribution": []}

    for factor in sorted(all_factors):
        # Raw portfolio-weighted exposure
        raw_exp = _finite_float(exposure_map.get(factor), 0.0)

        # Factor vol from cov matrix
        detail = factor_detail_map.get(factor)
        factor_vol = _finite_float(detail.get("factor_vol"), 0.0) if detail else 0.0
        # 1-sigma scaled factor exposure.
        sensitivity = _finite_float(detail.get("sensitivity"), raw_exp * factor_vol) if detail else raw_exp * factor_vol
        # Contribution to total portfolio variance (%) from risk decomposition.
        risk_pct = _finite_float(detail.get("pct_of_total"), 0.0) if detail else 0.0
        # Factor-level marginal variance contribution (includes cross-factor correlations).
        marginal_var = _finite_float(detail.get("marginal_var_contrib"), 0.0) if detail else 0.0
        cov_adj = _finite_float(cov_adj_map.get(factor), 0.0)

        # Position-level drilldown
        drilldown_raw = []
        drilldown_sens = []
        drilldown_risk = []
        for pos in positions:
            pos_exp = _finite_float(pos.get("exposures", {}).get(factor, 0.0), 0.0)
            if abs(pos_exp) > 1e-8:
                weight = _finite_float(pos.get("weight", 0.0), 0.0)
                raw_contrib = weight * pos_exp
                pos_sens = pos_exp * factor_vol
                sens_contrib = weight * pos_sens
                # Variance-units position contribution for this factor:
                # (w_i * x_{i,f}) * (F @ h)_f
                risk_var_contrib = weight * pos_exp * cov_adj
                # Scale to displayed risk % so row sums align with bar value.
                if abs(marginal_var) > 1e-12:
                    risk_pct_contrib = (risk_var_contrib / marginal_var) * risk_pct
                else:
                    risk_pct_contrib = 0.0
                drilldown_raw.append({
                    "ticker": pos["ticker"],
                    "weight": weight,
                    "exposure": round(pos_exp, 4),
                    "contribution": round(raw_contrib, 6),
                })
                drilldown_sens.append({
                    "ticker": pos["ticker"],
                    "weight": weight,
                    "exposure": round(pos_exp, 4),
                    "sensitivity": round(pos_sens, 6),
                    "contribution": round(sens_contrib, 6),
                })
                drilldown_risk.append({
                    "ticker": pos["ticker"],
                    "weight": weight,
                    "exposure": round(pos_exp, 4),
                    # Covariance-adjusted loading scalar (not yet weight-scaled).
                    "sensitivity": round(pos_exp * cov_adj, 8),
                    # Percent contribution to total portfolio variance for this factor.
                    "contribution": round(risk_pct_contrib, 8),
                })

        fv_rounded = round(factor_vol, 6)
        cov_stats = coverage_map.get(factor, {})
        cross_section_n = int(cov_stats.get("cross_section_n", 0) or 0)
        eligible_n = int(cov_stats.get("eligible_n", 0) or 0)
        coverage_pct = float(cov_stats.get("coverage_pct", 0.0) or 0.0)
        result["raw"].append({
            "factor": factor,
            "value": round(raw_exp, 6),
            "factor_vol": fv_rounded,
            "cross_section_n": cross_section_n,
            "eligible_n": eligible_n,
            "coverage_pct": round(coverage_pct, 6),
            "coverage_date": coverage_date,
            "drilldown": drilldown_raw,
        })
        result["sensitivity"].append({
            "factor": factor,
            "value": round(sensitivity, 6),
            "factor_vol": fv_rounded,
            "cross_section_n": cross_section_n,
            "eligible_n": eligible_n,
            "coverage_pct": round(coverage_pct, 6),
            "coverage_date": coverage_date,
            "drilldown": drilldown_sens,
        })
        result["risk_contribution"].append({
            "factor": factor,
            "value": round(risk_pct, 4),
            "factor_vol": fv_rounded,
            "cross_section_n": cross_section_n,
            "eligible_n": eligible_n,
            "coverage_pct": round(coverage_pct, 6),
            "coverage_date": coverage_date,
            "drilldown": drilldown_risk,
        })

    return result


def _compute_position_risk_mix(
    positions: list[dict[str, Any]],
    cov,
    specific_risk_by_ticker: dict[str, dict[str, float | int | str]] | None = None,
) -> dict[str, dict[str, float]]:
    """Per-position risk split using Barra-style factor + specific variance."""
    if cov is None or cov.empty:
        out: dict[str, dict[str, float]] = {}
        for pos in positions:
            ticker = str(pos.get("ticker", "")).upper()
            if ticker:
                out[ticker] = {"industry": 0.0, "style": 0.0, "idio": 0.0}
        return out

    factors = [str(c) for c in cov.columns if str(c).lower() != "market"]
    if not factors:
        return {}
    idx = {f: i for i, f in enumerate(factors)}
    f_mat = cov.reindex(index=factors, columns=factors).to_numpy(dtype=float)
    style_idx = [idx[f] for f in factors if f in STYLE_COLUMN_TO_LABEL.values()]
    industry_idx = [idx[f] for f in factors if f not in STYLE_COLUMN_TO_LABEL.values()]

    spec_map = specific_risk_by_ticker or {}
    out: dict[str, dict[str, float]] = {}
    for pos in positions:
        ticker = str(pos.get("ticker", "")).upper()
        if not ticker:
            continue
        exps = pos.get("exposures", {}) or {}
        x = np.array([_finite_float(exps.get(f), 0.0) for f in factors], dtype=float)

        # Decompose systematic variance into industry/style blocks and allocate cross term.
        var_ind = 0.0
        var_style = 0.0
        var_cross = 0.0
        if industry_idx:
            xi = x[industry_idx]
            fii = f_mat[np.ix_(industry_idx, industry_idx)]
            var_ind = _finite_float(float(xi.T @ fii @ xi), 0.0)
        if style_idx:
            xs = x[style_idx]
            fss = f_mat[np.ix_(style_idx, style_idx)]
            var_style = _finite_float(float(xs.T @ fss @ xs), 0.0)
        if industry_idx and style_idx:
            xi = x[industry_idx]
            xs = x[style_idx]
            fis = f_mat[np.ix_(industry_idx, style_idx)]
            var_cross = _finite_float(float(2.0 * xi.T @ fis @ xs), 0.0)

        base_ind = max(0.0, var_ind)
        base_style = max(0.0, var_style)
        denom = base_ind + base_style
        if abs(var_cross) <= 1e-12:
            cross_ind = 0.0
            cross_style = 0.0
        elif denom <= 1e-12:
            cross_ind = 0.5 * var_cross
            cross_style = 0.5 * var_cross
        else:
            w_ind = base_ind / denom
            w_style = base_style / denom
            cross_ind = w_ind * var_cross
            cross_style = w_style * var_cross

        sys_ind = max(0.0, base_ind + cross_ind)
        sys_style = max(0.0, base_style + cross_style)
        spec_var = _finite_float(spec_map.get(ticker, {}).get("specific_var"), 0.0)
        if spec_var < 0:
            spec_var = 0.0

        total = sys_ind + sys_style + spec_var
        if total <= 0:
            out[ticker] = {"industry": 0.0, "style": 0.0, "idio": 0.0}
            continue

        ind_pct = 100.0 * sys_ind / total
        style_pct = 100.0 * sys_style / total
        idio_pct = 100.0 * spec_var / total

        # Normalize after clipping to keep sums at exactly 100.
        ind_pct = max(0.0, ind_pct)
        style_pct = max(0.0, style_pct)
        idio_pct = max(0.0, idio_pct)
        s = ind_pct + style_pct + idio_pct
        if s > 0:
            k = 100.0 / s
            ind_pct *= k
            style_pct *= k
            idio_pct *= k

        out[ticker] = {
            "industry": round(ind_pct, 2),
            "style": round(style_pct, 2),
            "idio": round(idio_pct, 2),
        }
    return out


def run_refresh(
    *,
    force_risk_recompute: bool = False,
    mode: str = "full",
) -> dict[str, Any]:
    """Pipeline refresh with two modes:
    - full: weekly-gated risk engine + all downstream caches (including health diagnostics)
    - light: fast cache refresh path; skips health diagnostics and avoids risk recompute
      unless core risk caches are missing.
    """
    logger.info("Starting refresh pipeline...")
    refresh_mode = str(mode or "full").strip().lower()
    if refresh_mode not in {"full", "light"}:
        refresh_mode = "full"
    light_mode = refresh_mode == "light"

    refresh_started_at = datetime.now(timezone.utc).isoformat()
    today_utc = datetime.fromisoformat(
        previous_or_same_xnys_session(datetime.now(timezone.utc).date().isoformat())
    ).date()

    logger.info("Rebuilding canonical cross-section snapshot...")
    snapshot_build = rebuild_cross_section_snapshot(DATA_DB)

    # 1. Fetch full-universe data from local data.db
    logger.info("Fetching data from local database...")
    source_dates = postgres.load_source_dates()
    fundamentals_asof = source_dates.get("exposures_asof")
    prices_universe_df = postgres.load_latest_prices()
    fundamentals_universe_df = postgres.load_fundamental_snapshots(
        as_of_date=str(fundamentals_asof) if fundamentals_asof else None,
    )
    exposures_universe_df = postgres.load_raw_cross_section_latest()

    # Optional cUSE4 foundation maintenance (additive, non-breaking).
    cuse4_foundation: dict[str, Any] = {"status": "disabled"}
    if bool(config.CUSE4_ENABLE_ESTU_AUDIT):
        cuse4_bootstrap: dict[str, Any] | None = None
        cuse4_estu: dict[str, Any] | None = None
        try:
            if bool(config.CUSE4_AUTO_BOOTSTRAP):
                cuse4_bootstrap = bootstrap_cuse4_source_tables(
                    db_path=DATA_DB,
                    replace_all=True,
                )
            estu_asof = (
                source_dates.get("fundamentals_asof")
                or source_dates.get("exposures_asof")
                or today_utc.isoformat()
            )
            cuse4_estu = build_and_persist_estu_membership(
                db_path=DATA_DB,
                as_of_date=str(estu_asof),
            )
            cuse4_foundation = {
                "status": "ok",
                "bootstrap": cuse4_bootstrap,
                "estu": cuse4_estu,
            }
        except Exception as exc:  # noqa: BLE001
            logger.exception("cUSE4 foundation update failed")
            cuse4_foundation = {
                "status": "error",
                "error": {"type": type(exc).__name__, "message": str(exc)},
                "bootstrap": cuse4_bootstrap,
                "estu": cuse4_estu,
            }

    # 2. Weekly risk-engine recompute gate.
    risk_engine_meta = sqlite.cache_get("risk_engine_meta") or {}
    should_recompute, recompute_reason = _risk_recompute_due(risk_engine_meta, today_utc=today_utc)
    if light_mode:
        should_recompute = False
        recompute_reason = "light_mode_skip"
    if force_risk_recompute:
        should_recompute = True
        recompute_reason = "force_risk_recompute"

    cov = _deserialize_covariance(sqlite.cache_get("risk_engine_cov"))
    cached_specific = sqlite.cache_get("risk_engine_specific_risk")
    specific_risk_by_ticker = cached_specific if isinstance(cached_specific, dict) else {}
    latest_r2 = _finite_float(risk_engine_meta.get("latest_r2"), 0.0)

    if cov.empty:
        should_recompute = True
        recompute_reason = "missing_covariance_cache"
    if not isinstance(cached_specific, dict):
        should_recompute = True
        recompute_reason = "missing_specific_risk_cache"

    recomputed_this_refresh = False
    if should_recompute:
        logger.info(
            "Recomputing risk engine (%s): daily factor returns -> covariance -> specific risk",
            recompute_reason,
        )
        compute_daily_factor_returns(
            DATA_DB,
            CACHE_DB,
            min_cross_section_age_days=config.CROSS_SECTION_MIN_AGE_DAYS,
        )
        cov, latest_r2 = build_factor_covariance_from_cache(
            CACHE_DB, lookback_days=config.LOOKBACK_DAYS
        )
        specific_risk_by_ticker = build_specific_risk_from_cache(
            CACHE_DB,
            lookback_days=config.LOOKBACK_DAYS,
        )
        risk_engine_meta = {
            "status": "ok",
            "method_version": RISK_ENGINE_METHOD_VERSION,
            "last_recompute_date": today_utc.isoformat(),
            "factor_returns_latest_date": _latest_factor_return_date(CACHE_DB),
            "lookback_days": int(config.LOOKBACK_DAYS),
            "cross_section_min_age_days": int(config.CROSS_SECTION_MIN_AGE_DAYS),
            "recompute_interval_days": int(config.RISK_RECOMPUTE_INTERVAL_DAYS),
            "latest_r2": float(latest_r2),
            "specific_risk_ticker_count": int(len(specific_risk_by_ticker)),
        }
        sqlite.cache_set("risk_engine_cov", _serialize_covariance(cov))
        sqlite.cache_set("risk_engine_specific_risk", specific_risk_by_ticker)
        sqlite.cache_set("risk_engine_meta", risk_engine_meta)
        recomputed_this_refresh = True
    else:
        logger.info(
            "Skipping risk-engine recompute (%s). Reusing cached covariance/specific risk.",
            recompute_reason,
        )

    # 3. Build/cached full-universe loadings first (portfolio is a final projection only).
    logger.info("Building full-universe ticker loadings...")
    universe_loadings = _build_universe_ticker_loadings(
        exposures_universe_df,
        fundamentals_universe_df,
        prices_universe_df,
        cov,
        specific_risk_by_ticker=specific_risk_by_ticker,
    )

    # 4. Project held positions from full-universe cache
    logger.info("Projecting held positions from full-universe cache...")
    positions, total_value = _build_positions_from_universe(universe_loadings["by_ticker"])

    # 5. Risk decomposition
    logger.info("Computing risk decomposition...")
    risk_shares, component_shares, factor_details = risk_decomposition(
        cov=cov,
        positions=positions,
        specific_risk_by_ticker=specific_risk_by_ticker,
    )
    position_risk_mix = _compute_position_risk_mix(
        positions=positions,
        cov=cov,
        specific_risk_by_ticker=specific_risk_by_ticker,
    )

    # 6. Compute per-position risk contributions
    for pos in positions:
        exps = pos.get("exposures", {})
        risk_score = sum(
            abs(float(exps.get(d["factor"], 0.0)) * d["sensitivity"])
            for d in factor_details
        )
        pos["risk_contrib_pct"] = round(risk_score * pos["weight"] * 100, 2)
        pos["risk_mix"] = dict(position_risk_mix.get(str(pos.get("ticker", "")).upper(), {
            "industry": 0.0,
            "style": 0.0,
            "idio": 0.0,
        }))

    # 7. Compute condition number from cov matrix
    condition_number = 0.0
    if not cov.empty:
        try:
            cn = float(np.linalg.cond(cov.to_numpy()))
            condition_number = cn if np.isfinite(cn) else 9999.99
        except Exception:
            pass

    # 8. Compute exposure modes
    logger.info("Computing exposure modes...")
    coverage_date, factor_coverage = _load_latest_factor_coverage(CACHE_DB)
    exposure_modes = _compute_exposures_modes(
        positions,
        cov,
        factor_details,
        factor_coverage=factor_coverage,
        coverage_date=coverage_date,
    )

    # 9. Build covariance matrix for frontend (correlation) — style factors only
    STYLE_FACTOR_NAMES = {
        "Size", "Nonlinear Size", "Liquidity", "Beta",
        "Book-to-Price", "Earnings Yield", "Value", "Leverage",
        "Growth", "Profitability", "Investment", "Dividend Yield",
        "Momentum", "Short-Term Reversal", "Residual Volatility",
    }
    cov_matrix: dict[str, Any] = {}
    if not cov.empty:
        all_factors = list(cov.columns)
        style_idx = [i for i, f in enumerate(all_factors) if f in STYLE_FACTOR_NAMES]
        if style_idx:
            style_factors = [all_factors[i] for i in style_idx]
            sub_cov = cov.to_numpy()[np.ix_(style_idx, style_idx)]
            stds = np.sqrt(np.diag(sub_cov))
            stds[stds == 0] = 1.0
            corr = sub_cov / np.outer(stds, stds)
            corr = np.clip(corr, -1.0, 1.0)
            cov_matrix = {
                "factors": style_factors,
                "correlation": [[round(float(v), 4) for v in row] for row in corr],
            }

    # 10. Sanitize non-finite floats (NaN/Inf break JSON serialization)
    def _safe(v):
        if isinstance(v, float) and not np.isfinite(v):
            return 0.0
        return v

    for d in factor_details:
        for k, v in d.items():
            d[k] = _safe(v)
    for k in risk_shares:
        risk_shares[k] = _safe(risk_shares[k])
    for k in component_shares:
        component_shares[k] = _safe(component_shares[k])

    # 11. Cache everything
    logger.info("Caching results...")
    risk_engine_state = {
        "status": str(risk_engine_meta.get("status") or "unknown"),
        "method_version": str(risk_engine_meta.get("method_version") or ""),
        "last_recompute_date": str(risk_engine_meta.get("last_recompute_date") or ""),
        "factor_returns_latest_date": risk_engine_meta.get("factor_returns_latest_date"),
        "cross_section_min_age_days": int(risk_engine_meta.get("cross_section_min_age_days") or 0),
        "recompute_interval_days": int(risk_engine_meta.get("recompute_interval_days") or 0),
        "lookback_days": int(risk_engine_meta.get("lookback_days") or 0),
        "specific_risk_ticker_count": int(risk_engine_meta.get("specific_risk_ticker_count") or 0),
        "recomputed_this_refresh": bool(recomputed_this_refresh),
        "recompute_reason": str(recompute_reason),
    }
    portfolio_data = {
        "positions": positions,
        "total_value": round(total_value, 2),
        "position_count": len(positions),
        "refresh_started_at": refresh_started_at,
        "source_dates": source_dates,
    }
    sqlite.cache_set("portfolio", portfolio_data)

    risk_data = {
        "risk_shares": risk_shares,
        "component_shares": component_shares,
        "factor_details": factor_details,
        "cov_matrix": cov_matrix,
        "r_squared": round(latest_r2, 4),
        "condition_number": round(condition_number, 2),
        "risk_engine": risk_engine_state,
        "refresh_started_at": refresh_started_at,
    }
    sqlite.cache_set("risk", risk_data)

    universe_loadings["risk_engine"] = risk_engine_state
    universe_loadings["refresh_started_at"] = refresh_started_at
    universe_loadings["source_dates"] = source_dates
    sqlite.cache_set("universe_loadings", universe_loadings)
    sqlite.cache_set(
        "universe_factors",
        {
            "factors": universe_loadings.get("factors", []),
            "factor_vols": universe_loadings.get("factor_vols", {}),
            "r_squared": round(latest_r2, 4),
            "condition_number": round(condition_number, 2),
            "ticker_count": universe_loadings.get("ticker_count", 0),
            "eligible_ticker_count": universe_loadings.get("eligible_ticker_count", 0),
            "risk_engine": risk_engine_state,
            "refresh_started_at": refresh_started_at,
        },
    )
    sqlite.cache_set("exposures", exposure_modes)
    eligibility_summary = _load_latest_eligibility_summary(CACHE_DB)
    sqlite.cache_set("eligibility", eligibility_summary)
    sanity = _build_model_sanity_report(
        risk_shares=risk_shares,
        factor_details=factor_details,
        eligibility_summary=eligibility_summary,
    )
    sqlite.cache_set("model_sanity", sanity)
    sqlite.cache_set("cuse4_foundation", cuse4_foundation)
    health_refreshed = False
    existing_health = sqlite.cache_get("health_diagnostics")
    light_mode_health_missing = (
        light_mode
        and (
            not isinstance(existing_health, dict)
            or not isinstance(existing_health.get("section5"), dict)
            or not isinstance(existing_health.get("section5", {}).get("fundamentals"), dict)
            or not isinstance(existing_health.get("section5", {}).get("trbc_history"), dict)
            or not isinstance(existing_health.get("section5", {}).get("fundamentals", {}).get("fields"), list)
            or not isinstance(existing_health.get("section5", {}).get("trbc_history", {}).get("fields"), list)
        )
    )
    if (not light_mode) or light_mode_health_missing:
        sqlite.cache_set("health_diagnostics", compute_health_diagnostics(DATA_DB, CACHE_DB))
        health_refreshed = True
    sqlite.cache_set(
        "refresh_meta",
        {
            "status": "ok",
            "mode": refresh_mode,
            "refresh_started_at": refresh_started_at,
            "source_dates": source_dates,
            "cross_section_snapshot": snapshot_build,
            "risk_engine": risk_engine_state,
            "model_sanity_status": sanity.get("status"),
            "cuse4_foundation": cuse4_foundation,
            "health_refreshed": bool(health_refreshed),
        },
    )

    logger.info("Refresh complete.")
    return {
        "status": "ok",
        "positions": len(positions),
        "total_value": round(total_value, 2),
        "mode": refresh_mode,
        "cross_section_snapshot": snapshot_build,
        "risk_engine": risk_engine_state,
        "model_sanity": sanity,
        "cuse4_foundation": cuse4_foundation,
        "health_refreshed": bool(health_refreshed),
    }
