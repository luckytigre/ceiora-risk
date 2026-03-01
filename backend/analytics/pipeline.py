"""Analytics pipeline: fetch → compute → cache."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import config
from barra.covariance import build_factor_covariance_from_cache
from barra.daily_factor_returns import compute_daily_factor_returns
from barra.descriptors import FULL_STYLE_ORTH_RULES, canonicalize_style_scores
from barra.risk_attribution import (
    STYLE_COLUMN_TO_LABEL,
    portfolio_factor_exposure,
    risk_decomposition,
)
from barra.specific_risk import build_specific_risk_from_cache
from db import postgres, sqlite
from portfolio.mock_portfolio import get_position_meta, get_shares, get_tickers

logger = logging.getLogger(__name__)

DATA_DB = Path(__file__).resolve().parent.parent / "data.db"
CACHE_DB = Path(config.SQLITE_PATH)


def _finite_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    return out if np.isfinite(out) else float(default)


def _build_universe_ticker_loadings(
    exposures_df: pd.DataFrame,
    fundamentals_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    cov: pd.DataFrame,
    specific_risk_by_ticker: dict[str, dict[str, float | int | str]] | None = None,
) -> dict[str, Any]:
    """Build full-universe cached loadings/risk context keyed by ticker."""
    # Latest price map for whole universe
    price_map: dict[str, float] = {}
    if prices_df is not None and not prices_df.empty:
        for _, row in prices_df.iterrows():
            ticker = str(row.get("ticker", "")).upper()
            if ticker:
                price_map[ticker] = _finite_float(row.get("close"), 0.0)

    # Fundamentals maps for whole universe
    mcap_map: dict[str, float] = {}
    sector_map: dict[str, str] = {}
    name_map: dict[str, str] = {}
    if fundamentals_df is not None and not fundamentals_df.empty:
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
            if "sector" in fundamentals_df.columns:
                sector_map[ticker] = str(row.get("sector") or "")
            if name_col:
                raw_name = row.get(name_col)
                if raw_name is not None:
                    s = str(raw_name).strip()
                    if s and s.lower() != "nan":
                        name_map[ticker] = s

    cap_vals = np.array([v for v in mcap_map.values() if np.isfinite(v) and v > 0], dtype=float)
    cap_fallback = float(np.nanmedian(cap_vals)) if cap_vals.size > 0 else 1.0
    if not np.isfinite(cap_fallback) or cap_fallback <= 0:
        cap_fallback = 1.0

    # Canonicalize style scores on the full universe cross-section
    canonical_style_map: dict[str, dict[str, float]] = {}
    style_cols_present = [c for c in STYLE_COLUMN_TO_LABEL if c in exposures_df.columns]
    if exposures_df is not None and not exposures_df.empty and style_cols_present:
        style_names = [STYLE_COLUMN_TO_LABEL[c] for c in style_cols_present]
        style_scores = exposures_df[["ticker", *style_cols_present]].copy()
        style_scores["ticker"] = style_scores["ticker"].astype(str).str.upper()
        style_scores = style_scores.drop_duplicates(subset=["ticker"], keep="last").set_index("ticker")
        style_scores.columns = style_names
        cap_series = pd.Series(
            {t: float(mcap_map.get(t, cap_fallback)) for t in style_scores.index},
            dtype=float,
        )
        if "gics_industry_group" in exposures_df.columns:
            industry_series = (
                exposures_df[["ticker", "gics_industry_group"]]
                .copy()
                .assign(ticker=lambda d: d["ticker"].astype(str).str.upper())
                .drop_duplicates(subset=["ticker"], keep="last")
                .set_index("ticker")["gics_industry_group"]
                .reindex(style_scores.index)
                .fillna("Unmapped")
            )
            industry_dummies = pd.get_dummies(industry_series, dtype=float)
        else:
            industry_dummies = pd.DataFrame(index=style_scores.index)

        canonical_scores = canonicalize_style_scores(
            style_scores=style_scores,
            market_caps=cap_series,
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

    universe_by_ticker: dict[str, dict[str, Any]] = {}
    industry_col = "gics_industry_group" if "gics_industry_group" in exposures_df.columns else (
        "industry_group" if "industry_group" in exposures_df.columns else None
    )
    for _, row in exposures_df.iterrows():
        ticker = str(row.get("ticker", "")).upper()
        if not ticker:
            continue

        exposures: dict[str, float] = {}
        for col, label in STYLE_COLUMN_TO_LABEL.items():
            if col not in exposures_df.columns:
                continue
            canon = canonical_style_map.get(ticker, {}).get(label)
            if canon is not None:
                exposures[label] = _finite_float(canon, 0.0)
            else:
                exposures[label] = _finite_float(row.get(col), 0.0)

        industry_group = ""
        if industry_col:
            ig = str(row.get(industry_col) or "").strip()
            if ig and ig.lower() not in {"", "nan", "none"}:
                industry_group = ig
                exposures[ig] = 1.0

        sensitivities = {
            factor: round(_finite_float(exposures.get(factor), 0.0) * _finite_float(vol, 0.0), 6)
            for factor, vol in factor_vol_map.items()
        }
        risk_loading = round(float(sum(abs(v) for v in sensitivities.values())), 6)
        spec = (specific_risk_by_ticker or {}).get(ticker, {})
        spec_var = _finite_float(spec.get("specific_var"), 0.0)
        spec_vol = _finite_float(spec.get("specific_vol"), 0.0)

        universe_by_ticker[ticker] = {
            "ticker": ticker,
            "name": name_map.get(ticker, ticker),
            "gics_sector": sector_map.get(ticker, ""),
            "sector": sector_map.get(ticker, ""),
            "industry_group": industry_group,
            "market_cap": round(_finite_float(mcap_map.get(ticker), 0.0), 2),
            "price": round(_finite_float(price_map.get(ticker), 0.0), 4),
            "exposures": exposures,
            "sensitivities": sensitivities,
            "risk_loading": risk_loading,
            "specific_var": round(spec_var, 8),
            "specific_vol": round(spec_vol, 6),
        }

    # Lightweight search index for instant lookup
    search_index = [
        {
            "ticker": t,
            "name": d.get("name", t),
            "sector": d.get("gics_sector", ""),
            "risk_loading": d.get("risk_loading", 0.0),
            "specific_vol": d.get("specific_vol", 0.0),
        }
        for t, d in universe_by_ticker.items()
    ]
    search_index.sort(key=lambda x: str(x["ticker"]))

    return {
        "ticker_count": len(universe_by_ticker),
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
            "name": str(base.get("name") or t),
            "long_short": "LONG" if shares >= 0 else "SHORT",
            "shares": shares,
            "price": round(price, 2),
            "market_value": round(mv, 2),
            "weight": 0.0,
            "gics_sector": str(base.get("gics_sector") or ""),
            "sector": str(base.get("sector") or ""),
            "account": meta["account"],
            "sleeve": meta["sleeve"],
            "source": meta["source"],
            "industry_group": str(base.get("industry_group") or ""),
            "exposures": dict(base.get("exposures") or {}),
            "specific_var": _finite_float(base.get("specific_var"), 0.0),
            "specific_vol": _finite_float(base.get("specific_vol"), 0.0),
            "risk_contrib_pct": 0.0,
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

    result: dict[str, list[dict]] = {"raw": [], "sensitivity": [], "risk_contribution": []}

    for factor in sorted(all_factors):
        # Raw portfolio-weighted exposure
        raw_exp = portfolio_factor_exposure(positions, factor)

        # Factor vol from cov matrix
        detail = factor_detail_map.get(factor)
        factor_vol = detail["factor_vol"] if detail else 0.0
        sensitivity = detail["sensitivity"] if detail else raw_exp * factor_vol
        risk_pct = detail["pct_of_total"] if detail else 0.0

        # Position-level drilldown
        drilldown_raw = []
        drilldown_sens = []
        for pos in positions:
            pos_exp = float(pos.get("exposures", {}).get(factor, 0.0))
            if abs(pos_exp) > 1e-8:
                raw_contrib = pos["weight"] * pos_exp
                pos_sens = pos_exp * factor_vol
                drilldown_raw.append({
                    "ticker": pos["ticker"],
                    "weight": pos["weight"],
                    "exposure": round(pos_exp, 4),
                    "contribution": round(raw_contrib, 6),
                })
                drilldown_sens.append({
                    "ticker": pos["ticker"],
                    "weight": pos["weight"],
                    "exposure": round(pos_exp, 4),
                    "sensitivity": round(pos_sens, 6),
                    "contribution": round(pos["weight"] * pos_sens, 6),
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
            "drilldown": drilldown_raw,
        })

    return result


def run_refresh() -> dict[str, Any]:
    """Full pipeline: compute daily factor returns → covariance → risk → cache."""
    logger.info("Starting refresh pipeline...")

    # 1. Ensure daily factor returns are computed (incremental)
    logger.info("Computing daily factor returns (incremental)...")
    compute_daily_factor_returns(DATA_DB, CACHE_DB)

    # 2. Fetch full-universe data from local data.db
    logger.info("Fetching data from local database...")
    prices_universe_df = postgres.load_latest_prices()
    fundamentals_universe_df = postgres.load_fundamental_snapshots()
    exposures_universe_df = postgres.load_barra_exposures()
    source_dates = postgres.load_source_dates()

    # 4. Build factor covariance from daily factor returns (504-day lookback)
    logger.info("Computing factor covariance from daily returns...")
    cov, latest_r2 = build_factor_covariance_from_cache(
        CACHE_DB, lookback_days=config.LOOKBACK_DAYS
    )
    logger.info("Computing stock-level specific risk from residual history...")
    specific_risk_by_ticker = build_specific_risk_from_cache(
        CACHE_DB,
        lookback_days=config.LOOKBACK_DAYS,
    )

    # 5. Build/cached full-universe loadings first (portfolio is a final projection only).
    logger.info("Building full-universe ticker loadings...")
    universe_loadings = _build_universe_ticker_loadings(
        exposures_universe_df,
        fundamentals_universe_df,
        prices_universe_df,
        cov,
        specific_risk_by_ticker=specific_risk_by_ticker,
    )

    # 6. Project held positions from full-universe cache
    logger.info("Projecting held positions from full-universe cache...")
    positions, total_value = _build_positions_from_universe(universe_loadings["by_ticker"])

    # 7. Risk decomposition
    logger.info("Computing risk decomposition...")
    risk_shares, component_shares, factor_details = risk_decomposition(
        cov=cov,
        positions=positions,
        specific_risk_by_ticker=specific_risk_by_ticker,
    )

    # 8. Compute per-position risk contributions
    for pos in positions:
        exps = pos.get("exposures", {})
        risk_score = sum(
            abs(float(exps.get(d["factor"], 0.0)) * d["sensitivity"])
            for d in factor_details
        )
        pos["risk_contrib_pct"] = round(risk_score * pos["weight"] * 100, 2)

    # 9. Compute condition number from cov matrix
    condition_number = 0.0
    if not cov.empty:
        try:
            cn = float(np.linalg.cond(cov.to_numpy()))
            condition_number = cn if np.isfinite(cn) else 9999.99
        except Exception:
            pass

    # 10. Compute exposure modes
    logger.info("Computing exposure modes...")
    coverage_date, factor_coverage = _load_latest_factor_coverage(CACHE_DB)
    exposure_modes = _compute_exposures_modes(
        positions,
        cov,
        factor_details,
        factor_coverage=factor_coverage,
        coverage_date=coverage_date,
    )

    # 11. Build covariance matrix for frontend (correlation)
    cov_matrix: dict[str, Any] = {}
    if not cov.empty:
        factors = list(cov.columns)
        # Convert to correlation
        stds = np.sqrt(np.diag(cov.to_numpy()))
        stds[stds == 0] = 1.0
        corr = cov.to_numpy() / np.outer(stds, stds)
        corr = np.clip(corr, -1.0, 1.0)
        cov_matrix = {
            "factors": factors,
            "correlation": [[round(float(v), 4) for v in row] for row in corr],
        }

    # 12. Sanitize non-finite floats (NaN/Inf break JSON serialization)
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

    # 13. Cache everything
    logger.info("Caching results...")
    portfolio_data = {
        "positions": positions,
        "total_value": round(total_value, 2),
        "position_count": len(positions),
    }
    sqlite.cache_set("portfolio", portfolio_data)

    risk_data = {
        "risk_shares": risk_shares,
        "component_shares": component_shares,
        "factor_details": factor_details,
        "cov_matrix": cov_matrix,
        "r_squared": round(latest_r2, 4),
        "condition_number": round(condition_number, 2),
    }
    sqlite.cache_set("risk", risk_data)

    sqlite.cache_set("universe_loadings", universe_loadings)
    sqlite.cache_set(
        "universe_factors",
        {
            "factors": universe_loadings.get("factors", []),
            "factor_vols": universe_loadings.get("factor_vols", {}),
            "r_squared": round(latest_r2, 4),
            "condition_number": round(condition_number, 2),
            "ticker_count": universe_loadings.get("ticker_count", 0),
        },
    )
    sqlite.cache_set("exposures", exposure_modes)
    sqlite.cache_set("source_dates", source_dates)

    logger.info("Refresh complete.")
    return {"status": "ok", "positions": len(positions), "total_value": round(total_value, 2)}
