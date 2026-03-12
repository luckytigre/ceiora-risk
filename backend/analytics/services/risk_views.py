"""Risk/exposure view builders used by the analytics pipeline."""

from __future__ import annotations

from typing import Any

import numpy as np

from backend.analytics.contracts import (
    ExposureModesPayload,
    FactorCoveragePayload,
    FactorDetailPayload,
    PositionPayload,
    PositionRiskMixPayload,
    SpecificRiskPayload,
)
from backend.risk_model.risk_attribution import (
    SYSTEMATIC_CATEGORIES,
    portfolio_factor_exposure,
    systematic_variance_by_category,
)
from backend.portfolio.positions_store import (
    DEFAULT_ACCOUNT,
    DEFAULT_SLEEVE,
    DEFAULT_SOURCE,
    load_positions_snapshot,
)


def _finite_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    return out if np.isfinite(out) else float(default)


def specific_risk_by_ticker_view(
    specific_risk_by_security: dict[str, SpecificRiskPayload] | None,
) -> dict[str, SpecificRiskPayload]:
    """Create a ticker-keyed view from a canonical security-keyed specific-risk map."""
    out: dict[str, SpecificRiskPayload] = {}
    for key, row in (specific_risk_by_security or {}).items():
        key_txt = str(key or "").upper().strip()
        ticker = str(row.get("ticker") or (key_txt if "." not in key_txt else "")).upper().strip()
        if not ticker:
            continue
        ric = str(row.get("ric") or (key_txt if "." in key_txt else "")).upper().strip()
        candidate = dict(row)
        candidate["ticker"] = ticker
        candidate["ric"] = ric
        prev = out.get(ticker)
        if prev is None:
            out[ticker] = candidate
            continue
        prev_obs = int(prev.get("obs") or 0)
        cand_obs = int(candidate.get("obs") or 0)
        if cand_obs >= prev_obs:
            out[ticker] = candidate
    return out


def build_positions_from_universe(
    universe_by_ticker: dict[str, dict[str, Any]],
) -> tuple[list[PositionPayload], float]:
    """Project held positions from full-universe cached analytics."""
    shares_map, dynamic_meta = load_positions_snapshot()
    tickers = list(shares_map.keys())

    positions: list[PositionPayload] = []
    total_value = 0.0
    gross_value = 0.0
    for ticker in tickers:
        t = ticker.upper()
        base = universe_by_ticker.get(t, {})
        shares = _finite_float(shares_map.get(t, 0.0), 0.0)
        price = _finite_float(base.get("price"), 0.0)
        mv = shares * price
        total_value += mv
        gross_value += abs(mv)
        meta_base = dynamic_meta.get(t, {})
        meta = {
            "account": str(meta_base.get("account") or DEFAULT_ACCOUNT),
            "sleeve": str(meta_base.get("sleeve") or DEFAULT_SLEEVE),
            "source": str(meta_base.get("source") or DEFAULT_SOURCE),
        }
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
        pos["weight"] = round(mv / gross_value, 6) if gross_value != 0 else 0.0

    return positions, round(total_value, 2)


def compute_exposures_modes(
    positions: list[PositionPayload],
    cov,
    factor_details: list[FactorDetailPayload],
    factor_coverage: dict[str, FactorCoveragePayload] | None = None,
    coverage_date: str | None = None,
) -> ExposureModesPayload:
    """Compute the 3-mode exposure data for all factors."""
    all_factors: set[str] = set()
    for pos in positions:
        all_factors.update(pos.get("exposures", {}).keys())

    factor_detail_map = {d["factor"]: d for d in factor_details}
    coverage_map = factor_coverage or {}
    exposure_map = {factor: portfolio_factor_exposure(positions, factor) for factor in all_factors}

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

    result: ExposureModesPayload = {"raw": [], "sensitivity": [], "risk_contribution": []}

    for factor in sorted(all_factors):
        raw_exp = _finite_float(exposure_map.get(factor), 0.0)
        detail = factor_detail_map.get(factor)
        factor_vol = _finite_float(detail.get("factor_vol"), 0.0) if detail else 0.0
        sensitivity = _finite_float(detail.get("sensitivity"), raw_exp * factor_vol) if detail else raw_exp * factor_vol
        risk_pct = _finite_float(detail.get("pct_of_total"), 0.0) if detail else 0.0
        marginal_var = _finite_float(detail.get("marginal_var_contrib"), 0.0) if detail else 0.0
        cov_adj = _finite_float(cov_adj_map.get(factor), 0.0)

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
                risk_var_contrib = weight * pos_exp * cov_adj
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
                    "sensitivity": round(pos_exp * cov_adj, 8),
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


def compute_position_risk_mix(
    positions: list[PositionPayload],
    cov,
    specific_risk_by_ticker: dict[str, SpecificRiskPayload] | None = None,
) -> dict[str, PositionRiskMixPayload]:
    """Per-position risk split using Barra-style factor + specific variance."""
    if cov is None or cov.empty:
        out: dict[str, PositionRiskMixPayload] = {}
        for pos in positions:
            ticker = str(pos.get("ticker", "")).upper()
            if ticker:
                out[ticker] = {"country": 0.0, "industry": 0.0, "style": 0.0, "idio": 0.0}
        return out

    factors = [str(c) for c in cov.columns if str(c).lower() != "market"]
    if not factors:
        return {}
    f_mat = cov.reindex(index=factors, columns=factors).to_numpy(dtype=float)

    spec_map = specific_risk_by_ticker or {}
    out: dict[str, PositionRiskMixPayload] = {}
    for pos in positions:
        ticker = str(pos.get("ticker", "")).upper()
        if not ticker:
            continue
        exps = pos.get("exposures", {}) or {}
        x = np.array([_finite_float(exps.get(f), 0.0) for f in factors], dtype=float)
        systematic = systematic_variance_by_category(
            factors=factors,
            exposures=x,
            covariance=f_mat,
        )
        spec_var = _finite_float(spec_map.get(ticker, {}).get("specific_var"), 0.0)
        if spec_var < 0:
            spec_var = 0.0

        total = float(sum(systematic.values())) + spec_var
        if total <= 0:
            out[ticker] = {"country": 0.0, "industry": 0.0, "style": 0.0, "idio": 0.0}
            continue

        systematic_pct = {
            category: 100.0 * float(systematic.get(category, 0.0)) / total
            for category in SYSTEMATIC_CATEGORIES
        }
        idio_pct = 100.0 * spec_var / total

        for category in systematic_pct:
            systematic_pct[category] = max(0.0, systematic_pct[category])
        idio_pct = max(0.0, idio_pct)
        s = float(sum(systematic_pct.values())) + idio_pct
        if s > 0:
            k = 100.0 / s
            for category in systematic_pct:
                systematic_pct[category] *= k
            idio_pct *= k

        out[ticker] = {
            "country": round(systematic_pct.get("country", 0.0), 2),
            "industry": round(systematic_pct.get("industry", 0.0), 2),
            "style": round(systematic_pct.get("style", 0.0), 2),
            "idio": round(idio_pct, 2),
        }
    return out


def compute_position_total_risk_contributions(
    positions: list[PositionPayload],
    cov,
    specific_risk_by_ticker: dict[str, SpecificRiskPayload] | None = None,
) -> dict[str, float]:
    """Exact per-position contribution to total portfolio variance, in percent."""
    if cov is None or cov.empty or not positions:
        return {
            str(pos.get("ticker", "")).upper(): 0.0
            for pos in positions
            if str(pos.get("ticker", "")).strip()
        }

    factors = [str(c) for c in cov.columns if str(c).lower() != "market"]
    if not factors:
        return {
            str(pos.get("ticker", "")).upper(): 0.0
            for pos in positions
            if str(pos.get("ticker", "")).strip()
        }

    f_mat = cov.reindex(index=factors, columns=factors).to_numpy(dtype=float)
    if f_mat.size == 0:
        return {
            str(pos.get("ticker", "")).upper(): 0.0
            for pos in positions
            if str(pos.get("ticker", "")).strip()
        }

    spec_map = specific_risk_by_ticker or {}
    tickers: list[str] = []
    x_rows: list[np.ndarray] = []
    w_vec: list[float] = []
    spec_vars: list[float] = []

    for pos in positions:
        ticker = str(pos.get("ticker", "")).upper().strip()
        if not ticker:
            continue
        exps = pos.get("exposures", {}) or {}
        x_rows.append(np.array([_finite_float(exps.get(f), 0.0) for f in factors], dtype=float))
        w_vec.append(_finite_float(pos.get("weight", 0.0), 0.0))
        spec_var = _finite_float(spec_map.get(ticker, {}).get("specific_var"), 0.0)
        spec_vars.append(max(0.0, spec_var))
        tickers.append(ticker)

    if not tickers:
        return {}

    x_mat = np.vstack(x_rows)
    w_arr = np.asarray(w_vec, dtype=float)
    spec_arr = np.asarray(spec_vars, dtype=float)

    h_vec = (w_arr[:, None] * x_mat).sum(axis=0)
    fh_vec = f_mat @ h_vec
    systematic_contrib = w_arr * (x_mat @ fh_vec)
    specific_contrib = (w_arr ** 2) * spec_arr
    total_contrib = systematic_contrib + specific_contrib

    systematic_total = float(h_vec.T @ f_mat @ h_vec)
    specific_total = float(specific_contrib.sum())
    total_var = systematic_total + specific_total
    if not np.isfinite(total_var) or total_var <= 1e-12:
        return {ticker: 0.0 for ticker in tickers}

    return {
        tickers[i]: round(float(total_contrib[i]) / total_var * 100.0, 2)
        for i in range(len(tickers))
    }
