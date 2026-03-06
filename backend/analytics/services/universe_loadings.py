"""Universe loadings and coverage builders used by analytics pipeline."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.analytics.contracts import (
    FactorCoveragePayload,
    SpecificRiskPayload,
    UniverseLoadingsPayload,
    UniverseTickerPayload,
)
from backend.analytics.trbc_economic_sector_short import abbreviate_trbc_economic_sector_short
from backend.risk_model.descriptors import FULL_STYLE_ORTH_RULES, canonicalize_style_scores
from backend.risk_model.eligibility import build_eligibility_context, structural_eligibility_for_date
from backend.risk_model.risk_attribution import STYLE_COLUMN_TO_LABEL

logger = logging.getLogger(__name__)


def _finite_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    return out if np.isfinite(out) else float(default)


def build_universe_ticker_loadings(
    exposures_df: pd.DataFrame,
    fundamentals_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    cov: pd.DataFrame,
    *,
    data_db: Path,
    specific_risk_by_ticker: dict[str, SpecificRiskPayload] | None = None,
) -> UniverseLoadingsPayload:
    """Build full-universe cached loadings/risk context keyed by ticker."""
    exposures_df = exposures_df.copy() if exposures_df is not None else pd.DataFrame()
    fundamentals_df = fundamentals_df.copy() if fundamentals_df is not None else pd.DataFrame()
    prices_df = prices_df.copy() if prices_df is not None else pd.DataFrame()

    if not exposures_df.empty:
        exposures_df["ticker"] = exposures_df["ticker"].astype(str).str.upper()
        if "ric" in exposures_df.columns:
            exposures_df["ric"] = exposures_df["ric"].astype(str).str.upper()
    if not fundamentals_df.empty:
        fundamentals_df["ticker"] = fundamentals_df["ticker"].astype(str).str.upper()
        if "ric" in fundamentals_df.columns:
            fundamentals_df["ric"] = fundamentals_df["ric"].astype(str).str.upper()
    if not prices_df.empty:
        prices_df["ticker"] = prices_df["ticker"].astype(str).str.upper()
        if "ric" in prices_df.columns:
            prices_df["ric"] = prices_df["ric"].astype(str).str.upper()

    ric_by_ticker: dict[str, str] = {}
    ticker_by_ric: dict[str, str] = {}

    def _collect_maps(df: pd.DataFrame) -> None:
        if df.empty or "ric" not in df.columns or "ticker" not in df.columns:
            return
        for _, row in df.iterrows():
            ric = str(row.get("ric") or "").upper().strip()
            ticker = str(row.get("ticker") or "").upper().strip()
            if not ric or not ticker:
                continue
            ric_by_ticker[ticker] = ric
            if ric not in ticker_by_ric:
                ticker_by_ric[ric] = ticker

    _collect_maps(exposures_df)
    _collect_maps(fundamentals_df)
    _collect_maps(prices_df)

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
    trbc_business_sector_map: dict[str, str] = {}
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
            trbc_business_sector = str(row.get("trbc_business_sector") or "").strip()
            if trbc_business_sector:
                trbc_business_sector_map[ticker] = trbc_business_sector
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
        asof_series = exposures_df["as_of_date"].astype(str).str.strip()
        asof_counts = asof_series.value_counts()
        if not asof_counts.empty:
            max_count = int(asof_counts.max())
            # Guard against thin end-of-day snapshots (for example, incomplete latest date).
            min_coverage = max(100, int(0.50 * max_count))
            well_covered_dates = sorted(
                str(dt) for dt, cnt in asof_counts.items()
                if int(cnt) >= min_coverage
            )
            if well_covered_dates:
                latest_asof = well_covered_dates[-1]
            else:
                latest_asof = str(asof_series.max())
            if latest_asof != str(asof_series.max()):
                logger.warning(
                    "Using well-covered exposure as-of date %s instead of sparse latest %s "
                    "(coverage threshold=%s, max_count=%s)",
                    latest_asof,
                    str(asof_series.max()),
                    min_coverage,
                    max_count,
                )

    eligibility_df = pd.DataFrame()
    if latest_asof:
        elig_ctx = build_eligibility_context(data_db, dates=[latest_asof])
        _, eligibility_df = structural_eligibility_for_date(elig_ctx, latest_asof)

    eligible_mask = eligibility_df.get("is_structural_eligible", pd.Series(dtype=bool)).astype(bool)
    eligible_rics = set(eligibility_df.index[eligible_mask].astype(str).str.upper())
    eligible_tickers = {
        ticker for ticker, ric in ric_by_ticker.items()
        if ric in eligible_rics
    }
    ineligible_reason: dict[str, str] = {}
    for ticker, ric in ric_by_ticker.items():
        if ric in eligibility_df.index:
            ineligible_reason[ticker] = str(eligibility_df.loc[ric, "exclusion_reason"] or "")

    # Canonicalize style scores on the structurally eligible cross-section only.
    canonical_style_map: dict[str, dict[str, float]] = {}
    style_cols_present = [c for c in STYLE_COLUMN_TO_LABEL if c in exposures_df.columns] if not exposures_df.empty else []
    if not exposures_df.empty and style_cols_present and eligible_rics and "ric" in exposures_df.columns:
        style_names = [STYLE_COLUMN_TO_LABEL[c] for c in style_cols_present]
        style_scores = exposures_df[["ric", *style_cols_present]].copy()
        style_scores["ric"] = style_scores["ric"].astype(str).str.upper()
        style_scores = style_scores[style_scores["ric"].isin(eligible_rics)]
        style_scores = style_scores.drop_duplicates(subset=["ric"], keep="last").set_index("ric")
        style_scores.columns = style_names
        if not style_scores.empty:
            caps_from_elig = pd.to_numeric(
                eligibility_df.reindex(style_scores.index)["market_cap"],
                errors="coerce",
            )
            industries_from_elig = (
                eligibility_df.reindex(style_scores.index)["trbc_business_sector"]
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
                for ric, row in canonical_scores.iterrows():
                    ticker = ticker_by_ric.get(str(ric).upper(), str(ric).upper())
                    canonical_style_map[ticker] = {
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
    universe_by_ticker: dict[str, UniverseTickerPayload] = {}
    for ticker in all_tickers:
        if not ticker:
            continue

        ric = str(ric_by_ticker.get(ticker, "")).upper()
        eligible = bool(ric in eligible_rics) if ric else bool(ticker in eligible_tickers)
        trbc_economic_sector_short = str(
            (
                eligibility_df.loc[ric, "trbc_economic_sector_short"]
                if ric in eligibility_df.index and "trbc_economic_sector_short" in eligibility_df.columns
                else (
                    eligibility_df.loc[ric, "trbc_sector"]
                    if ric in eligibility_df.index and "trbc_sector" in eligibility_df.columns
                    else ""
                )
            )
            or trbc_economic_sector_short_map.get(ticker, "")
        )
        trbc_industry_group = str(
            (eligibility_df.loc[ric, "trbc_industry_group"] if ric in eligibility_df.index else "")
            or trbc_industry_map.get(ticker, "")
        )
        trbc_business_sector = str(
            (eligibility_df.loc[ric, "trbc_business_sector"] if ric in eligibility_df.index else "")
            or trbc_business_sector_map.get(ticker, "")
        )
        market_cap = _finite_float(
            eligibility_df.loc[ric, "market_cap"] if ric in eligibility_df.index else mcap_map.get(ticker),
            np.nan,
        )

        exposures: dict[str, float] = {}
        if eligible and ticker in canonical_style_map:
            exposures.update(canonical_style_map[ticker])
            if trbc_business_sector:
                exposures[trbc_business_sector] = 1.0

        sensitivities = {
            factor: round(_finite_float(exposures.get(factor), 0.0) * _finite_float(vol, 0.0), 6)
            for factor, vol in factor_vol_map.items()
        }
        risk_loading = round(float(sum(abs(v) for v in sensitivities.values())), 6) if eligible else None
        spec = (specific_risk_by_ticker or {}).get(ric, {}) if ric else {}
        if not spec:
            spec = (specific_risk_by_ticker or {}).get(ticker, {})
        spec_var = _finite_float(spec.get("specific_var"), np.nan) if eligible else np.nan
        spec_vol = _finite_float(spec.get("specific_vol"), np.nan) if eligible else np.nan

        universe_by_ticker[ticker] = {
            "ticker": ticker,
            "ric": ric or None,
            "name": name_map.get(ticker, ""),
            "trbc_economic_sector_short": trbc_economic_sector_short,
            "trbc_economic_sector_short_abbr": abbreviate_trbc_economic_sector_short(trbc_economic_sector_short),
            "trbc_business_sector": trbc_business_sector,
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
            "ric": d.get("ric", None),
            "name": d.get("name", ""),
            "trbc_economic_sector_short": d.get("trbc_economic_sector_short", ""),
            "trbc_economic_sector_short_abbr": d.get(
                "trbc_economic_sector_short_abbr",
                d.get("trbc_sector_abbr", ""),
            ),
            "trbc_business_sector": d.get("trbc_business_sector", ""),
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


def load_latest_factor_coverage(cache_db: Path) -> tuple[str | None, dict[str, FactorCoveragePayload]]:
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

    out: dict[str, FactorCoveragePayload] = {}
    for factor_name, cross_n, eligible_n, coverage in rows:
        out[str(factor_name)] = {
            "cross_section_n": int(cross_n or 0),
            "eligible_n": int(eligible_n or 0),
            "coverage_pct": float(coverage or 0.0),
        }
    return latest, out
