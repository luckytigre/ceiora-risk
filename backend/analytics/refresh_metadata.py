"""Serving refresh metadata and diagnostics summary helpers."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np

from backend.analytics.contracts import (
    EligibilitySummaryPayload,
    FactorDetailPayload,
    ModelSanityPayload,
    RiskEngineMetaPayload,
    RiskEngineStatePayload,
    RiskSharesPayload,
    SourceDatesPayload,
    UniverseLoadingsPayload,
)

logger = logging.getLogger(__name__)
ELIGIBILITY_WELL_COVERED_RATIO = 0.50
ELIGIBILITY_WELL_COVERED_MIN_N = 100


def finite_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    return out if np.isfinite(out) else float(default)


def max_iso_date(*values: Any) -> str | None:
    clean = sorted(
        {
            str(value).strip()
            for value in values
            if str(value or "").strip()
        }
    )
    return clean[-1] if clean else None


def build_risk_engine_state(
    *,
    risk_engine_meta: RiskEngineMetaPayload,
    recomputed_this_refresh: bool,
    recompute_reason: str,
    estimation_exposure_anchor_date: str | None = None,
) -> RiskEngineStatePayload:
    factor_returns_latest_date = risk_engine_meta.get("factor_returns_latest_date")
    last_recompute_date = str(risk_engine_meta.get("last_recompute_date") or "")
    return {
        "status": str(risk_engine_meta.get("status") or "unknown"),
        "method_version": str(risk_engine_meta.get("method_version") or ""),
        "last_recompute_date": last_recompute_date,
        "factor_returns_latest_date": factor_returns_latest_date,
        "core_rebuild_date": last_recompute_date,
        "core_state_through_date": factor_returns_latest_date,
        "estimation_exposure_anchor_date": estimation_exposure_anchor_date,
        "cross_section_min_age_days": int(risk_engine_meta.get("cross_section_min_age_days") or 0),
        "recompute_interval_days": int(risk_engine_meta.get("recompute_interval_days") or 0),
        "lookback_days": int(risk_engine_meta.get("lookback_days") or 0),
        "specific_risk_ticker_count": int(risk_engine_meta.get("specific_risk_ticker_count") or 0),
        "recomputed_this_refresh": bool(recomputed_this_refresh),
        "recompute_reason": str(recompute_reason),
    }


def serving_source_dates(
    *,
    source_dates: SourceDatesPayload,
    universe_loadings: UniverseLoadingsPayload,
    eligibility_summary: EligibilitySummaryPayload | None = None,
) -> SourceDatesPayload:
    out: SourceDatesPayload = dict(source_dates or {})
    latest_available = max_iso_date(
        out.get("exposures_latest_available_asof"),
        out.get("exposures_asof"),
        universe_loadings.get("latest_available_asof"),
        (eligibility_summary or {}).get("latest_available_date"),
    )
    served_asof = str(
        universe_loadings.get("as_of_date")
        or (eligibility_summary or {}).get("date")
        or out.get("exposures_served_asof")
        or ""
    ).strip() or None
    if latest_available is not None:
        out["exposures_asof"] = latest_available
        out["exposures_latest_available_asof"] = latest_available
    if served_asof is not None:
        out["exposures_served_asof"] = served_asof
    return out


def refreshed_eligibility_summary(
    *,
    eligibility_summary: EligibilitySummaryPayload | None,
    universe_loadings: UniverseLoadingsPayload,
    source_dates: SourceDatesPayload,
) -> EligibilitySummaryPayload:
    out: EligibilitySummaryPayload = dict(eligibility_summary or {})
    served_asof = str(
        universe_loadings.get("as_of_date")
        or source_dates.get("exposures_served_asof")
        or source_dates.get("exposures_asof")
        or ""
    ).strip() or None
    latest_available = max_iso_date(
        out.get("latest_available_date"),
        out.get("date"),
        universe_loadings.get("latest_available_asof"),
        source_dates.get("exposures_latest_available_asof"),
        source_dates.get("exposures_asof"),
        served_asof,
    )
    current_date = str(out.get("date") or "").strip() or None
    current_latest = str(out.get("latest_available_date") or "").strip() or None
    needs_overlay = bool(
        served_asof
        and (
            current_date is None
            or current_date < served_asof
            or (latest_available is not None and (current_latest is None or current_latest < latest_available))
        )
    )
    if not needs_overlay:
        return out

    exposure_n = int(universe_loadings.get("ticker_count") or out.get("exposure_n") or 0)
    structural_eligible_n = int(
        universe_loadings.get("eligible_ticker_count") or out.get("structural_eligible_n") or 0
    )
    core_structural_eligible_n = int(
        universe_loadings.get("core_estimated_ticker_count") or out.get("core_structural_eligible_n") or 0
    )
    projected_only_n = int(
        universe_loadings.get("projected_only_ticker_count") or out.get("projected_only_n") or 0
    )
    projectable_n = int(structural_eligible_n or out.get("projectable_n") or 0)
    regression_member_n = int(core_structural_eligible_n or out.get("regression_member_n") or 0)
    denominator = max(exposure_n, 1)
    used_older_than_latest = bool(served_asof and latest_available and latest_available > served_asof)

    out.update(
        {
            "status": "ok" if exposure_n > 0 else str(out.get("status") or "no-data"),
            "date": served_asof,
            "exp_date": served_asof,
            "latest_available_date": latest_available,
            "selection_mode": "serving_snapshot",
            "selected_well_covered": not used_older_than_latest,
            "used_older_than_latest": used_older_than_latest,
            "exposure_n": exposure_n,
            "structural_eligible_n": structural_eligible_n,
            "core_structural_eligible_n": core_structural_eligible_n,
            "projectable_n": projectable_n,
            "projected_only_n": projected_only_n,
            "regression_member_n": regression_member_n,
            "structural_coverage": round(structural_eligible_n / denominator, 6),
            "projectable_coverage": round(projectable_n / denominator, 6),
            "regression_coverage": round(regression_member_n / denominator, 6),
            "max_regression_member_n": int(
                max(int(out.get("max_regression_member_n") or 0), regression_member_n)
            ),
            "coverage_threshold_n": int(out.get("coverage_threshold_n") or 0),
        }
    )
    return out


def build_model_sanity_report(
    *,
    risk_shares: RiskSharesPayload,
    factor_details: list[FactorDetailPayload],
    eligibility_summary: EligibilitySummaryPayload,
) -> ModelSanityPayload:
    warnings: list[str] = []

    regression_cov = finite_float(eligibility_summary.get("regression_coverage"), 0.0)
    if regression_cov < 0.20:
        warnings.append(
            f"Low regression coverage on latest usable date: {regression_cov * 100.0:.1f}%."
        )

    drop_pct = finite_float(eligibility_summary.get("drop_pct_from_prev"), 0.0)
    if drop_pct > 0.10:
        warnings.append(
            f"Eligible universe dropped {drop_pct * 100.0:.1f}% vs previous cross-section."
        )

    industry_pct = finite_float(risk_shares.get("industry"), 0.0)
    market_pct = finite_float(risk_shares.get("market"), 0.0)
    style_pct = finite_float(risk_shares.get("style"), 0.0)
    if market_pct > 90.0:
        warnings.append(f"Market risk share is highly concentrated at {market_pct:.1f}% of total risk.")
    if industry_pct > 90.0:
        warnings.append(f"Industry risk share is highly concentrated at {industry_pct:.1f}% of total risk.")
    if style_pct > 90.0:
        warnings.append(f"Style risk share is highly concentrated at {style_pct:.1f}% of total risk.")

    sign_mismatch = 0
    for row in factor_details:
        exp = finite_float(row.get("exposure"), 0.0)
        sens = finite_float(row.get("sensitivity"), 0.0)
        if abs(exp) > 1e-12 and abs(sens) > 1e-12 and (exp * sens) < 0:
            sign_mismatch += 1
    if sign_mismatch > 0:
        warnings.append(
            f"{sign_mismatch} factors have exposure/sensitivity sign mismatch; expected same sign."
        )

    coverage_date = str(eligibility_summary.get("date") or "")
    latest_available_date = str(eligibility_summary.get("latest_available_date") or "")
    used_older_than_latest = bool(eligibility_summary.get("used_older_than_latest"))
    if used_older_than_latest and coverage_date and latest_available_date:
        warnings.append(
            f"Using latest well-covered date {coverage_date} (latest source date is {latest_available_date})."
        )

    return {
        "status": "warn" if warnings else "ok",
        "warnings": warnings,
        "coverage_date": coverage_date or None,
        "latest_available_date": latest_available_date or None,
        "selection_mode": str(eligibility_summary.get("selection_mode") or ""),
        "update_available": bool(used_older_than_latest),
        "checks": {
            "factor_sign_mismatch_count": int(sign_mismatch),
            "market_risk_share_pct": round(market_pct, 2),
            "latest_regression_coverage_pct": round(regression_cov * 100.0, 2),
            "latest_structural_eligible_n": int(eligibility_summary.get("structural_eligible_n", 0) or 0),
            "latest_core_structural_eligible_n": int(
                eligibility_summary.get("core_structural_eligible_n", 0) or 0
            ),
            "latest_projectable_n": int(eligibility_summary.get("projectable_n", 0) or 0),
            "latest_projected_only_n": int(eligibility_summary.get("projected_only_n", 0) or 0),
            "industry_risk_share_pct": round(industry_pct, 2),
            "style_risk_share_pct": round(style_pct, 2),
            "idio_risk_share_pct": round(finite_float(risk_shares.get("idio"), 0.0), 2),
        },
    }


def load_latest_eligibility_summary(cache_db: Path) -> EligibilitySummaryPayload:
    conn = sqlite3.connect(str(cache_db))
    try:
        elig_cols = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(daily_universe_eligibility_summary)").fetchall()
        }
        core_structural_expr = (
            "core_structural_eligible_n" if "core_structural_eligible_n" in elig_cols else "structural_eligible_n"
        )
        projectable_expr = "projectable_n" if "projectable_n" in elig_cols else "regression_member_n"
        projected_only_expr = "projected_only_n" if "projected_only_n" in elig_cols else "0"
        projectable_coverage_expr = (
            "projectable_coverage" if "projectable_coverage" in elig_cols else "regression_coverage"
        )
        latest_any = conn.execute(
            f"""
            SELECT date, exp_date, exposure_n, structural_eligible_n, regression_member_n,
                   {core_structural_expr} AS core_structural_eligible_n,
                   {projectable_expr} AS projectable_n,
                   {projected_only_expr} AS projected_only_n,
                   structural_coverage, regression_coverage,
                   {projectable_coverage_expr} AS projectable_coverage,
                   drop_pct_from_prev, alert_level
            FROM daily_universe_eligibility_summary
            ORDER BY date DESC
            LIMIT 1
            """
        ).fetchone()
        max_row = conn.execute(
            """
            SELECT MAX(regression_member_n)
            FROM daily_universe_eligibility_summary
            """
        ).fetchone()
        max_regression_n = int(max_row[0] or 0) if max_row else 0
        coverage_threshold_n = max(
            ELIGIBILITY_WELL_COVERED_MIN_N,
            int(ELIGIBILITY_WELL_COVERED_RATIO * max_regression_n),
        )

        row = conn.execute(
            f"""
            SELECT date, exp_date, exposure_n, structural_eligible_n, regression_member_n,
                   {core_structural_expr} AS core_structural_eligible_n,
                   {projectable_expr} AS projectable_n,
                   {projected_only_expr} AS projected_only_n,
                   structural_coverage, regression_coverage,
                   {projectable_coverage_expr} AS projectable_coverage,
                   drop_pct_from_prev, alert_level
            FROM daily_universe_eligibility_summary
            WHERE regression_member_n >= ?
            ORDER BY date DESC
            LIMIT 1
            """,
            (coverage_threshold_n,),
        ).fetchone()
        selection_mode = "well_covered"
        if row is None:
            row = conn.execute(
                f"""
                SELECT date, exp_date, exposure_n, structural_eligible_n, regression_member_n,
                       {core_structural_expr} AS core_structural_eligible_n,
                       {projectable_expr} AS projectable_n,
                       {projected_only_expr} AS projected_only_n,
                       structural_coverage, regression_coverage,
                       {projectable_coverage_expr} AS projectable_coverage,
                       drop_pct_from_prev, alert_level
                FROM daily_universe_eligibility_summary
                WHERE regression_member_n > 0
                ORDER BY date DESC
                LIMIT 1
                """
            ).fetchone()
            selection_mode = "latest_positive"
        if row is None:
            row = latest_any
            selection_mode = "latest_any"
    except sqlite3.OperationalError:
        latest_any = None
        max_regression_n = 0
        coverage_threshold_n = ELIGIBILITY_WELL_COVERED_MIN_N
        selection_mode = "none"
        row = None
    finally:
        conn.close()
    if not row:
        return {
            "status": "no-data",
            "selection_mode": "none",
            "max_regression_member_n": int(max_regression_n),
            "coverage_threshold_n": int(coverage_threshold_n),
            "latest_available_date": str(latest_any[0]) if latest_any and latest_any[0] is not None else None,
        }

    latest_available_date = str(latest_any[0]) if latest_any and latest_any[0] is not None else str(row[0])
    selected_date = str(row[0])
    selected_well_covered = bool(selection_mode == "well_covered")
    used_older_than_latest = bool(latest_available_date and latest_available_date > selected_date)
    return {
        "status": "ok",
        "date": selected_date,
        "exp_date": str(row[1]) if row[1] is not None else None,
        "exposure_n": int(row[2] or 0),
        "structural_eligible_n": int(row[3] or 0),
        "regression_member_n": int(row[4] or 0),
        "core_structural_eligible_n": int(row[5] or 0),
        "projectable_n": int(row[6] or 0),
        "projected_only_n": int(row[7] or 0),
        "structural_coverage": float(row[8] or 0.0),
        "regression_coverage": float(row[9] or 0.0),
        "projectable_coverage": float(row[10] or 0.0),
        "drop_pct_from_prev": float(row[11] or 0.0),
        "alert_level": str(row[12] or ""),
        "selection_mode": selection_mode,
        "max_regression_member_n": int(max_regression_n),
        "coverage_threshold_n": int(coverage_threshold_n),
        "latest_available_date": latest_available_date,
        "selected_well_covered": selected_well_covered,
        "used_older_than_latest": used_older_than_latest,
    }
