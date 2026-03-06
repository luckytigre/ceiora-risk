"""Cache staging/publish helpers for analytics refresh."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np

from backend.analytics.contracts import (
    ComponentSharesPayload,
    CovarianceMatrixPayload,
    CovariancePayload,
    EligibilitySummaryPayload,
    ExposureModesPayload,
    FactorDetailPayload,
    ModelSanityPayload,
    PositionPayload,
    RefreshMetaPayload,
    RiskEngineMetaPayload,
    RiskEngineStatePayload,
    RiskSharesPayload,
    SnapshotBuildPayload,
    SourceDatesPayload,
    SpecificRiskPayload,
    StageRefreshSnapshotResult,
    UniverseFactorsPayload,
    UniverseLoadingsPayload,
)
from backend.analytics.health import compute_health_diagnostics
from backend.data import sqlite

logger = logging.getLogger(__name__)
ELIGIBILITY_WELL_COVERED_RATIO = 0.50
ELIGIBILITY_WELL_COVERED_MIN_N = 100


def _finite_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    return out if np.isfinite(out) else float(default)


def build_risk_engine_state(
    *,
    risk_engine_meta: RiskEngineMetaPayload,
    recomputed_this_refresh: bool,
    recompute_reason: str,
) -> RiskEngineStatePayload:
    return {
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


def build_model_sanity_report(
    *,
    risk_shares: RiskSharesPayload,
    factor_details: list[FactorDetailPayload],
    eligibility_summary: EligibilitySummaryPayload,
) -> ModelSanityPayload:
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
            "latest_regression_coverage_pct": round(regression_cov * 100.0, 2),
            "latest_structural_eligible_n": int(eligibility_summary.get("structural_eligible_n", 0) or 0),
            "industry_risk_share_pct": round(industry_pct, 2),
            "style_risk_share_pct": round(style_pct, 2),
            "idio_risk_share_pct": round(_finite_float(risk_shares.get("idio"), 0.0), 2),
        },
    }


def load_latest_eligibility_summary(cache_db: Path) -> EligibilitySummaryPayload:
    conn = sqlite3.connect(str(cache_db))
    try:
        latest_any = conn.execute(
            """
            SELECT date, exp_date, exposure_n, structural_eligible_n, regression_member_n,
                   structural_coverage, regression_coverage, drop_pct_from_prev, alert_level
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
            """
            SELECT date, exp_date, exposure_n, structural_eligible_n, regression_member_n,
                   structural_coverage, regression_coverage, drop_pct_from_prev, alert_level
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
                """
                SELECT date, exp_date, exposure_n, structural_eligible_n, regression_member_n,
                       structural_coverage, regression_coverage, drop_pct_from_prev, alert_level
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
        "structural_coverage": float(row[5] or 0.0),
        "regression_coverage": float(row[6] or 0.0),
        "drop_pct_from_prev": float(row[7] or 0.0),
        "alert_level": str(row[8] or ""),
        "selection_mode": selection_mode,
        "max_regression_member_n": int(max_regression_n),
        "coverage_threshold_n": int(coverage_threshold_n),
        "latest_available_date": latest_available_date,
        "selected_well_covered": selected_well_covered,
        "used_older_than_latest": used_older_than_latest,
    }


def stage_refresh_cache_snapshot(
    *,
    run_id: str,
    refresh_mode: str,
    refresh_started_at: str,
    source_dates: SourceDatesPayload,
    snapshot_build: SnapshotBuildPayload,
    risk_engine_meta: RiskEngineMetaPayload,
    recomputed_this_refresh: bool,
    recompute_reason: str,
    cov_payload: CovariancePayload,
    specific_risk_by_security: dict[str, SpecificRiskPayload],
    positions: list[PositionPayload],
    total_value: float,
    risk_shares: RiskSharesPayload,
    component_shares: ComponentSharesPayload,
    factor_details: list[FactorDetailPayload],
    cov_matrix: CovarianceMatrixPayload,
    latest_r2: float,
    condition_number: float,
    universe_loadings: UniverseLoadingsPayload,
    exposure_modes: ExposureModesPayload,
    cuse4_foundation: dict[str, Any],
    light_mode: bool,
    data_db: Path,
    cache_db: Path,
) -> StageRefreshSnapshotResult:
    """Stage all refresh payloads under a snapshot id (not yet published)."""
    snapshot_id = str(run_id)
    logger.info("Staging refresh cache snapshot: snapshot_id=%s mode=%s", snapshot_id, refresh_mode)

    def _stage_cache(key: str, value: Any) -> None:
        sqlite.cache_set(key, value, snapshot_id=snapshot_id)

    risk_engine_state = build_risk_engine_state(
        risk_engine_meta=risk_engine_meta,
        recomputed_this_refresh=bool(recomputed_this_refresh),
        recompute_reason=str(recompute_reason),
    )
    _stage_cache("risk_engine_cov", cov_payload)
    _stage_cache("risk_engine_specific_risk", specific_risk_by_security)
    _stage_cache("risk_engine_meta", risk_engine_meta)

    portfolio_data = {
        "positions": positions,
        "total_value": round(total_value, 2),
        "position_count": len(positions),
        "refresh_started_at": refresh_started_at,
        "source_dates": source_dates,
    }
    _stage_cache("portfolio", portfolio_data)

    risk_data = {
        "risk_shares": risk_shares,
        "component_shares": component_shares,
        "factor_details": factor_details,
        "cov_matrix": cov_matrix,
        "r_squared": round(float(latest_r2), 4),
        "condition_number": round(float(condition_number), 2),
        "risk_engine": risk_engine_state,
        "refresh_started_at": refresh_started_at,
    }
    _stage_cache("risk", risk_data)

    universe_loadings["risk_engine"] = risk_engine_state
    universe_loadings["refresh_started_at"] = refresh_started_at
    universe_loadings["source_dates"] = source_dates
    _stage_cache("universe_loadings", universe_loadings)
    universe_factors: UniverseFactorsPayload = {
        "factors": universe_loadings.get("factors", []),
        "factor_vols": universe_loadings.get("factor_vols", {}),
        "r_squared": round(float(latest_r2), 4),
        "condition_number": round(float(condition_number), 2),
        "ticker_count": universe_loadings.get("ticker_count", 0),
        "eligible_ticker_count": universe_loadings.get("eligible_ticker_count", 0),
        "risk_engine": risk_engine_state,
        "refresh_started_at": refresh_started_at,
    }
    _stage_cache("universe_factors", universe_factors)
    _stage_cache("exposures", exposure_modes)

    eligibility_summary = load_latest_eligibility_summary(cache_db)
    _stage_cache("eligibility", eligibility_summary)
    sanity = build_model_sanity_report(
        risk_shares=risk_shares,
        factor_details=factor_details,
        eligibility_summary=eligibility_summary,
    )
    _stage_cache("model_sanity", sanity)
    _stage_cache("cuse4_foundation", cuse4_foundation)

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
        _stage_cache("health_diagnostics", compute_health_diagnostics(data_db, cache_db))
        health_refreshed = True
    logger.info(
        "Staged core payloads: positions=%s factors=%s health_refreshed=%s",
        len(positions),
        len(universe_loadings.get("factors", [])),
        bool(health_refreshed),
    )

    refresh_meta: RefreshMetaPayload = {
        "status": "ok",
        "mode": refresh_mode,
        "run_id": run_id,
        "snapshot_id": snapshot_id,
        "refresh_started_at": refresh_started_at,
        "source_dates": source_dates,
        "cross_section_snapshot": snapshot_build,
        "risk_engine": risk_engine_state,
        "model_sanity_status": sanity.get("status", "unknown"),
        "cuse4_foundation": cuse4_foundation,
        "health_refreshed": bool(health_refreshed),
    }
    _stage_cache("refresh_meta", refresh_meta)

    return {
        "snapshot_id": snapshot_id,
        "risk_engine_state": risk_engine_state,
        "sanity": sanity,
        "health_refreshed": bool(health_refreshed),
    }
