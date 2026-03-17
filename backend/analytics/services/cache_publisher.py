"""Cache staging/publish helpers for analytics refresh."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from backend.analytics import health_payloads, refresh_metadata
from backend.analytics.contracts import (
    ComponentSharesPayload,
    CovarianceMatrixPayload,
    CovariancePayload,
    EligibilitySummaryPayload,
    ExposureModesPayload,
    FactorCatalogEntryPayload,
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
HEALTH_DIAGNOSTICS_CACHE_VERSION = health_payloads.HEALTH_DIAGNOSTICS_CACHE_VERSION


def build_risk_engine_state(
    *,
    risk_engine_meta: RiskEngineMetaPayload,
    recomputed_this_refresh: bool,
    recompute_reason: str,
    estimation_exposure_anchor_date: str | None = None,
) -> RiskEngineStatePayload:
    return refresh_metadata.build_risk_engine_state(
        risk_engine_meta=risk_engine_meta,
        recomputed_this_refresh=recomputed_this_refresh,
        recompute_reason=recompute_reason,
        estimation_exposure_anchor_date=estimation_exposure_anchor_date,
    )


def _health_reuse_signature(
    *,
    source_dates: SourceDatesPayload,
    risk_engine_state: RiskEngineStatePayload,
    positions: list[PositionPayload],
    total_value: float,
) -> dict[str, Any]:
    return health_payloads.health_reuse_signature(
        source_dates=source_dates,
        risk_engine_state=risk_engine_state,
        positions=positions,
        total_value=total_value,
    )


def _serving_source_dates(
    *,
    source_dates: SourceDatesPayload,
    universe_loadings: UniverseLoadingsPayload,
    eligibility_summary: EligibilitySummaryPayload | None = None,
) -> SourceDatesPayload:
    return refresh_metadata.serving_source_dates(
        source_dates=source_dates,
        universe_loadings=universe_loadings,
        eligibility_summary=eligibility_summary,
    )


def _carry_forward_health_payload(
    cached_payload: dict[str, Any] | None,
    *,
    run_id: str,
    snapshot_id: str,
    refresh_started_at: str,
    source_dates: SourceDatesPayload,
    risk_engine_state: RiskEngineStatePayload,
) -> tuple[dict[str, Any], str]:
    return health_payloads.carry_forward_health_payload(
        cached_payload,
        run_id=run_id,
        snapshot_id=snapshot_id,
        refresh_started_at=refresh_started_at,
        source_dates=source_dates,
        risk_engine_state=risk_engine_state,
    )


def build_model_sanity_report(
    *,
    risk_shares: RiskSharesPayload,
    factor_details: list[FactorDetailPayload],
    eligibility_summary: EligibilitySummaryPayload,
) -> ModelSanityPayload:
    return refresh_metadata.build_model_sanity_report(
        risk_shares=risk_shares,
        factor_details=factor_details,
        eligibility_summary=eligibility_summary,
    )


def load_latest_eligibility_summary(cache_db: Path) -> EligibilitySummaryPayload:
    return refresh_metadata.load_latest_eligibility_summary(cache_db)


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
    latest_r2: float | None,
    universe_loadings: UniverseLoadingsPayload,
    exposure_modes: ExposureModesPayload,
    factor_catalog: list[FactorCatalogEntryPayload],
    cuse4_foundation: dict[str, Any],
    recompute_health_diagnostics: bool = False,
    reuse_cached_static_payloads: bool = False,
    data_db: Path,
    cache_db: Path,
) -> StageRefreshSnapshotResult:
    """Stage all refresh payloads under a snapshot id (not yet published)."""
    snapshot_id = str(run_id)
    logger.info("Staging refresh cache snapshot: snapshot_id=%s mode=%s", snapshot_id, refresh_mode)

    def _stage_cache(key: str, value: Any) -> None:
        sqlite.cache_set(key, value, snapshot_id=snapshot_id, db_path=cache_db)

    eligibility_summary = (
        sqlite.cache_get("eligibility", db_path=cache_db)
        if reuse_cached_static_payloads
        else None
    )
    if not isinstance(eligibility_summary, dict) or not eligibility_summary:
        eligibility_summary = load_latest_eligibility_summary(cache_db)
    eligibility_summary = refresh_metadata.refreshed_eligibility_summary(
        eligibility_summary=eligibility_summary,
        universe_loadings=universe_loadings,
        source_dates=source_dates,
    )
    risk_engine_state = build_risk_engine_state(
        risk_engine_meta=risk_engine_meta,
        recomputed_this_refresh=bool(recomputed_this_refresh),
        recompute_reason=str(recompute_reason),
        estimation_exposure_anchor_date=refresh_metadata.derive_estimation_exposure_anchor_date(
            factor_returns_latest_date=risk_engine_meta.get("factor_returns_latest_date"),
            cross_section_min_age_days=risk_engine_meta.get("cross_section_min_age_days"),
            existing_anchor_date=risk_engine_meta.get("estimation_exposure_anchor_date"),
        ),
    )
    effective_source_dates = _serving_source_dates(
        source_dates=source_dates,
        universe_loadings=universe_loadings,
        eligibility_summary=eligibility_summary,
    )
    _stage_cache("risk_engine_cov", cov_payload)
    _stage_cache("risk_engine_specific_risk", specific_risk_by_security)
    _stage_cache("risk_engine_meta", risk_engine_meta)

    portfolio_data = {
        "positions": positions,
        "total_value": round(total_value, 2),
        "position_count": len(positions),
        "run_id": str(run_id),
        "snapshot_id": snapshot_id,
        "refresh_started_at": refresh_started_at,
        "source_dates": effective_source_dates,
    }
    _stage_cache("portfolio", portfolio_data)
    health_reuse_signature = _health_reuse_signature(
        source_dates=effective_source_dates,
        risk_engine_state=risk_engine_state,
        positions=positions,
        total_value=total_value,
    )

    risk_data = {
        "risk_shares": risk_shares,
        "component_shares": component_shares,
        "factor_details": factor_details,
        "factor_catalog": factor_catalog,
        "cov_matrix": cov_matrix,
        "r_squared": round(float(latest_r2), 4) if latest_r2 is not None else None,
        "risk_engine": risk_engine_state,
        "run_id": str(run_id),
        "snapshot_id": snapshot_id,
        "refresh_started_at": refresh_started_at,
        "source_dates": effective_source_dates,
    }
    _stage_cache("risk", risk_data)

    universe_loadings["run_id"] = str(run_id)
    universe_loadings["snapshot_id"] = snapshot_id
    universe_loadings["risk_engine"] = risk_engine_state
    universe_loadings["refresh_started_at"] = refresh_started_at
    universe_loadings["source_dates"] = effective_source_dates
    _stage_cache("universe_loadings", universe_loadings)
    universe_factors: UniverseFactorsPayload = {
        "factors": universe_loadings.get("factors", []),
        "factor_vols": universe_loadings.get("factor_vols", {}),
        "factor_catalog": factor_catalog,
        "r_squared": round(float(latest_r2), 4) if latest_r2 is not None else None,
        "ticker_count": universe_loadings.get("ticker_count", 0),
        "eligible_ticker_count": universe_loadings.get("eligible_ticker_count", 0),
        "core_estimated_ticker_count": universe_loadings.get("core_estimated_ticker_count", 0),
        "projected_only_ticker_count": universe_loadings.get("projected_only_ticker_count", 0),
        "ineligible_ticker_count": universe_loadings.get("ineligible_ticker_count", 0),
        "risk_engine": risk_engine_state,
        "run_id": str(run_id),
        "snapshot_id": snapshot_id,
        "refresh_started_at": refresh_started_at,
        "source_dates": effective_source_dates,
    }
    _stage_cache("universe_factors", universe_factors)
    _stage_cache(
        "exposures",
        {
            **exposure_modes,
            "run_id": str(run_id),
            "snapshot_id": snapshot_id,
            "refresh_started_at": refresh_started_at,
            "source_dates": effective_source_dates,
        },
    )

    _stage_cache("eligibility", eligibility_summary)
    sanity = build_model_sanity_report(
        risk_shares=risk_shares,
        factor_details=factor_details,
        eligibility_summary=eligibility_summary,
    )
    _stage_cache("model_sanity", sanity)
    _stage_cache("cuse4_foundation", cuse4_foundation)

    cached_health_payload = sqlite.cache_get("health_diagnostics", db_path=cache_db)
    health_refresh_state = "deferred"
    if bool(recompute_health_diagnostics):
        health_payload = compute_health_diagnostics(
            data_db,
            cache_db,
            risk_payload=risk_data,
            portfolio_payload=portfolio_data,
            universe_payload=universe_loadings,
            covariance_payload=cov_payload,
            source_dates=effective_source_dates,
            run_id=run_id,
            snapshot_id=snapshot_id,
        )
        health_refreshed = True
        health_refresh_state = "recomputed"
    else:
        health_payload, health_refresh_state = _carry_forward_health_payload(
            cached_health_payload if isinstance(cached_health_payload, dict) else None,
            run_id=str(run_id),
            snapshot_id=str(snapshot_id),
            refresh_started_at=str(refresh_started_at),
            source_dates=effective_source_dates,
            risk_engine_state=risk_engine_state,
        )
        health_refreshed = False
    if isinstance(health_payload, dict):
        health_payload = dict(health_payload)
        health_payload["run_id"] = str(run_id)
        health_payload["snapshot_id"] = str(snapshot_id)
        health_payload["_reuse_signature"] = health_reuse_signature
        health_payload["cache_version"] = HEALTH_DIAGNOSTICS_CACHE_VERSION
        health_payload["diagnostics_refresh_state"] = str(health_refresh_state)
        health_payload.setdefault(
            "diagnostics_generated_from_run_id",
            str(run_id) if bool(recompute_health_diagnostics) else None,
        )
        health_payload.setdefault(
            "diagnostics_generated_from_snapshot_id",
            str(snapshot_id) if bool(recompute_health_diagnostics) else None,
        )
    _stage_cache("health_diagnostics", health_payload)
    logger.info(
        "Staged core payloads: positions=%s factors=%s health_refreshed=%s health_refresh_state=%s",
        len(positions),
        len(universe_loadings.get("factors", [])),
        bool(health_refreshed),
        str(health_refresh_state),
    )

    refresh_meta: RefreshMetaPayload = {
        "status": "ok",
        "mode": refresh_mode,
        "run_id": run_id,
        "snapshot_id": snapshot_id,
        "refresh_started_at": refresh_started_at,
        "source_dates": effective_source_dates,
        "cross_section_snapshot": snapshot_build,
        "risk_engine": risk_engine_state,
        "model_sanity_status": sanity.get("status", "unknown"),
        "cuse4_foundation": cuse4_foundation,
        "health_refreshed": bool(health_refreshed),
        "health_refresh_state": str(health_refresh_state),
    }
    _stage_cache("refresh_meta", refresh_meta)

    return {
        "snapshot_id": snapshot_id,
        "risk_engine_state": risk_engine_state,
        "sanity": sanity,
        "health_refreshed": bool(health_refreshed),
        "health_refresh_state": str(health_refresh_state),
        "persisted_payloads": {
            "portfolio": portfolio_data,
            "risk": risk_data,
            "risk_engine_cov": cov_payload,
            "risk_engine_specific_risk": specific_risk_by_security,
            "exposures": {
                **exposure_modes,
                "run_id": str(run_id),
                "snapshot_id": snapshot_id,
                "refresh_started_at": refresh_started_at,
                "source_dates": effective_source_dates,
            },
            "universe_loadings": universe_loadings,
            "universe_factors": universe_factors,
            "eligibility": eligibility_summary,
            "model_sanity": sanity,
            "health_diagnostics": health_payload,
            "refresh_meta": refresh_meta,
        },
    }
