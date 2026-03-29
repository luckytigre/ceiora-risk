"""GET /api/exposures?mode= — cUSE4 per-factor values with position drilldown."""

from __future__ import annotations

from fastapi import APIRouter, Query

from backend.api.routes.readiness import raise_cache_not_ready
import backend.services.cuse4_dashboard_payload_service as dashboard_payload_service
import backend.services.cuse4_factor_history_service as factor_history_service

router = APIRouter()
load_exposures_response = dashboard_payload_service.load_exposures_response
load_factor_history_response = factor_history_service.load_factor_history_response

@router.get("/exposures")
async def get_exposures(mode: str = Query("raw", pattern="^(raw|sensitivity|risk_contribution)$")):
    try:
        return load_exposures_response(mode=mode)
    except dashboard_payload_service.DashboardPayloadNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )


@router.get("/exposures/history")
async def get_exposure_history(
    factor_id: str = Query(..., min_length=1),
    years: int = Query(5, ge=1, le=10),
):
    try:
        return load_factor_history_response(
            factor_token=factor_id,
            years=int(years),
        )
    except factor_history_service.FactorHistoryNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )
