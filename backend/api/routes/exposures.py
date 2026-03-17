"""GET /api/exposures?mode= — per-factor values with position-level drilldown."""

from __future__ import annotations

from fastapi import APIRouter, Query

from backend.api.routes.readiness import raise_cache_not_ready
from backend.services import dashboard_payload_service, factor_history_service

router = APIRouter()

@router.get("/exposures")
async def get_exposures(mode: str = Query("raw", pattern="^(raw|sensitivity|risk_contribution)$")):
    try:
        return dashboard_payload_service.load_exposures_response(mode=mode)
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
        return factor_history_service.load_factor_history_response(
            factor_token=factor_id,
            years=int(years),
        )
    except factor_history_service.FactorHistoryNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )
