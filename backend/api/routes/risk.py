"""GET /api/risk — variance decomposition, factor details, covariance matrix."""

from fastapi import APIRouter

from backend.api.routes.readiness import raise_cache_not_ready
from backend.services import dashboard_payload_service

router = APIRouter()


@router.get("/risk")
async def get_risk():
    try:
        return dashboard_payload_service.load_risk_response()
    except dashboard_payload_service.DashboardPayloadNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )
