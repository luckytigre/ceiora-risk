"""GET /api/risk — cUSE4 variance decomposition, factor details, covariance."""

from fastapi import APIRouter

from backend.api.routes.readiness import raise_cache_not_ready
import backend.services.cuse4_dashboard_payload_service as dashboard_payload_service

router = APIRouter()
load_risk_response = dashboard_payload_service.load_risk_response


@router.get("/risk")
async def get_risk():
    try:
        return load_risk_response()
    except dashboard_payload_service.DashboardPayloadNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )
