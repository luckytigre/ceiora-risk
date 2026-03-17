"""GET /api/health/diagnostics — model health diagnostics payload."""

from __future__ import annotations

from fastapi import APIRouter, Header

from backend.api.auth import require_role
from backend.api.routes.readiness import raise_cache_not_ready
from backend.services import health_diagnostics_service

router = APIRouter()


@router.get("/health/diagnostics")
async def get_health_diagnostics(
    x_operator_token: str | None = Header(default=None, alias="X-Operator-Token"),
    authorization: str | None = Header(default=None),
):
    require_role(
        "operator",
        x_operator_token=x_operator_token,
        authorization=authorization,
    )
    try:
        return health_diagnostics_service.load_health_diagnostics_payload()
    except health_diagnostics_service.HealthDiagnosticsNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )
