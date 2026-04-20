"""GET /api/health/diagnostics — model health diagnostics payload."""

from __future__ import annotations

from fastapi import APIRouter, Header

from backend.api.auth import require_role
from backend.api.routes.readiness import raise_authority_unavailable
from backend.api.routes.readiness import raise_cache_not_ready
import backend.services.cuse4_health_diagnostics_service as health_diagnostics_service

router = APIRouter()
load_health_diagnostics_payload = health_diagnostics_service.load_health_diagnostics_payload


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
        return load_health_diagnostics_payload()
    except health_diagnostics_service.HealthDiagnosticsNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )
    except health_diagnostics_service.HealthDiagnosticsUnavailable as exc:
        raise_authority_unavailable(
            error="health_diagnostics_authority_unavailable",
            message=exc.message,
            source=exc.source,
        )
