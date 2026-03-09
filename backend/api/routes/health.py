"""GET /api/health/diagnostics — model health diagnostics payload."""

from __future__ import annotations

from fastapi import APIRouter, Header

from backend.api.auth import require_role
from backend.api.routes.readiness import raise_cache_not_ready
from backend.data.sqlite import cache_get

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
    data = cache_get("health_diagnostics")
    if data is not None:
        return {**data, "_cached": True}
    raise_cache_not_ready(
        cache_key="health_diagnostics",
        message="Health diagnostics are not ready yet. Run serve-refresh or a deeper local refresh.",
    )
