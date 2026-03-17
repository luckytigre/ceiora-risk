"""GET /api/operator/status — operator-facing run-lane summary and recency."""

from __future__ import annotations

from fastapi import APIRouter, Header

from backend import config
from backend.api.auth import require_role
from backend.services import operator_status_service

router = APIRouter()


@router.get("/operator/status")
def get_operator_status(
    x_operator_token: str | None = Header(default=None, alias="X-Operator-Token"),
    x_refresh_token: str | None = Header(default=None, alias="X-Refresh-Token"),
    authorization: str | None = Header(default=None),
):
    if config.cloud_mode():
        require_role(
            "operator",
            x_operator_token=x_operator_token,
            x_refresh_token=x_refresh_token,
            authorization=authorization,
        )
    return operator_status_service.build_operator_status_payload()
