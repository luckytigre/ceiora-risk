"""GET /api/data/diagnostics — data freshness and engine observability."""

from __future__ import annotations

from fastapi import APIRouter, Header, Query

from backend.api.auth import require_role
from backend.services import data_diagnostics_service

router = APIRouter()


@router.get("/data/diagnostics")
def get_data_diagnostics(
    include_paths: bool = Query(False),
    include_exact_row_counts: bool = Query(False),
    include_expensive_checks: bool = Query(False),
    x_operator_token: str | None = Header(default=None, alias="X-Operator-Token"),
    authorization: str | None = Header(default=None),
):
    if bool(include_paths or include_exact_row_counts or include_expensive_checks):
        require_role(
            "operator",
            x_operator_token=x_operator_token,
            authorization=authorization,
        )
    return data_diagnostics_service.build_data_diagnostics_payload(
        include_paths=include_paths,
        include_exact_row_counts=include_exact_row_counts,
        include_expensive_checks=include_expensive_checks,
    )
