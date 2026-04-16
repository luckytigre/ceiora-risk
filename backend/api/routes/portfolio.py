"""cUSE4 portfolio serving and cUSE4 what-if preview routes."""

from typing import Any

from fastapi import APIRouter
from fastapi import Header
from fastapi import HTTPException
from pydantic import BaseModel, Field, FiniteFloat

from backend import config
from backend.api.auth import require_role
from backend.api.routes.presenters import normalize_trbc_sector_fields
from backend.api.routes.readiness import raise_cache_not_ready
from backend.services import cuse4_holdings_service as holdings_service
import backend.services.cuse4_dashboard_payload_service as dashboard_payload_service
from backend.services.cuse4_portfolio_whatif import preview_portfolio_whatif

router = APIRouter()
MAX_WHATIF_SCENARIO_ROWS = 100
load_portfolio_response = dashboard_payload_service.load_portfolio_response


class WhatIfScenarioRow(BaseModel):
    account_id: str
    quantity: FiniteFloat
    ticker: str
    ric: str | None = None
    source: str | None = None


class WhatIfPreviewRequest(BaseModel):
    scenario_rows: list[WhatIfScenarioRow] = Field(default_factory=list)


class WhatIfApplyRequest(BaseModel):
    scenario_rows: list[WhatIfScenarioRow] = Field(default_factory=list)
    requested_by: str | None = None
    default_source: str = "what_if"


@router.get("/portfolio")
async def get_portfolio():
    try:
        return load_portfolio_response(
            position_normalizer=normalize_trbc_sector_fields,
        )
    except dashboard_payload_service.DashboardPayloadNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )


@router.post("/portfolio/whatif")
async def post_portfolio_whatif(
    payload: WhatIfPreviewRequest,
):
    scenario_rows = [dict(row) for row in payload.model_dump().get("scenario_rows", [])]
    if len(scenario_rows) > MAX_WHATIF_SCENARIO_ROWS:
        raise HTTPException(status_code=400, detail=f"Too many what-if rows. Max {MAX_WHATIF_SCENARIO_ROWS}.")
    try:
        out = preview_portfolio_whatif(
            scenario_rows=scenario_rows,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"What-if preview failed: {exc}") from exc

    def _normalize_positions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [normalize_trbc_sector_fields(dict(row)) for row in rows]

    out["current"]["positions"] = _normalize_positions(out["current"].get("positions", []))
    out["hypothetical"]["positions"] = _normalize_positions(out["hypothetical"].get("positions", []))
    return out


@router.post("/portfolio/whatif/apply")
async def post_portfolio_whatif_apply(
    payload: WhatIfApplyRequest,
    x_editor_token: str | None = Header(default=None, alias="X-Editor-Token"),
    x_operator_token: str | None = Header(default=None, alias="X-Operator-Token"),
    authorization: str | None = Header(default=None),
):
    require_role(
        "editor",
        x_editor_token=x_editor_token,
        x_operator_token=x_operator_token,
        authorization=authorization,
    )
    scenario_rows = [dict(row) for row in payload.model_dump().get("scenario_rows", [])]
    if len(scenario_rows) > MAX_WHATIF_SCENARIO_ROWS:
        raise HTTPException(status_code=400, detail=f"Too many what-if rows. Max {MAX_WHATIF_SCENARIO_ROWS}.")
    try:
        out = holdings_service.run_whatif_apply(
            scenario_rows=scenario_rows,
            requested_by=payload.requested_by,
            default_source=payload.default_source,
            dry_run=False,
        )
        out["refresh"] = None
        return out
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"What-if apply failed: {exc}") from exc
