"""cUSE4 portfolio serving and cUSE4 what-if preview routes."""

from typing import Any

from fastapi import APIRouter
from fastapi import Header
from fastapi import HTTPException
from pydantic import BaseModel, Field, FiniteFloat

from backend import config
from backend.api.auth import require_authenticated_session
from backend.data.account_scope import AccountScope
from backend.data.account_scope import AccountScopeAuthRequired
from backend.data.account_scope import AccountScopeDenied
from backend.data.account_scope import AccountScopeProvisioningError
from backend.data.account_scope import account_enforcement_enabled
from backend.data.account_scope import validate_requested_account
from backend.api.routes.holdings_scope import raise_account_scope_error
from backend.api.routes.holdings_scope import resolve_holdings_scope
from backend.api.routes.presenters import normalize_trbc_sector_fields
from backend.api.routes.readiness import raise_cache_not_ready
from backend.services import cuse4_holdings_service as holdings_service
import backend.services.cuse4_dashboard_payload_service as dashboard_payload_service
from backend.services.cuse4_portfolio_whatif import preview_portfolio_whatif

router = APIRouter()
MAX_WHATIF_SCENARIO_ROWS = 100
load_portfolio_response = dashboard_payload_service.load_portfolio_response
_resolve_holdings_scope = resolve_holdings_scope


def _resolve_mutation_scope(
    *,
    x_app_session_token: str | None,
) -> AccountScope:
    return _resolve_holdings_scope(
        x_app_session_token=x_app_session_token,
    )

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
async def get_portfolio(
    x_app_session_token: str | None = Header(default=None, alias="X-App-Session-Token"),
):
    try:
        if account_enforcement_enabled():
            scope = _resolve_holdings_scope(
                x_app_session_token=x_app_session_token,
            )
            scoped = preview_portfolio_whatif(
                scenario_rows=[],
                account_id=scope.default_account_id,
                allowed_account_ids=scope.account_ids,
            )
            current = dict(scoped.get("current") or {})
            current["positions"] = [
                normalize_trbc_sector_fields(dict(row))
                for row in current.get("positions", [])
            ]
            current["source_dates"] = scoped.get("source_dates") or {}
            current["_account_scoped"] = True
            current["account_id"] = scope.default_account_id
            current["_cached"] = False
            return current
        return load_portfolio_response(
            position_normalizer=normalize_trbc_sector_fields,
        )
    except (AccountScopeAuthRequired, AccountScopeDenied, AccountScopeProvisioningError) as exc:
        raise_account_scope_error(exc)
        raise
    except dashboard_payload_service.DashboardPayloadNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )


@router.post("/portfolio/whatif")
async def post_portfolio_whatif(
    payload: WhatIfPreviewRequest,
    x_app_session_token: str | None = Header(default=None, alias="X-App-Session-Token"),
):
    scenario_rows = [dict(row) for row in payload.model_dump().get("scenario_rows", [])]
    if len(scenario_rows) > MAX_WHATIF_SCENARIO_ROWS:
        raise HTTPException(status_code=400, detail=f"Too many what-if rows. Max {MAX_WHATIF_SCENARIO_ROWS}.")
    try:
        allowed_account_ids = None
        if account_enforcement_enabled():
            scope = _resolve_holdings_scope(
                x_app_session_token=x_app_session_token,
            )
            for row in scenario_rows:
                validate_requested_account(scope, str(row.get("account_id") or ""))
            if not scope.is_admin:
                allowed_account_ids = list(scope.account_ids)
        out = preview_portfolio_whatif(
            scenario_rows=scenario_rows,
            allowed_account_ids=allowed_account_ids,
        )
    except (AccountScopeAuthRequired, AccountScopeDenied, AccountScopeProvisioningError) as exc:
        raise_account_scope_error(exc)
        raise
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
    x_app_session_token: str | None = Header(default=None, alias="X-App-Session-Token"),
):
    require_authenticated_session(
        x_app_session_token=x_app_session_token,
    )
    scenario_rows = [dict(row) for row in payload.model_dump().get("scenario_rows", [])]
    if len(scenario_rows) > MAX_WHATIF_SCENARIO_ROWS:
        raise HTTPException(status_code=400, detail=f"Too many what-if rows. Max {MAX_WHATIF_SCENARIO_ROWS}.")
    try:
        scope = _resolve_mutation_scope(
            x_app_session_token=x_app_session_token,
        )
        for row in scenario_rows:
            validate_requested_account(scope, str(row.get("account_id") or ""))
        out = holdings_service.run_whatif_apply(
            scenario_rows=scenario_rows,
            requested_by=payload.requested_by,
            default_source=payload.default_source,
            dry_run=False,
        )
        out["refresh"] = None
        return out
    except (AccountScopeAuthRequired, AccountScopeDenied, AccountScopeProvisioningError) as exc:
        raise_account_scope_error(exc)
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"What-if apply failed: {exc}") from exc
