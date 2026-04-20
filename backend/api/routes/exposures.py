"""GET /api/exposures?mode= — cUSE4 per-factor values with position drilldown."""

from __future__ import annotations

from fastapi import APIRouter, Header, Query

from backend.api.routes.readiness import raise_cache_not_ready
from backend.api.routes.holdings_scope import raise_account_scope_error
from backend.api.routes.holdings_scope import resolve_holdings_scope
from backend.data.account_scope import AccountScopeAuthRequired
from backend.data.account_scope import AccountScopeDenied
from backend.data.account_scope import AccountScopeProvisioningError
from backend.data.account_scope import account_enforcement_enabled
import backend.services.cuse4_dashboard_payload_service as dashboard_payload_service
import backend.services.cuse4_factor_history_service as factor_history_service
from backend.services.cuse4_portfolio_whatif import preview_portfolio_whatif

router = APIRouter()
load_exposures_response = dashboard_payload_service.load_exposures_response
load_factor_history_response = factor_history_service.load_factor_history_response

@router.get("/exposures")
async def get_exposures(
    mode: str = Query("raw", pattern="^(raw|sensitivity|risk_contribution)$"),
    x_app_session_token: str | None = Header(default=None, alias="X-App-Session-Token"),
):
    try:
        if account_enforcement_enabled():
            scope = resolve_holdings_scope(
                x_app_session_token=x_app_session_token,
            )
            scoped = preview_portfolio_whatif(
                scenario_rows=[],
                account_id=scope.default_account_id,
                allowed_account_ids=scope.account_ids,
            )
            return dashboard_payload_service.load_account_scoped_exposures_response(
                mode=mode,
                scoped_preview=scoped,
                account_id=scope.default_account_id or "",
            )
        return load_exposures_response(mode=mode)
    except (AccountScopeAuthRequired, AccountScopeDenied, AccountScopeProvisioningError) as exc:
        raise_account_scope_error(exc)
        raise
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
        return load_factor_history_response(
            factor_token=factor_id,
            years=int(years),
        )
    except factor_history_service.FactorHistoryNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )
