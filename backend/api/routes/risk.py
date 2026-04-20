"""GET /api/risk — cUSE4 variance decomposition, factor details, covariance."""

from fastapi import Header
from fastapi import APIRouter

from backend.api.routes.holdings_scope import raise_account_scope_error
from backend.api.routes.holdings_scope import resolve_holdings_scope
from backend.api.routes.readiness import raise_cache_not_ready
from backend.data.account_scope import AccountScopeAuthRequired
from backend.data.account_scope import AccountScopeDenied
from backend.data.account_scope import AccountScopeProvisioningError
from backend.data.account_scope import account_enforcement_enabled
import backend.services.cuse4_dashboard_payload_service as dashboard_payload_service
from backend.services.cuse4_portfolio_whatif import preview_portfolio_whatif

router = APIRouter()
load_risk_response = dashboard_payload_service.load_risk_response


@router.get("/risk")
async def get_risk(
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
            return dashboard_payload_service.load_account_scoped_risk_response(
                scoped_preview=scoped,
                account_id=scope.default_account_id or "",
            )
        return load_risk_response()
    except (AccountScopeAuthRequired, AccountScopeDenied, AccountScopeProvisioningError) as exc:
        raise_account_scope_error(exc)
        raise
    except dashboard_payload_service.DashboardPayloadNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )
