"""GET /api/cuse/risk-page* — first-render and lazy cUSE risk-page payloads."""

from __future__ import annotations

from fastapi import APIRouter, Header, Query

from backend.api.routes.holdings_scope import raise_account_scope_error
from backend.api.routes.holdings_scope import resolve_holdings_scope
from backend.api.routes.readiness import raise_cache_not_ready
from backend.data.account_scope import AccountScopeAuthRequired
from backend.data.account_scope import AccountScopeDenied
from backend.data.account_scope import AccountScopeProvisioningError
from backend.data.account_scope import account_enforcement_enabled
from backend.services import cuse4_dashboard_payload_service, cuse4_risk_page_service

router = APIRouter()


def _resolve_cuse_risk_page_scope(
    *,
    x_app_session_token: str | None,
):
    if not account_enforcement_enabled():
        return None
    return resolve_holdings_scope(
        x_app_session_token=x_app_session_token,
    )


@router.get("/cuse/risk-page")
async def get_cuse_risk_page(
    x_app_session_token: str | None = Header(default=None, alias="X-App-Session-Token"),
):
    try:
        scope = _resolve_cuse_risk_page_scope(
            x_app_session_token=x_app_session_token,
        )
        if scope is not None:
            return cuse4_risk_page_service.load_cuse_risk_page_payload(
                account_id=scope.default_account_id,
                allowed_account_ids=scope.account_ids,
            )
        return cuse4_risk_page_service.load_cuse_risk_page_payload()
    except (AccountScopeAuthRequired, AccountScopeDenied, AccountScopeProvisioningError) as exc:
        raise_account_scope_error(exc)
        raise
    except cuse4_dashboard_payload_service.DashboardPayloadNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )


@router.get("/cuse/risk-page/exposure-mode")
async def get_cuse_risk_page_exposure_mode(
    mode: str = Query(..., pattern="^(raw|sensitivity|risk_contribution)$"),
    x_app_session_token: str | None = Header(default=None, alias="X-App-Session-Token"),
):
    try:
        scope = _resolve_cuse_risk_page_scope(
            x_app_session_token=x_app_session_token,
        )
        if scope is not None:
            return cuse4_risk_page_service.load_cuse_risk_page_exposure_mode_payload(
                mode=mode,
                account_id=scope.default_account_id,
                allowed_account_ids=scope.account_ids,
            )
        return cuse4_risk_page_service.load_cuse_risk_page_exposure_mode_payload(mode=mode)
    except (AccountScopeAuthRequired, AccountScopeDenied, AccountScopeProvisioningError) as exc:
        raise_account_scope_error(exc)
        raise
    except cuse4_dashboard_payload_service.DashboardPayloadNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )


@router.get("/cuse/risk-page/covariance")
async def get_cuse_risk_page_covariance(
    x_app_session_token: str | None = Header(default=None, alias="X-App-Session-Token"),
):
    try:
        _resolve_cuse_risk_page_scope(
            x_app_session_token=x_app_session_token,
        )
        return cuse4_risk_page_service.load_cuse_risk_page_covariance_payload()
    except (AccountScopeAuthRequired, AccountScopeDenied, AccountScopeProvisioningError) as exc:
        raise_account_scope_error(exc)
        raise
    except cuse4_dashboard_payload_service.DashboardPayloadNotReady as exc:
        raise_cache_not_ready(
            cache_key=exc.cache_key,
            message=exc.message,
            refresh_profile=exc.refresh_profile,
        )
