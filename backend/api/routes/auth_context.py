"""App-authenticated session context for frontend account bootstrap."""

from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException

from backend import config
from backend.api.auth import parse_app_principal
from backend.data.account_scope import AccountScopeAuthRequired
from backend.data.account_scope import AccountScopeBootstrapDisabled
from backend.data.account_scope import AccountScopeDenied
from backend.data.account_scope import AccountScopeProvisioningError
from backend.data.account_scope import account_enforcement_enabled
from backend.data.account_scope import resolve_effective_principal
from backend.data.account_scope import resolve_account_scope
from backend.data.neon import connect, resolve_dsn

router = APIRouter()


def _resolve_auth_scope(*, x_app_session_token: str | None):
    principal = parse_app_principal(x_app_session_token=x_app_session_token)
    if principal is None:
        raise AccountScopeAuthRequired("Authenticated app session required.")
    if not account_enforcement_enabled():
        return principal, resolve_account_scope(None, principal=principal)
    conn = connect(dsn=resolve_dsn(None), autocommit=True)
    try:
        principal = resolve_effective_principal(conn, principal=principal)
        return principal, resolve_account_scope(conn, principal=principal)
    finally:
        conn.close()


def _raise_scope_error(exc: Exception) -> None:
    if isinstance(exc, AccountScopeAuthRequired):
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    if isinstance(exc, AccountScopeDenied):
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    if isinstance(exc, AccountScopeBootstrapDisabled):
        raise HTTPException(
            status_code=409,
            detail={"message": str(exc), "code": "account_bootstrap_disabled"},
        ) from exc
    if isinstance(exc, AccountScopeProvisioningError):
        raise HTTPException(
            status_code=409,
            detail={"message": str(exc), "code": "account_provisioning_required"},
        ) from exc
    raise exc


@router.get("/auth/context")
async def get_auth_context(
    x_app_session_token: str | None = Header(default=None, alias="X-App-Session-Token"),
):
    try:
        principal, scope = _resolve_auth_scope(x_app_session_token=x_app_session_token)
    except (AccountScopeAuthRequired, AccountScopeDenied, AccountScopeProvisioningError) as exc:
        _raise_scope_error(exc)
        raise
    return {
        "auth_provider": principal.provider,
        "subject": principal.subject,
        "email": principal.email,
        "display_name": principal.display_name,
        "is_admin": principal.is_admin,
        "account_enforcement_enabled": scope.enforced,
        "default_account_id": scope.default_account_id,
        "account_ids": list(scope.account_ids),
        "admin_settings_enabled": bool(config.APP_ADMIN_SETTINGS_ENABLED),
    }
