"""Shared app-session account-scope resolution for holdings-backed cUSE routes."""

from __future__ import annotations

from fastapi import HTTPException

from backend.api.auth import parse_app_principal
from backend.data.account_scope import AccountScopeAuthRequired
from backend.data.account_scope import AccountScopeDenied
from backend.data.account_scope import AccountScopeProvisioningError
from backend.data.account_scope import account_enforcement_enabled
from backend.data.account_scope import resolve_account_scope
from backend.data.neon import connect, resolve_dsn


def resolve_holdings_scope(
    *,
    x_app_session_token: str | None,
):
    principal = parse_app_principal(
        x_app_session_token=x_app_session_token,
    )
    if not account_enforcement_enabled():
        return resolve_account_scope(None, principal=principal)
    conn = connect(dsn=resolve_dsn(None), autocommit=True)
    try:
        return resolve_account_scope(conn, principal=principal)
    finally:
        conn.close()


def raise_account_scope_error(exc: Exception) -> None:
    if isinstance(exc, AccountScopeAuthRequired):
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    if isinstance(exc, AccountScopeDenied):
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    if isinstance(exc, AccountScopeProvisioningError):
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    raise exc
