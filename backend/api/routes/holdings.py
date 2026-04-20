"""Holdings management API (Neon-backed)."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel, Field

from backend import config
from backend.api.auth import parse_app_principal
from backend.api.auth import require_authenticated_session
from backend.data.account_scope import AccountScope
from backend.data.account_scope import AccountScopeAuthRequired
from backend.data.account_scope import AccountScopeDenied
from backend.data.account_scope import AccountScopeProvisioningError
from backend.data.account_scope import account_enforcement_enabled
from backend.data.account_scope import resolve_account_scope
from backend.data.account_scope import validate_requested_account
from backend.data.neon import connect, resolve_dsn
from backend.services import cuse4_holdings_service as holdings_service

router = APIRouter()


def _resolve_holdings_scope(
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


def _resolve_mutation_scope(
    *,
    x_app_session_token: str | None,
) -> AccountScope:
    return _resolve_holdings_scope(
        x_app_session_token=x_app_session_token,
    )


def _raise_account_scope_error(exc: Exception) -> None:
    if isinstance(exc, AccountScopeAuthRequired):
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    if isinstance(exc, AccountScopeDenied):
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    if isinstance(exc, AccountScopeProvisioningError):
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    raise exc


class HoldingsImportRow(BaseModel):
    account_id: str | None = None
    ric: str | None = None
    ticker: str | None = None
    quantity: float
    source: str | None = None


class HoldingsImportRequest(BaseModel):
    account_id: str
    mode: Literal["replace_account", "upsert_absolute", "increment_delta"]
    rows: list[HoldingsImportRow] = Field(default_factory=list)
    filename: str | None = None
    requested_by: str | None = None
    notes: str | None = None
    default_source: str = "csv_upload"
    dry_run: bool = False
    trigger_refresh: bool = True


class HoldingsPositionEditRequest(BaseModel):
    account_id: str
    quantity: float
    ric: str | None = None
    ticker: str | None = None
    source: str = "ui_edit"
    requested_by: str | None = None
    notes: str | None = None
    dry_run: bool = False
    trigger_refresh: bool = True


class HoldingsPositionRemoveRequest(BaseModel):
    account_id: str
    ric: str | None = None
    ticker: str | None = None
    requested_by: str | None = None
    notes: str | None = None
    dry_run: bool = False
    trigger_refresh: bool = True


@router.get("/holdings/modes")
async def get_holdings_modes():
    return {
        "modes": sorted(holdings_service.IMPORT_MODES),
        "default": "upsert_absolute",
    }


@router.get("/holdings/accounts")
async def get_holdings_accounts(
    x_app_session_token: str | None = Header(default=None, alias="X-App-Session-Token"),
):
    try:
        scope = _resolve_holdings_scope(
            x_app_session_token=x_app_session_token,
        )
        rows = holdings_service.load_holdings_accounts(
            allowed_account_ids=scope.account_ids,
        )
    except (AccountScopeAuthRequired, AccountScopeDenied, AccountScopeProvisioningError) as exc:
        _raise_account_scope_error(exc)
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Neon not available: {exc}") from exc
    return {"accounts": rows}


@router.get("/holdings/positions")
async def get_holdings_positions(
    account_id: str | None = Query(default=None),
    x_app_session_token: str | None = Header(default=None, alias="X-App-Session-Token"),
):
    try:
        scope = _resolve_holdings_scope(
            x_app_session_token=x_app_session_token,
        )
        resolved_account_id = validate_requested_account(scope, account_id)
        rows = holdings_service.load_holdings_positions(
            account_id=resolved_account_id,
            allowed_account_ids=scope.account_ids,
        )
    except (AccountScopeAuthRequired, AccountScopeDenied, AccountScopeProvisioningError) as exc:
        _raise_account_scope_error(exc)
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Neon not available: {exc}") from exc
    return {
        "positions": rows,
        "account_id": resolved_account_id,
        "count": int(len(rows)),
    }


@router.post("/holdings/import")
async def post_holdings_import(
    payload: HoldingsImportRequest,
    x_app_session_token: str | None = Header(default=None, alias="X-App-Session-Token"),
):
    require_authenticated_session(
        x_app_session_token=x_app_session_token,
    )
    try:
        scope = _resolve_mutation_scope(
            x_app_session_token=x_app_session_token,
        )
        validate_requested_account(scope, payload.account_id)
        return holdings_service.run_holdings_import(
            account_id=payload.account_id,
            mode=str(payload.mode),
            rows=[r.model_dump() for r in payload.rows],
            filename=payload.filename,
            requested_by=payload.requested_by,
            notes=payload.notes,
            default_source=payload.default_source,
            dry_run=bool(payload.dry_run),
            trigger_refresh=bool(payload.trigger_refresh),
        )
    except (AccountScopeAuthRequired, AccountScopeDenied, AccountScopeProvisioningError) as exc:
        _raise_account_scope_error(exc)
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Neon not available: {exc}") from exc


@router.post("/holdings/position")
async def post_holdings_position(
    payload: HoldingsPositionEditRequest,
    x_app_session_token: str | None = Header(default=None, alias="X-App-Session-Token"),
):
    require_authenticated_session(
        x_app_session_token=x_app_session_token,
    )
    try:
        scope = _resolve_mutation_scope(
            x_app_session_token=x_app_session_token,
        )
        validate_requested_account(scope, payload.account_id)
        return holdings_service.run_position_upsert(
            account_id=payload.account_id,
            quantity=payload.quantity,
            ric=payload.ric,
            ticker=payload.ticker,
            source=payload.source,
            requested_by=payload.requested_by,
            notes=payload.notes,
            dry_run=bool(payload.dry_run),
            trigger_refresh=bool(payload.trigger_refresh),
        )
    except (AccountScopeAuthRequired, AccountScopeDenied, AccountScopeProvisioningError) as exc:
        _raise_account_scope_error(exc)
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Neon not available: {exc}") from exc


@router.post("/holdings/position/remove")
async def post_holdings_position_remove(
    payload: HoldingsPositionRemoveRequest,
    x_app_session_token: str | None = Header(default=None, alias="X-App-Session-Token"),
):
    require_authenticated_session(
        x_app_session_token=x_app_session_token,
    )
    try:
        scope = _resolve_mutation_scope(
            x_app_session_token=x_app_session_token,
        )
        validate_requested_account(scope, payload.account_id)
        return holdings_service.run_position_remove(
            account_id=payload.account_id,
            ric=payload.ric,
            ticker=payload.ticker,
            requested_by=payload.requested_by,
            notes=payload.notes,
            dry_run=bool(payload.dry_run),
            trigger_refresh=bool(payload.trigger_refresh),
        )
    except (AccountScopeAuthRequired, AccountScopeDenied, AccountScopeProvisioningError) as exc:
        _raise_account_scope_error(exc)
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Neon not available: {exc}") from exc
