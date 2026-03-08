"""Holdings management API (Neon-backed)."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field
from fastapi import APIRouter, HTTPException, Query

from backend.data.neon import connect, resolve_dsn
from backend.services.neon_holdings import (
    IMPORT_MODES,
    apply_holdings_import,
    apply_single_position_edit,
    list_holdings_accounts,
    list_holdings_positions,
    parse_holdings_rows,
    remove_single_position,
)
from backend.services.holdings_runtime_state import mark_holdings_dirty
from backend.services.refresh_manager import start_refresh

router = APIRouter()


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


def _trigger_light_refresh_if_requested(trigger: bool) -> dict[str, Any] | None:
    if not bool(trigger):
        return None
    started, state = start_refresh(
        mode="light",
        force_risk_recompute=False,
    )
    return {
        "started": bool(started),
        "state": state,
    }


def _record_holdings_dirty(
    *,
    action: str,
    account_id: str | None,
    summary: str,
    import_batch_id: str | None,
    change_count: int,
) -> None:
    mark_holdings_dirty(
        action=action,
        account_id=account_id,
        summary=summary,
        import_batch_id=import_batch_id,
        change_count=change_count,
    )


@router.get("/holdings/modes")
async def get_holdings_modes():
    return {
        "modes": sorted(IMPORT_MODES),
        "default": "upsert_absolute",
    }


@router.get("/holdings/accounts")
async def get_holdings_accounts():
    try:
        conn = connect(dsn=resolve_dsn(None), autocommit=True)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Neon not available: {exc}") from exc
    try:
        rows = list_holdings_accounts(conn)
        return {"accounts": rows}
    finally:
        conn.close()


@router.get("/holdings/positions")
async def get_holdings_positions(account_id: str | None = Query(default=None)):
    try:
        conn = connect(dsn=resolve_dsn(None), autocommit=True)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Neon not available: {exc}") from exc
    try:
        rows = list_holdings_positions(conn, account_id=account_id)
        return {
            "positions": rows,
            "account_id": account_id,
            "count": int(len(rows)),
        }
    finally:
        conn.close()


@router.post("/holdings/import")
async def post_holdings_import(payload: HoldingsImportRequest):
    try:
        conn = connect(dsn=resolve_dsn(None), autocommit=False)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Neon not available: {exc}") from exc
    try:
        parsed = parse_holdings_rows(
            conn,
            rows=[r.model_dump() for r in payload.rows],
            mode=str(payload.mode),
            default_account_id=payload.account_id,
            default_source=payload.default_source,
        )
        out = apply_holdings_import(
            conn,
            parsed=parsed,
            mode=str(payload.mode),
            account_id=payload.account_id,
            requested_by=payload.requested_by,
            filename=payload.filename,
            notes=payload.notes,
            dry_run=bool(payload.dry_run),
        )
        applied_changes = int(out.get("applied_upserts") or 0) + int(out.get("applied_deletes") or 0)
        if not payload.dry_run and str(out.get("status")) == "ok" and applied_changes > 0:
            _record_holdings_dirty(
                action=f"holdings_import:{payload.mode}",
                account_id=payload.account_id,
                summary=(
                    f"{payload.mode} import applied for {payload.account_id}: "
                    f"{int(out.get('applied_upserts') or 0)} upserts, {int(out.get('applied_deletes') or 0)} deletes"
                ),
                import_batch_id=str(out.get("import_batch_id") or "") or None,
                change_count=applied_changes,
            )
        out["refresh"] = _trigger_light_refresh_if_requested(
            bool(payload.trigger_refresh and not payload.dry_run and str(out.get("status")) == "ok")
        )
        out["preview_rejections"] = parsed.get("rejected", [])[:100]
        return out
    except ValueError as exc:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        conn.close()


@router.post("/holdings/position")
async def post_holdings_position(payload: HoldingsPositionEditRequest):
    try:
        conn = connect(dsn=resolve_dsn(None), autocommit=False)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Neon not available: {exc}") from exc
    try:
        out = apply_single_position_edit(
            conn,
            account_id=payload.account_id,
            quantity=payload.quantity,
            ric=payload.ric,
            ticker=payload.ticker,
            source=payload.source,
            requested_by=payload.requested_by,
            notes=payload.notes,
            dry_run=bool(payload.dry_run),
        )
        if not payload.dry_run and str(out.get("status")) == "ok" and str(out.get("action") or "") != "none":
            _record_holdings_dirty(
                action="holdings_position_edit",
                account_id=payload.account_id,
                summary=f"Position {out.get('action')} for {out.get('ticker') or out.get('ric')}",
                import_batch_id=str(out.get("import_batch_id") or "") or None,
                change_count=1,
            )
        out["refresh"] = _trigger_light_refresh_if_requested(
            bool(payload.trigger_refresh and not payload.dry_run and str(out.get("status")) == "ok")
        )
        return out
    except ValueError as exc:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        conn.close()


@router.post("/holdings/position/remove")
async def post_holdings_position_remove(payload: HoldingsPositionRemoveRequest):
    try:
        conn = connect(dsn=resolve_dsn(None), autocommit=False)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"Neon not available: {exc}") from exc
    try:
        out = remove_single_position(
            conn,
            account_id=payload.account_id,
            ric=payload.ric,
            ticker=payload.ticker,
            requested_by=payload.requested_by,
            notes=payload.notes,
            dry_run=bool(payload.dry_run),
        )
        if not payload.dry_run and str(out.get("status")) == "ok" and str(out.get("action") or "") != "none":
            _record_holdings_dirty(
                action="holdings_position_remove",
                account_id=payload.account_id,
                summary=f"Position removed for {out.get('ticker') or out.get('ric')}",
                import_batch_id=str(out.get("import_batch_id") or "") or None,
                change_count=1,
            )
        out["refresh"] = _trigger_light_refresh_if_requested(
            bool(payload.trigger_refresh and not payload.dry_run and str(out.get("status")) == "ok")
        )
        return out
    except ValueError as exc:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        conn.close()
