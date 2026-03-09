"""Holdings application service for Neon-backed portfolio mutations."""

from __future__ import annotations

from typing import Any

from backend.data.neon import connect, resolve_dsn
from backend.services.holdings_runtime_state import mark_holdings_dirty
from backend.services.neon_holdings import (
    IMPORT_MODES,
    apply_holdings_import,
    apply_single_position_edit,
    list_holdings_accounts,
    list_holdings_positions,
    parse_holdings_rows,
    remove_single_position,
)
from backend.services.refresh_manager import start_refresh


def trigger_light_refresh_if_requested(trigger: bool) -> dict[str, Any] | None:
    if not bool(trigger):
        return None
    started, state = start_refresh(
        mode="light",
        force_risk_recompute=False,
        refresh_scope="holdings_only",
    )
    return {
        "started": bool(started),
        "state": state,
    }


def record_holdings_dirty(
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


def load_holdings_accounts() -> list[dict[str, Any]]:
    conn = connect(dsn=resolve_dsn(None), autocommit=True)
    try:
        return list_holdings_accounts(conn)
    finally:
        conn.close()


def load_holdings_positions(account_id: str | None = None) -> list[dict[str, Any]]:
    conn = connect(dsn=resolve_dsn(None), autocommit=True)
    try:
        return list_holdings_positions(conn, account_id=account_id)
    finally:
        conn.close()


def run_holdings_import(
    *,
    account_id: str,
    mode: str,
    rows: list[dict[str, Any]],
    filename: str | None = None,
    requested_by: str | None = None,
    notes: str | None = None,
    default_source: str = "csv_upload",
    dry_run: bool = False,
    trigger_refresh: bool = True,
) -> dict[str, Any]:
    conn = connect(dsn=resolve_dsn(None), autocommit=False)
    try:
        parsed = parse_holdings_rows(
            conn,
            rows=rows,
            mode=str(mode),
            default_account_id=account_id,
            default_source=default_source,
        )
        out = apply_holdings_import(
            conn,
            parsed=parsed,
            mode=str(mode),
            account_id=account_id,
            requested_by=requested_by,
            filename=filename,
            notes=notes,
            dry_run=bool(dry_run),
        )
        applied_changes = int(out.get("applied_upserts") or 0) + int(out.get("applied_deletes") or 0)
        if not dry_run and str(out.get("status")) == "ok" and applied_changes > 0:
            record_holdings_dirty(
                action=f"holdings_import:{mode}",
                account_id=account_id,
                summary=(
                    f"{mode} import applied for {account_id}: "
                    f"{int(out.get('applied_upserts') or 0)} upserts, {int(out.get('applied_deletes') or 0)} deletes"
                ),
                import_batch_id=str(out.get("import_batch_id") or "") or None,
                change_count=applied_changes,
            )
        out["refresh"] = trigger_light_refresh_if_requested(
            bool(trigger_refresh and not dry_run and str(out.get("status")) == "ok")
        )
        out["preview_rejections"] = parsed.get("rejected", [])[:100]
        return out
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def run_position_upsert(
    *,
    account_id: str,
    quantity: float,
    ric: str | None = None,
    ticker: str | None = None,
    source: str = "ui_edit",
    requested_by: str | None = None,
    notes: str | None = None,
    dry_run: bool = False,
    trigger_refresh: bool = True,
) -> dict[str, Any]:
    conn = connect(dsn=resolve_dsn(None), autocommit=False)
    try:
        out = apply_single_position_edit(
            conn,
            account_id=account_id,
            quantity=quantity,
            ric=ric,
            ticker=ticker,
            source=source,
            requested_by=requested_by,
            notes=notes,
            dry_run=bool(dry_run),
        )
        if not dry_run and str(out.get("status")) == "ok" and str(out.get("action") or "") != "none":
            record_holdings_dirty(
                action="holdings_position_edit",
                account_id=account_id,
                summary=f"Position {out.get('action')} for {out.get('ticker') or out.get('ric')}",
                import_batch_id=str(out.get("import_batch_id") or "") or None,
                change_count=1,
            )
        out["refresh"] = trigger_light_refresh_if_requested(
            bool(trigger_refresh and not dry_run and str(out.get("status")) == "ok")
        )
        return out
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def run_position_remove(
    *,
    account_id: str,
    ric: str | None = None,
    ticker: str | None = None,
    requested_by: str | None = None,
    notes: str | None = None,
    dry_run: bool = False,
    trigger_refresh: bool = True,
) -> dict[str, Any]:
    conn = connect(dsn=resolve_dsn(None), autocommit=False)
    try:
        out = remove_single_position(
            conn,
            account_id=account_id,
            ric=ric,
            ticker=ticker,
            requested_by=requested_by,
            notes=notes,
            dry_run=bool(dry_run),
        )
        if not dry_run and str(out.get("status")) == "ok" and str(out.get("action") or "") != "none":
            record_holdings_dirty(
                action="holdings_position_remove",
                account_id=account_id,
                summary=f"Position removed for {out.get('ticker') or out.get('ric')}",
                import_batch_id=str(out.get("import_batch_id") or "") or None,
                change_count=1,
            )
        out["refresh"] = trigger_light_refresh_if_requested(
            bool(trigger_refresh and not dry_run and str(out.get("status")) == "ok")
        )
        return out
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


__all__ = [
    "IMPORT_MODES",
    "load_holdings_accounts",
    "load_holdings_positions",
    "run_holdings_import",
    "run_position_remove",
    "run_position_upsert",
    "trigger_light_refresh_if_requested",
    "record_holdings_dirty",
]
