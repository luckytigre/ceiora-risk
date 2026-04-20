"""Concrete cUSE4 owner for holdings read/mutation semantics."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import logging
from typing import Any

from backend.data.neon import connect, resolve_dsn
from backend.services.holdings_runtime_state import mark_holdings_dirty
from backend.services.neon_holdings import (
    IMPORT_MODES,
    apply_ticker_bucket_scenario,
    apply_holdings_import,
    apply_single_position_edit,
    list_holdings_accounts,
    list_holdings_positions,
    parse_holdings_rows,
    remove_single_position,
)
from backend.services.refresh_dispatcher import request_serve_refresh

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HoldingsDependencies:
    dsn_resolver: Callable[[str | None], str]
    connection_factory: Callable[..., Any]
    accounts_loader: Callable[..., list[dict[str, Any]]]
    positions_loader: Callable[..., list[dict[str, Any]]]
    rows_parser: Callable[..., dict[str, Any]]
    import_applier: Callable[..., dict[str, Any]]
    position_upserter: Callable[..., dict[str, Any]]
    position_remover: Callable[..., dict[str, Any]]
    scenario_applier: Callable[..., dict[str, Any]]
    dirty_recorder: Callable[..., None]
    refresh_requester: Callable[[bool], dict[str, Any] | None]


def get_holdings_dependencies() -> HoldingsDependencies:
    return HoldingsDependencies(
        dsn_resolver=resolve_dsn,
        connection_factory=connect,
        accounts_loader=list_holdings_accounts,
        positions_loader=list_holdings_positions,
        rows_parser=parse_holdings_rows,
        import_applier=apply_holdings_import,
        position_upserter=apply_single_position_edit,
        position_remover=remove_single_position,
        scenario_applier=apply_ticker_bucket_scenario,
        dirty_recorder=record_holdings_dirty,
        refresh_requester=trigger_light_refresh_if_requested,
    )


def trigger_light_refresh_if_requested(
    trigger: bool,
    *,
    refresh_requester: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    if not bool(trigger):
        return None
    return (refresh_requester or request_serve_refresh)(refresh_scope="holdings_only")


def record_holdings_dirty(
    *,
    action: str,
    account_id: str | None,
    summary: str,
    import_batch_id: str | None,
    change_count: int,
    dirty_marker: Callable[..., None] | None = None,
) -> None:
    try:
        (dirty_marker or mark_holdings_dirty)(
            action=action,
            account_id=account_id,
            summary=summary,
            import_batch_id=import_batch_id,
            change_count=change_count,
        )
    except Exception:
        logger.exception("Failed to persist holdings dirty state")


def load_holdings_accounts(
    *,
    allowed_account_ids: list[str] | tuple[str, ...] | None = None,
    dependencies: HoldingsDependencies | None = None,
) -> list[dict[str, Any]]:
    deps = dependencies or get_holdings_dependencies()
    conn = deps.connection_factory(dsn=deps.dsn_resolver(None), autocommit=True)
    try:
        return deps.accounts_loader(conn, allowed_account_ids=allowed_account_ids)
    finally:
        conn.close()


def load_holdings_positions(
    account_id: str | None = None,
    *,
    allowed_account_ids: list[str] | tuple[str, ...] | None = None,
    dependencies: HoldingsDependencies | None = None,
) -> list[dict[str, Any]]:
    deps = dependencies or get_holdings_dependencies()
    conn = deps.connection_factory(dsn=deps.dsn_resolver(None), autocommit=True)
    try:
        return deps.positions_loader(conn, account_id=account_id, allowed_account_ids=allowed_account_ids)
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
    dependencies: HoldingsDependencies | None = None,
) -> dict[str, Any]:
    deps = dependencies or get_holdings_dependencies()
    conn = deps.connection_factory(dsn=deps.dsn_resolver(None), autocommit=False)
    try:
        parsed = deps.rows_parser(
            conn,
            rows=rows,
            mode=str(mode),
            default_account_id=account_id,
            default_source=default_source,
        )
        out = deps.import_applier(
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
            deps.dirty_recorder(
                action=f"holdings_import:{mode}",
                account_id=account_id,
                summary=(
                    f"{mode} import applied for {account_id}: "
                    f"{int(out.get('applied_upserts') or 0)} upserts, {int(out.get('applied_deletes') or 0)} deletes"
                ),
                import_batch_id=str(out.get("import_batch_id") or "") or None,
                change_count=applied_changes,
            )
        out["refresh"] = deps.refresh_requester(
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
    dependencies: HoldingsDependencies | None = None,
) -> dict[str, Any]:
    deps = dependencies or get_holdings_dependencies()
    conn = deps.connection_factory(dsn=deps.dsn_resolver(None), autocommit=False)
    try:
        out = deps.position_upserter(
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
            deps.dirty_recorder(
                action="holdings_position_edit",
                account_id=account_id,
                summary=f"Position {out.get('action')} for {out.get('ticker') or out.get('ric')}",
                import_batch_id=str(out.get("import_batch_id") or "") or None,
                change_count=1,
            )
        out["refresh"] = deps.refresh_requester(
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
    dependencies: HoldingsDependencies | None = None,
) -> dict[str, Any]:
    deps = dependencies or get_holdings_dependencies()
    conn = deps.connection_factory(dsn=deps.dsn_resolver(None), autocommit=False)
    try:
        out = deps.position_remover(
            conn,
            account_id=account_id,
            ric=ric,
            ticker=ticker,
            requested_by=requested_by,
            notes=notes,
            dry_run=bool(dry_run),
        )
        if not dry_run and str(out.get("status")) == "ok" and str(out.get("action") or "") != "none":
            deps.dirty_recorder(
                action="holdings_position_remove",
                account_id=account_id,
                summary=f"Position removed for {out.get('ticker') or out.get('ric')}",
                import_batch_id=str(out.get("import_batch_id") or "") or None,
                change_count=1,
            )
        out["refresh"] = deps.refresh_requester(
            bool(trigger_refresh and not dry_run and str(out.get("status")) == "ok")
        )
        return out
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def run_whatif_apply(
    *,
    scenario_rows: list[dict[str, Any]],
    requested_by: str | None = None,
    default_source: str = "what_if",
    dry_run: bool = False,
    dependencies: HoldingsDependencies | None = None,
) -> dict[str, Any]:
    deps = dependencies or get_holdings_dependencies()
    conn = deps.connection_factory(dsn=deps.dsn_resolver(None), autocommit=False)
    try:
        out = deps.scenario_applier(
            conn,
            scenario_rows=scenario_rows,
            requested_by=requested_by,
            default_source=default_source,
            dry_run=bool(dry_run),
        )
        if not dry_run and str(out.get("status")) == "ok":
            change_count = int(out.get("applied_upserts") or 0) + int(out.get("applied_deletes") or 0)
            if change_count > 0:
                deps.dirty_recorder(
                    action="whatif_apply",
                    account_id=None,
                    summary=f"What-if apply committed: {change_count} row mutations across {len(out.get('import_batch_ids') or {})} accounts",
                    import_batch_id=None,
                    change_count=change_count,
                )
        return out
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


__all__ = [
    "IMPORT_MODES",
    "HoldingsDependencies",
    "get_holdings_dependencies",
    "load_holdings_accounts",
    "load_holdings_positions",
    "logger",
    "record_holdings_dirty",
    "run_holdings_import",
    "run_position_remove",
    "run_position_upsert",
    "run_whatif_apply",
    "trigger_light_refresh_if_requested",
]
