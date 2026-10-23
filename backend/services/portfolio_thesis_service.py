"""Authenticated workspace classification service; no analytics or refresh ownership."""
from __future__ import annotations

import os

import psycopg

from backend.data import portfolio_thesis_store as store
from backend.data.account_scope import (
    AccountScopeDenied, AccountScopeProvisioningError, resolve_effective_principal,
)
from backend.data.neon import connect, resolve_dsn


# Public service errors used by the thin HTTP adapter.
ThesisError = store.ThesisError


def request_for_principal(*, principal, operation: str, payload: dict | None = None) -> dict:
    """Validate the canonical principal before opening the owned workspace transaction."""
    if enabled_for(principal.subject, principal.provider):
        conn = connect(dsn=resolve_dsn(None), autocommit=True)
        try:
            principal = resolve_effective_principal(conn, principal=principal)
        finally:
            conn.close()
    return request(subject=principal.subject, provider=principal.provider, operation=operation, payload=payload)


def enabled_for(subject: str, provider: str) -> bool:
    enabled = os.getenv("PORTFOLIO_THESES_ENABLED", "false").lower() == "true"
    owners = {part.strip() for part in os.getenv("PORTFOLIO_THESES_OWNER_SUBJECTS", "").split(",") if part.strip()}
    shared = os.getenv("PORTFOLIO_THESES_ALLOW_SHARED_AUTH", "false").lower() == "true"
    return enabled and subject in owners and (provider == "neon" or provider == "shared" and shared)


def request(*, subject: str, provider: str, operation: str, payload: dict | None = None, connection_factory=None) -> dict:
    if not enabled_for(subject, provider):
        if operation == "capability":
            return {"available": False, "reason": "not_enabled"}
        raise store.ThesisUnavailable("Thesis workspace is not enabled for this session.")
    data = payload or {}
    conn = (connection_factory or connect)(dsn=resolve_dsn(None), autocommit=False)
    try:
        workspace, accounts = store.open_workspace(conn, subject=subject)
        if operation not in {"read", "capability"}:
            store.assert_revision(conn, workspace, data["expected_revision"])
        if operation == "create":
            store.create_thesis(conn, workspace, data["name"].strip())
        elif operation in {"rename", "delete"}:
            store.change_thesis(conn, workspace, str(data["thesis_id"]), data["name"].strip() if operation == "rename" else None)
        elif operation == "allocate":
            store.replace_allocations(conn, workspace, accounts, account_id=data["account_id"].strip().lower(), ric=data["ric"].strip().upper(), allocations=data["allocations"])
        elif operation == "order":
            store.save_order(conn, workspace, data["scope"], data["keys"])
        elif operation not in {"read", "capability"}:
            raise store.ThesisError("Unknown thesis operation.")
        result = {"available": True, "workspace_id": workspace} if operation == "capability" else store.read_state(conn, workspace)
        conn.commit()
        return result
    except store.ThesisUnavailable:
        conn.rollback()
        if operation == "capability":
            return {"available": False, "reason": "account_not_provisioned"}
        raise
    except psycopg.Error as exc:
        conn.rollback()
        if exc.sqlstate == "23505":
            raise store.ThesisConflict("A thesis with that name already exists.") from exc
        if exc.sqlstate in {"PTH01", "PTH04", "40001", "40P01"}:
            raise store.ThesisConflict("Portfolio data changed or the write could not complete. Refresh and retry.") from exc
        if exc.sqlstate == "PTH02":
            raise store.ThesisDenied("Account is outside this workspace.") from exc
        if exc.sqlstate in {"23503", "23514", "22003", "PTH03"}:
            raise store.ThesisError("Thesis data does not match the current portfolio.") from exc
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
