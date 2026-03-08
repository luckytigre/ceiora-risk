"""Authoritative holdings/recalc sync state stored in the cache."""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any

from backend.data.cache import cache_get, cache_set

_CACHE_KEY = "holdings_sync_state"
_STATE_LOCK = threading.Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _base_state() -> dict[str, Any]:
    return {
        "pending": False,
        "pending_count": 0,
        "dirty_revision": 0,
        "dirty_since": None,
        "last_mutation_at": None,
        "last_mutation_kind": None,
        "last_mutation_summary": None,
        "last_mutation_account_id": None,
        "last_import_batch_id": None,
        "last_refresh_started_at": None,
        "last_refresh_finished_at": None,
        "last_refresh_status": None,
        "last_refresh_profile": None,
        "last_refresh_run_id": None,
        "last_refresh_message": None,
        "last_refresh_started_dirty_revision": None,
    }


def get_holdings_sync_state() -> dict[str, Any]:
    cached = cache_get(_CACHE_KEY)
    if not isinstance(cached, dict):
        return _base_state()
    out = _base_state()
    out.update(cached)
    out["pending"] = bool(out.get("pending"))
    out["pending_count"] = max(0, int(out.get("pending_count") or 0))
    return out


def mark_holdings_dirty(
    *,
    action: str,
    account_id: str | None,
    summary: str,
    import_batch_id: str | None = None,
    change_count: int = 1,
) -> dict[str, Any]:
    with _STATE_LOCK:
        state = get_holdings_sync_state()
        now = _now_iso()
        if not state.get("dirty_since"):
            state["dirty_since"] = now
        state["pending"] = True
        state["pending_count"] = max(1, int(state.get("pending_count") or 0) + max(1, int(change_count or 1)))
        state["dirty_revision"] = int(state.get("dirty_revision") or 0) + 1
        state["last_mutation_at"] = now
        state["last_mutation_kind"] = str(action or "holdings_edit")
        state["last_mutation_summary"] = str(summary or "Holdings changed")
        state["last_mutation_account_id"] = str(account_id or "").upper() or None
        state["last_import_batch_id"] = str(import_batch_id or "").strip() or None
        cache_set(_CACHE_KEY, state)
        return state


def mark_refresh_started(*, profile: str, run_id: str | None) -> dict[str, Any]:
    with _STATE_LOCK:
        state = get_holdings_sync_state()
        state["last_refresh_started_at"] = _now_iso()
        state["last_refresh_profile"] = str(profile or "").strip() or None
        state["last_refresh_run_id"] = str(run_id or "").strip() or None
        state["last_refresh_status"] = "running"
        state["last_refresh_message"] = "Serving refresh started"
        state["last_refresh_started_dirty_revision"] = int(state.get("dirty_revision") or 0)
        cache_set(_CACHE_KEY, state)
        return state


def mark_refresh_finished(
    *,
    profile: str,
    run_id: str | None,
    status: str,
    message: str | None = None,
    clear_pending: bool = False,
) -> dict[str, Any]:
    with _STATE_LOCK:
        state = get_holdings_sync_state()
        state["last_refresh_finished_at"] = _now_iso()
        state["last_refresh_profile"] = str(profile or "").strip() or None
        state["last_refresh_run_id"] = str(run_id or "").strip() or None
        state["last_refresh_status"] = str(status or "").strip() or "unknown"
        state["last_refresh_message"] = str(message or "").strip() or None
        if clear_pending and int(state.get("last_refresh_started_dirty_revision") or 0) == int(state.get("dirty_revision") or 0):
            state["pending"] = False
            state["pending_count"] = 0
            state["dirty_since"] = None
        cache_set(_CACHE_KEY, state)
        return state
