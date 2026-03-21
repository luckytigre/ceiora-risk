"""Runtime-aware dispatch for serve-refresh requests."""

from __future__ import annotations

from typing import Any

from backend import config
from backend.services.refresh_status_service import load_persisted_refresh_status


def start_refresh(**kwargs):
    from backend.services.refresh_manager import start_refresh as _start_refresh

    return _start_refresh(**kwargs)


def request_serve_refresh(*, refresh_scope: str = "holdings_only") -> dict[str, Any]:
    if config.runtime_role_allows_ingest():
        started, state = start_refresh(
            profile="serve-refresh",
            force_risk_recompute=False,
            refresh_scope=refresh_scope,
        )
        return {
            "started": bool(started),
            "state": state,
            "dispatch": "in_process",
        }

    return {
        "started": False,
        "dispatch": "control_plane_required",
        "message": "Serve refresh must be triggered from the control-plane app when APP_RUNTIME_ROLE=cloud-serve.",
        "action": {
            "method": "POST",
            "endpoint": "/api/refresh?profile=serve-refresh",
        },
        "state": load_persisted_refresh_status(),
    }
