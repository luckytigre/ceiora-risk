"""Readiness payload helpers for cache-backed endpoints."""

from __future__ import annotations

from typing import Any

from backend.data.sqlite import cache_get
from backend.services.refresh_status_service import load_persisted_refresh_status


def cache_not_ready_payload(
    *,
    cache_key: str,
    message: str,
    refresh_profile: str = "serve-refresh",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": "not_ready",
        "error": "cache_not_ready",
        "cache_key": cache_key,
        "message": message,
        "action": {
            "method": "POST",
            "endpoint": f"/api/refresh?profile={refresh_profile}",
        },
    }
    refresh = load_persisted_refresh_status(fallback_loader=cache_get)
    if isinstance(refresh, dict):
        payload["refresh"] = refresh
    return payload
