"""Shared API readiness helpers for cache-backed endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from backend.data.sqlite import cache_get


def cache_not_ready_payload(
    *,
    cache_key: str,
    message: str,
    refresh_mode: str = "light",
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
    refresh = cache_get("refresh_status")
    if isinstance(refresh, dict):
        payload["refresh"] = refresh
    return payload


def raise_cache_not_ready(
    *,
    cache_key: str,
    message: str,
    refresh_mode: str = "light",
    refresh_profile: str = "serve-refresh",
) -> None:
    raise HTTPException(
        status_code=503,
        detail=cache_not_ready_payload(
            cache_key=cache_key,
            message=message,
            refresh_mode=refresh_mode,
            refresh_profile=refresh_profile,
        ),
    )
