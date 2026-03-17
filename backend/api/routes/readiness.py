"""Shared API readiness helpers for cache-backed endpoints."""

from __future__ import annotations

from fastapi import HTTPException

from backend.services import readiness_service


def cache_not_ready_payload(
    *,
    cache_key: str,
    message: str,
    refresh_profile: str = "serve-refresh",
):
    return readiness_service.cache_not_ready_payload(
        cache_key=cache_key,
        message=message,
        refresh_profile=refresh_profile,
    )


def raise_cache_not_ready(
    *,
    cache_key: str,
    message: str,
    refresh_profile: str = "serve-refresh",
) -> None:
    raise HTTPException(
        status_code=503,
        detail=cache_not_ready_payload(
            cache_key=cache_key,
            message=message,
            refresh_profile=refresh_profile,
        ),
    )
