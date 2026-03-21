"""Application-facing health payload for shared app surfaces."""

from __future__ import annotations

from typing import Any

from backend import config
from backend.data import runtime_state, sqlite


def build_api_health_payload(*, app_surface: str) -> dict[str, Any]:
    fallback_loader = sqlite.cache_get if config.runtime_state_cache_fallback_enabled() else None
    try:
        neon_sync_health_state = runtime_state.read_runtime_state(
            "neon_sync_health",
            fallback_loader=fallback_loader,
        )
        neon_sync_health = neon_sync_health_state.get("value")
        api_status = "ok"
        if str(neon_sync_health_state.get("status") or "") != "ok":
            api_status = "degraded"
        if isinstance(neon_sync_health, dict) and str(neon_sync_health.get("status") or "").lower() == "error":
            api_status = "degraded"
        return {
            "status": api_status,
            "app_surface": str(app_surface or "").strip().lower() or "full",
            "cache_age_seconds": sqlite.get_cache_age() if config.runtime_role_allows_ingest() else None,
            "neon_sync_health": neon_sync_health,
            "runtime_state_status": {
                "neon_sync_health": {
                    "status": str(neon_sync_health_state.get("status") or "unknown"),
                    "source": str(neon_sync_health_state.get("source") or "unknown"),
                    "error": neon_sync_health_state.get("error"),
                }
            },
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "degraded",
            "app_surface": str(app_surface or "").strip().lower() or "full",
            "cache_age_seconds": None,
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
