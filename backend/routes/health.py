"""GET /api/health/diagnostics — model health diagnostics payload."""

from __future__ import annotations

from fastapi import APIRouter

from db.sqlite import cache_get

router = APIRouter()


@router.get("/health/diagnostics")
async def get_health_diagnostics():
    data = cache_get("health_diagnostics")
    if data is not None:
        return {**data, "_cached": True}
    return {
        "status": "no-data",
        "as_of": None,
        "notes": ["Health diagnostics cache not ready. Run /api/refresh to compute and cache."],
        "_cached": False,
    }
