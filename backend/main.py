"""FastAPI app for Barra Factor Risk Dashboard."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend import config
from backend.api import API_ROUTERS

app = FastAPI(title="Barra Factor Risk Dashboard", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ALLOW_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

for _router in API_ROUTERS:
    app.include_router(_router, prefix="/api")


@app.get("/api/health")
async def health():
    from backend.data.cache import cache_get, get_cache_age
    try:
        neon_sync_health = cache_get("neon_sync_health")
        api_status = "ok"
        if isinstance(neon_sync_health, dict) and str(neon_sync_health.get("status") or "").lower() == "error":
            api_status = "degraded"
        return {
            "status": api_status,
            "cache_age_seconds": get_cache_age(),
            "neon_sync_health": neon_sync_health,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "degraded",
            "cache_age_seconds": None,
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
