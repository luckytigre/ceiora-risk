"""FastAPI app for Barra Factor Risk Dashboard."""

import math

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from backend import config
from backend.api import API_ROUTERS
from backend.data import sqlite
from backend.data import runtime_state

app = FastAPI(title="Barra Factor Risk Dashboard", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ALLOW_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

for _router in API_ROUTERS:
    app.include_router(_router, prefix="/api")


def _sanitize_validation_payload(value):
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, list):
        return [_sanitize_validation_payload(item) for item in value]
    if isinstance(value, dict):
        return {key: _sanitize_validation_payload(val) for key, val in value.items()}
    return value


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(_, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={"detail": _sanitize_validation_payload(exc.errors())},
    )


@app.get("/api/health")
async def health():
    try:
        neon_sync_health_state = runtime_state.read_runtime_state(
            "neon_sync_health",
            fallback_loader=sqlite.cache_get,
        )
        neon_sync_health = neon_sync_health_state.get("value")
        api_status = "ok"
        if str(neon_sync_health_state.get("status") or "") != "ok":
            api_status = "degraded"
        if isinstance(neon_sync_health, dict) and str(neon_sync_health.get("status") or "").lower() == "error":
            api_status = "degraded"
        return {
            "status": api_status,
            "cache_age_seconds": sqlite.get_cache_age(),
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
            "cache_age_seconds": None,
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
