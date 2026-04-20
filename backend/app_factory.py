"""FastAPI app factory for full, serve, and control surfaces."""

from __future__ import annotations

import math

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from backend import config
from backend.api.router_registry import routers_for_surface
from backend.services.api_health_service import build_api_health_payload


def _surface_title(surface: str) -> str:
    clean = str(surface or "full").strip().lower()
    if clean == "serve":
        return "cUSE4 Factor Risk Dashboard - Serve"
    if clean == "control":
        return "cUSE4 Factor Risk Dashboard - Control"
    return "cUSE4 Factor Risk Dashboard"


def _sanitize_validation_payload(value):
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, tuple):
        return [_sanitize_validation_payload(item) for item in value]
    if isinstance(value, list):
        return [_sanitize_validation_payload(item) for item in value]
    if isinstance(value, dict):
        return {key: _sanitize_validation_payload(val) for key, val in value.items()}
    return value


def _validate_cloud_dispatch_config(*, surface: str) -> None:
    if not config.cloud_mode():
        return
    if config.DATA_BACKEND != "neon":
        return
    if not config.cloud_run_jobs_enabled():
        return

    clean_surface = str(surface or "full").strip().lower() or "full"
    if clean_surface != "control":
        return

    missing: list[str] = []
    if not config.CLOUD_RUN_PROJECT_ID:
        missing.append("CLOUD_RUN_PROJECT_ID")
    if not config.CLOUD_RUN_REGION:
        missing.append("CLOUD_RUN_REGION")
    if not config.SERVE_REFRESH_CLOUD_RUN_JOB_NAME:
        missing.append("SERVE_REFRESH_CLOUD_RUN_JOB_NAME")
    if not config.CORE_WEEKLY_CLOUD_RUN_JOB_NAME:
        missing.append("CORE_WEEKLY_CLOUD_RUN_JOB_NAME")
    if not config.COLD_CORE_CLOUD_RUN_JOB_NAME:
        missing.append("COLD_CORE_CLOUD_RUN_JOB_NAME")
    if not config.CPAR_BUILD_CLOUD_RUN_JOB_NAME:
        missing.append("CPAR_BUILD_CLOUD_RUN_JOB_NAME")

    if missing:
        raise RuntimeError(
            f"Step 3 Cutover Guardrail: Control-surface cloud dispatch is enabled but missing critical configuration: {', '.join(missing)}. "
            "Ensure the full Cloud Run job contract is set before starting the control surface in cloud-serve mode."
        )


def create_app(*, surface: str = "full") -> FastAPI:
    _validate_cloud_dispatch_config(surface=surface)
    app = FastAPI(title=_surface_title(surface), version="0.1.0")
    app.state.app_surface = str(surface or "full").strip().lower() or "full"

    app.add_middleware(
        CORSMiddleware,
        allow_origins=config.CORS_ALLOW_ORIGINS,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    for router in routers_for_surface(app.state.app_surface):
        app.include_router(router, prefix="/api")

    @app.exception_handler(RequestValidationError)
    async def request_validation_exception_handler(_, exc: RequestValidationError):
        return JSONResponse(
            status_code=422,
            content={"detail": _sanitize_validation_payload(exc.errors())},
        )

    @app.get("/api/health")
    async def health():
        return build_api_health_payload(app_surface=app.state.app_surface)

    return app
