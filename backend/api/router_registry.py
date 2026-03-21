"""Central registry for FastAPI router bundles."""

from __future__ import annotations

from backend.api.routes.cpar import router as cpar_router
from backend.api.routes.data import router as data_router
from backend.api.routes.exposures import router as exposures_router
from backend.api.routes.health import router as health_router
from backend.api.routes.holdings import router as holdings_router
from backend.api.routes.operator import router as operator_router
from backend.api.routes.portfolio import router as portfolio_router
from backend.api.routes.refresh import router as refresh_router
from backend.api.routes.risk import router as risk_router
from backend.api.routes.universe import router as universe_router

SERVE_API_ROUTERS = [
    portfolio_router,
    exposures_router,
    risk_router,
    holdings_router,
    universe_router,
    cpar_router,
]

CONTROL_API_ROUTERS = [
    refresh_router,
    operator_router,
    health_router,
    data_router,
]

API_ROUTERS = [
    portfolio_router,
    exposures_router,
    risk_router,
    holdings_router,
    universe_router,
    cpar_router,
    refresh_router,
    operator_router,
    health_router,
    data_router,
]

FULL_API_ROUTERS = API_ROUTERS


def routers_for_surface(surface: str) -> list:
    clean = str(surface or "full").strip().lower()
    if clean == "full":
        return list(FULL_API_ROUTERS)
    if clean == "serve":
        return list(SERVE_API_ROUTERS)
    if clean == "control":
        return list(CONTROL_API_ROUTERS)
    raise ValueError(f"unknown app surface: {surface}")
