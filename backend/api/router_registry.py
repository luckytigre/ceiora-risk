"""Central registry for FastAPI router bundles."""

from __future__ import annotations

from backend.api.routes.auth_context import router as auth_context_router
from backend.api.routes.cpar import router as cpar_router
from backend.api.routes.cpar_control import router as cpar_control_router
from backend.api.routes.cuse_explore import router as cuse_explore_router
from backend.api.routes.cuse_risk_page import router as cuse_risk_page_router
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
    auth_context_router,
    cuse_explore_router,
    cuse_risk_page_router,
    portfolio_router,
    exposures_router,
    risk_router,
    holdings_router,
    universe_router,
    cpar_router,
]

CONTROL_API_ROUTERS = [
    auth_context_router,
    refresh_router,
    operator_router,
    health_router,
    data_router,
    cpar_control_router,
]

API_ROUTERS = [
    auth_context_router,
    cuse_explore_router,
    cuse_risk_page_router,
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
