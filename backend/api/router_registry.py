"""Central registry for FastAPI routers."""

from backend.api.routes.data import router as data_router
from backend.api.routes.exposures import router as exposures_router
from backend.api.routes.health import router as health_router
from backend.api.routes.holdings import router as holdings_router
from backend.api.routes.portfolio import router as portfolio_router
from backend.api.routes.refresh import router as refresh_router
from backend.api.routes.risk import router as risk_router
from backend.api.routes.universe import router as universe_router

API_ROUTERS = [
    portfolio_router,
    exposures_router,
    health_router,
    risk_router,
    refresh_router,
    holdings_router,
    universe_router,
    data_router,
]
