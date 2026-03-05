"""FastAPI app for Barra Factor Risk Dashboard."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend import config
from backend.routes.portfolio import router as portfolio_router
from backend.routes.exposures import router as exposures_router
from backend.routes.health import router as health_router
from backend.routes.risk import router as risk_router
from backend.routes.refresh import router as refresh_router
from backend.routes.universe import router as universe_router
from backend.routes.data import router as data_router

app = FastAPI(title="Barra Factor Risk Dashboard", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ALLOW_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(portfolio_router, prefix="/api")
app.include_router(exposures_router, prefix="/api")
app.include_router(health_router, prefix="/api")
app.include_router(risk_router, prefix="/api")
app.include_router(refresh_router, prefix="/api")
app.include_router(universe_router, prefix="/api")
app.include_router(data_router, prefix="/api")


@app.get("/api/health")
async def health():
    from backend.db.sqlite import get_cache_age
    try:
        return {"status": "ok", "cache_age_seconds": get_cache_age()}
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "degraded",
            "cache_age_seconds": None,
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
