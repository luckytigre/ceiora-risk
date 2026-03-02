"""FastAPI app for Barra Factor Risk Dashboard."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routes.portfolio import router as portfolio_router
from routes.exposures import router as exposures_router
from routes.health import router as health_router
from routes.risk import router as risk_router
from routes.refresh import router as refresh_router
from routes.universe import router as universe_router

app = FastAPI(title="Barra Factor Risk Dashboard", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001", "http://localhost:3002"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(portfolio_router, prefix="/api")
app.include_router(exposures_router, prefix="/api")
app.include_router(health_router, prefix="/api")
app.include_router(risk_router, prefix="/api")
app.include_router(refresh_router, prefix="/api")
app.include_router(universe_router, prefix="/api")


@app.get("/api/health")
async def health():
    from db.sqlite import get_cache_age
    return {"status": "ok", "cache_age_seconds": get_cache_age()}
