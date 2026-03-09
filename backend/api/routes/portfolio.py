"""GET /api/portfolio — positions, total_value, position_count."""

from fastapi import APIRouter

from backend import config
from backend.api.routes.presenters import normalize_trbc_sector_fields
from backend.api.routes.readiness import raise_cache_not_ready
from backend.data.serving_outputs import load_current_payload
from backend.data.sqlite import cache_get

router = APIRouter()


@router.get("/portfolio")
async def get_portfolio():
    if config.cloud_mode() and config.neon_surface_enabled("serving_outputs"):
        data = load_current_payload("portfolio")
    elif config.serving_outputs_primary_reads_enabled() and config.neon_surface_enabled("serving_outputs"):
        data = load_current_payload("portfolio") or cache_get("portfolio")
    else:
        data = cache_get("portfolio") or load_current_payload("portfolio")
    if data is None:
        raise_cache_not_ready(
            cache_key="portfolio",
            message="Portfolio cache is empty. Run refresh to build positions.",
            refresh_mode="light",
        )
    positions = []
    for raw in data.get("positions", []):
        positions.append(normalize_trbc_sector_fields(raw))
    return {**data, "positions": positions, "_cached": True}
