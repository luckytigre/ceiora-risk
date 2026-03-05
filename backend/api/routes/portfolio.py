"""GET /api/portfolio — positions, total_value, position_count."""

from fastapi import APIRouter

from backend.api.routes.presenters import normalize_trbc_sector_fields
from backend.api.routes.readiness import raise_cache_not_ready
from backend.data.sqlite import cache_get

router = APIRouter()


@router.get("/portfolio")
async def get_portfolio():
    data = cache_get("portfolio")
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
