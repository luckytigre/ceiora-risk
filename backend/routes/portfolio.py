"""GET /api/portfolio — positions, total_value, position_count."""

from fastapi import APIRouter
from db.sqlite import cache_get

router = APIRouter()


@router.get("/portfolio")
async def get_portfolio():
    data = cache_get("portfolio")
    if data is None:
        return {"positions": [], "total_value": 0.0, "position_count": 0, "_cached": False}
    return {**data, "_cached": True}
