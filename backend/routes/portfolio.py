"""GET /api/portfolio — positions, total_value, position_count."""

from fastapi import APIRouter

from analytics.trbc_sector import abbreviate_trbc_sector
from db.sqlite import cache_get

router = APIRouter()


@router.get("/portfolio")
async def get_portfolio():
    data = cache_get("portfolio")
    if data is None:
        return {"positions": [], "total_value": 0.0, "position_count": 0, "_cached": False}
    positions = []
    for raw in data.get("positions", []):
        trbc_sector = str(raw.get("trbc_sector") or raw.get("sector") or "")
        positions.append(
            {
                **raw,
                "trbc_sector": trbc_sector,
                "trbc_sector_abbr": str(raw.get("trbc_sector_abbr") or abbreviate_trbc_sector(trbc_sector)),
            }
        )
    return {**data, "positions": positions, "_cached": True}
