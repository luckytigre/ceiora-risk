"""GET /api/risk — variance decomposition, factor details, covariance matrix."""

from fastapi import APIRouter
from db.sqlite import cache_get

router = APIRouter()


@router.get("/risk")
async def get_risk():
    data = cache_get("risk")
    if data is None:
        return {
            "risk_shares": {"industry": 0, "style": 0, "idio": 100},
            "component_shares": {"industry": 0, "style": 0},
            "factor_details": [],
            "cov_matrix": {},
            "r_squared": 0.0,
            "condition_number": 0.0,
            "model_sanity": {"status": "no-data", "warnings": [], "checks": {}},
            "_cached": False,
        }
    sanity = cache_get("model_sanity") or {"status": "no-data", "warnings": [], "checks": {}}
    return {**data, "model_sanity": sanity, "_cached": True}
