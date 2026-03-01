"""Universe-level cached loadings/search endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from db.sqlite import cache_get

router = APIRouter()


@router.get("/universe/ticker/{ticker}")
async def get_universe_ticker(ticker: str):
    data = cache_get("universe_loadings")
    if data is None:
        raise HTTPException(status_code=503, detail="Universe cache not ready")
    by_ticker = data.get("by_ticker") or {}
    item = by_ticker.get(str(ticker).upper().strip())
    if item is None:
        raise HTTPException(status_code=404, detail="Ticker not found in cached universe")
    return {"item": item, "_cached": True}


@router.get("/universe/search")
async def search_universe(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=200),
):
    data = cache_get("universe_loadings")
    if data is None:
        return {"query": q, "results": [], "total": 0, "_cached": False}

    needle = q.strip().upper()
    if not needle:
        return {"query": q, "results": [], "total": 0, "_cached": True}

    index = data.get("index") or []
    hits = []
    for row in index:
        ticker = str(row.get("ticker", "")).upper()
        name = str(row.get("name", "")).upper()
        if needle in ticker or needle in name:
            hits.append(row)
            if len(hits) >= limit:
                break
    return {"query": q, "results": hits, "total": len(hits), "_cached": True}


@router.get("/universe/factors")
async def get_universe_factors():
    data = cache_get("universe_factors")
    if data is None:
        return {"factors": [], "factor_vols": {}, "_cached": False}
    return {**data, "_cached": True}
