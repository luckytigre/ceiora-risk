"""Universe-level cached loadings/search endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from analytics.trbc_sector import abbreviate_trbc_sector
from db.sqlite import cache_get

router = APIRouter()


def _normalize_universe_item(item: dict) -> dict:
    trbc_sector = str(item.get("trbc_sector") or item.get("sector") or "")
    return {
        **item,
        "trbc_sector": trbc_sector,
        "trbc_sector_abbr": str(item.get("trbc_sector_abbr") or abbreviate_trbc_sector(trbc_sector)),
    }


def _normalize_search_row(row: dict) -> dict:
    trbc_sector = str(row.get("trbc_sector") or row.get("sector") or "")
    return {
        **row,
        "trbc_sector": trbc_sector,
        "trbc_sector_abbr": str(row.get("trbc_sector_abbr") or abbreviate_trbc_sector(trbc_sector)),
    }


@router.get("/universe/ticker/{ticker}")
async def get_universe_ticker(ticker: str):
    data = cache_get("universe_loadings")
    if data is None:
        raise HTTPException(status_code=503, detail="Universe cache not ready")
    by_ticker = data.get("by_ticker") or {}
    item = by_ticker.get(str(ticker).upper().strip())
    if item is None:
        raise HTTPException(status_code=404, detail="Ticker not found in cached universe")
    return {"item": _normalize_universe_item(item), "_cached": True}


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
            hits.append(_normalize_search_row(row))
            if len(hits) >= limit:
                break
    return {"query": q, "results": hits, "total": len(hits), "_cached": True}


@router.get("/universe/factors")
async def get_universe_factors():
    data = cache_get("universe_factors")
    if data is None:
        return {"factors": [], "factor_vols": {}, "_cached": False}
    return {**data, "_cached": True}
