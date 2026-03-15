"""Universe-level cached loadings/search endpoints."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query

from backend import config
from backend.api.routes.presenters import normalize_trbc_sector_fields
from backend.api.routes.readiness import raise_cache_not_ready
from backend.data.history_queries import load_price_history_rows
from backend.data.serving_outputs import load_current_payload
from backend.data.sqlite import cache_get

router = APIRouter()
DATA_DB = Path(config.DATA_DB_PATH)


def _search_rank(row: dict, needle: str) -> tuple[int, int, str]:
    """Rank search hits with ticker intent first, then RIC, then company-name intent."""
    ticker = str(row.get("ticker", "")).upper()
    name = str(row.get("name", "")).upper()
    ric = str(row.get("ric", "")).upper()

    if ticker == needle:
        return (0, 0, ticker)  # exact ticker
    if ric == needle:
        return (0, 1, ticker)  # exact RIC
    if ticker.startswith(needle):
        return (1, len(ticker), ticker)  # ticker prefix
    if ric.startswith(needle):
        return (1, len(ric), ticker)  # RIC prefix
    if needle in ticker:
        return (2, ticker.find(needle), ticker)  # ticker contains
    if needle in ric:
        return (2, ric.find(needle), ticker)  # RIC contains
    if name.startswith(needle):
        return (3, len(name), ticker)  # company prefix
    return (4, name.find(needle), ticker)  # company contains


def _week_ending_friday(d: date) -> date:
    # weekday(): Mon=0 ... Fri=4 ... Sun=6
    return d + timedelta(days=(4 - d.weekday()))


def _load_universe_payload():
    data = load_current_payload("universe_loadings")
    if data is None and not config.cloud_mode():
        data = cache_get("universe_loadings")
    return data


def _load_universe_factors_payload():
    data = load_current_payload("universe_factors")
    if data is None and not config.cloud_mode():
        data = cache_get("universe_factors")
    return data


@router.get("/universe/ticker/{ticker}")
async def get_universe_ticker(ticker: str):
    data = _load_universe_payload()
    if data is None:
        raise_cache_not_ready(
            cache_key="universe_loadings",
            message="Universe cache is not ready yet. Run refresh and try again.",
        )
    by_ticker = data.get("by_ticker") or {}
    item = by_ticker.get(str(ticker).upper().strip())
    if item is None:
        raise HTTPException(status_code=404, detail="Ticker not found in cached universe")
    return {"item": normalize_trbc_sector_fields(item), "_cached": True}


@router.get("/universe/ticker/{ticker}/history")
def get_universe_ticker_history(
    ticker: str,
    years: int = Query(5, ge=1, le=20),
):
    data = _load_universe_payload()
    if data is None:
        raise_cache_not_ready(
            cache_key="universe_loadings",
            message="Universe cache is not ready yet. Run refresh and try again.",
        )
    clean_ticker = str(ticker).upper().strip()
    item = (data.get("by_ticker") or {}).get(clean_ticker)
    if item is None:
        raise HTTPException(status_code=404, detail="Ticker not found in cached universe")
    ric = str(item.get("ric") or "").strip()
    if not ric:
        raise HTTPException(status_code=404, detail="RIC mapping unavailable for ticker")

    latest_date, rows = load_price_history_rows(
        DATA_DB,
        ric=ric,
        years=int(years),
    )
    if not latest_date:
        raise HTTPException(status_code=404, detail="No price history found for ticker")

    week_close: dict[str, float] = {}
    for d_raw, close_raw in rows:
        if d_raw is None or close_raw is None:
            continue
        d_txt = str(d_raw).strip()
        try:
            d = datetime.fromisoformat(d_txt).date()
            close = float(close_raw)
        except (TypeError, ValueError):
            continue
        week_end = _week_ending_friday(d).isoformat()
        # Rows are sorted asc by date, so later rows in the same week overwrite earlier ones.
        week_close[week_end] = close

    points = [
        {"date": week_end, "close": round(float(close), 4)}
        for week_end, close in sorted(week_close.items())
    ]
    return {
        "ticker": clean_ticker,
        "ric": ric,
        "years": int(years),
        "points": points,
        "_cached": True,
    }


@router.get("/universe/search")
async def search_universe(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=200),
):
    data = _load_universe_payload()
    if data is None:
        raise_cache_not_ready(
            cache_key="universe_loadings",
            message="Universe search is unavailable until cache is built.",
        )

    needle = q.strip().upper()
    if not needle:
        return {"query": q, "results": [], "total": 0, "_cached": True}

    index = data.get("index") or []
    by_ticker = data.get("by_ticker") or {}
    ranked: list[tuple[tuple[int, int, str], dict]] = []
    for row in index:
        ticker = str(row.get("ticker", "")).upper()
        name = str(row.get("name", "")).upper()
        ric = str(row.get("ric", "")).upper()
        if needle in ticker or needle in name or needle in ric:
            normalized = normalize_trbc_sector_fields(row)
            if not normalized.get("ric"):
                ric = str((by_ticker.get(ticker) or {}).get("ric") or "").upper().strip()
                if ric:
                    normalized["ric"] = ric
            ranked.append((_search_rank(normalized, needle), normalized))

    ranked.sort(key=lambda item: item[0])
    hits = [row for _, row in ranked[:limit]]
    return {"query": q, "results": hits, "total": len(hits), "_cached": True}


@router.get("/universe/factors")
async def get_universe_factors():
    data = _load_universe_factors_payload()
    if data is None:
        raise_cache_not_ready(
            cache_key="universe_factors",
            message="Universe factor cache is not ready yet.",
        )
    return {**data, "_cached": True}
