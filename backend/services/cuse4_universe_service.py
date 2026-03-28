"""Concrete cUSE4 owner for default universe/search/detail route semantics."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from backend import config
from backend.data.history_queries import load_price_history_rows
from backend.data.serving_outputs import load_runtime_payload
from backend.data.sqlite import cache_get


DATA_DB = Path(config.DATA_DB_PATH)


@dataclass(frozen=True)
class UniversePayloadNotReady(RuntimeError):
    cache_key: str
    message: str
    refresh_profile: str = "serve-refresh"


PayloadLoader = Callable[..., Any]
HistoryLoader = Callable[..., tuple[Any, list[tuple[Any, Any]]]]
RowNormalizer = Callable[[dict[str, Any]], dict[str, Any]]


def _search_rank(row: dict[str, Any], needle: str) -> tuple[int, int, str]:
    ticker = str(row.get("ticker", "")).upper()
    name = str(row.get("name", "")).upper()
    ric = str(row.get("ric", "")).upper()

    if ticker == needle:
        return (0, 0, ticker)
    if ric == needle:
        return (0, 1, ticker)
    if ticker.startswith(needle):
        return (1, len(ticker), ticker)
    if ric.startswith(needle):
        return (1, len(ric), ticker)
    if needle in ticker:
        return (2, ticker.find(needle), ticker)
    if needle in ric:
        return (2, ric.find(needle), ticker)
    if name.startswith(needle):
        return (3, len(name), ticker)
    return (4, name.find(needle), ticker)


def _week_ending_friday(day: date) -> date:
    return day + timedelta(days=(4 - day.weekday()))


def _load_universe_payload(
    payload_name: str,
    *,
    payload_loader: PayloadLoader,
    fallback_loader,
) -> dict[str, Any]:
    payload = payload_loader(payload_name, fallback_loader=fallback_loader)
    if payload is None:
        raise UniversePayloadNotReady(
            cache_key=payload_name,
            message=(
                "Universe cache is not ready yet. Run refresh and try again."
                if payload_name == "universe_loadings"
                else "Universe factor cache is not ready yet."
            ),
        )
    return payload


def load_universe_payload() -> dict[str, Any]:
    return _load_universe_payload(
        "universe_loadings",
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
    )


def load_universe_factors_payload() -> dict[str, Any]:
    return _load_universe_payload(
        "universe_factors",
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
    )


def _load_universe_ticker_payload(
    ticker: str,
    *,
    payload_loader: PayloadLoader,
    fallback_loader,
    row_normalizer: RowNormalizer,
) -> dict[str, Any]:
    data = _load_universe_payload(
        "universe_loadings",
        payload_loader=payload_loader,
        fallback_loader=fallback_loader,
    )
    by_ticker = data.get("by_ticker") or {}
    item = by_ticker.get(str(ticker).upper().strip())
    if item is None:
        raise HTTPException(status_code=404, detail="Ticker not found in cached universe")
    return {"item": row_normalizer(dict(item)), "_cached": True}


def load_universe_ticker_payload(
    ticker: str,
    *,
    row_normalizer: RowNormalizer,
) -> dict[str, Any]:
    return _load_universe_ticker_payload(
        ticker,
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
        row_normalizer=row_normalizer,
    )


def _load_universe_ticker_history_payload(
    ticker: str,
    *,
    years: int,
    payload_loader: PayloadLoader,
    fallback_loader,
    history_loader: HistoryLoader,
    data_db: Path,
) -> dict[str, Any]:
    data = _load_universe_payload(
        "universe_loadings",
        payload_loader=payload_loader,
        fallback_loader=fallback_loader,
    )
    clean_ticker = str(ticker).upper().strip()
    item = (data.get("by_ticker") or {}).get(clean_ticker)
    if item is None:
        raise HTTPException(status_code=404, detail="Ticker not found in cached universe")
    ric = str(item.get("ric") or "").strip()
    if not ric:
        raise HTTPException(status_code=404, detail="RIC mapping unavailable for ticker")

    latest_date, rows = history_loader(
        data_db,
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
            day = datetime.fromisoformat(d_txt).date()
            close = float(close_raw)
        except (TypeError, ValueError):
            continue
        week_end = _week_ending_friday(day).isoformat()
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


def load_universe_ticker_history_payload(ticker: str, *, years: int) -> dict[str, Any]:
    return _load_universe_ticker_history_payload(
        ticker,
        years=years,
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
        history_loader=load_price_history_rows,
        data_db=DATA_DB,
    )


def _search_universe_payload(
    *,
    q: str,
    limit: int,
    payload_loader: PayloadLoader,
    fallback_loader,
    row_normalizer: RowNormalizer,
) -> dict[str, Any]:
    data = _load_universe_payload(
        "universe_loadings",
        payload_loader=payload_loader,
        fallback_loader=fallback_loader,
    )
    needle = str(q).strip().upper()
    if not needle:
        return {"query": q, "results": [], "total": 0, "_cached": True}

    index = data.get("index") or []
    by_ticker = data.get("by_ticker") or {}
    ranked: list[tuple[tuple[int, int, str], dict[str, Any]]] = []
    for row in index:
        ticker = str(row.get("ticker", "")).upper()
        name = str(row.get("name", "")).upper()
        ric = str(row.get("ric", "")).upper()
        if needle in ticker or needle in name or needle in ric:
            normalized = row_normalizer(dict(row))
            if not normalized.get("ric"):
                resolved_ric = str((by_ticker.get(ticker) or {}).get("ric") or "").upper().strip()
                if resolved_ric:
                    normalized["ric"] = resolved_ric
            ranked.append((_search_rank(normalized, needle), normalized))

    ranked.sort(key=lambda item: item[0])
    hits = [row for _, row in ranked[:limit]]
    return {"query": q, "results": hits, "total": len(hits), "_cached": True}


def search_universe_payload(
    *,
    q: str,
    limit: int,
    row_normalizer: RowNormalizer,
) -> dict[str, Any]:
    return _search_universe_payload(
        q=q,
        limit=limit,
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
        row_normalizer=row_normalizer,
    )


__all__ = [
    "DATA_DB",
    "UniversePayloadNotReady",
    "cache_get",
    "load_price_history_rows",
    "load_runtime_payload",
    "load_universe_factors_payload",
    "load_universe_payload",
    "load_universe_ticker_history_payload",
    "load_universe_ticker_payload",
    "search_universe_payload",
]
