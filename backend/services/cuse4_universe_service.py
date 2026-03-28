"""Explicit cUSE4 alias for default universe/search/detail service surfaces."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from backend.services import universe_service as _legacy


DATA_DB = Path(_legacy.config.DATA_DB_PATH)
UniversePayloadNotReady = _legacy.UniversePayloadNotReady
cache_get = _legacy.cache_get
load_price_history_rows = _legacy.load_price_history_rows
load_runtime_payload = _legacy.load_runtime_payload


def load_universe_payload() -> dict[str, Any]:
    return _legacy._load_universe_payload(
        "universe_loadings",
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
    )


def load_universe_factors_payload() -> dict[str, Any]:
    return _legacy._load_universe_payload(
        "universe_factors",
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
    )


def load_universe_ticker_payload(
    ticker: str,
    *,
    row_normalizer,
) -> dict[str, Any]:
    return _legacy._load_universe_ticker_payload(
        ticker,
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
        row_normalizer=row_normalizer,
    )


def load_universe_ticker_history_payload(ticker: str, *, years: int) -> dict[str, Any]:
    return _legacy._load_universe_ticker_history_payload(
        ticker,
        years=years,
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
        history_loader=load_price_history_rows,
        data_db=DATA_DB,
    )


def search_universe_payload(
    *,
    q: str,
    limit: int,
    row_normalizer,
) -> dict[str, Any]:
    return _legacy._search_universe_payload(
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
