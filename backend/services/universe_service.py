"""Compatibility shim for cUSE4 universe/search/detail route semantics.

Prefer importing `backend.services.cuse4_universe_service` from the default
cUSE4 route family. This module remains only for older callers and direct
service tests that still import the legacy path.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from backend.services import cuse4_universe_service as _owner


DATA_DB = Path(_owner.config.DATA_DB_PATH)
UniversePayloadNotReady = _owner.UniversePayloadNotReady
cache_get = _owner.cache_get
load_price_history_rows = _owner.load_price_history_rows
load_runtime_payload = _owner.load_runtime_payload


def load_universe_payload() -> dict[str, Any]:
    return _owner._load_universe_payload(
        "universe_loadings",
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
    )


def load_universe_factors_payload() -> dict[str, Any]:
    return _owner._load_universe_payload(
        "universe_factors",
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
    )


def load_universe_ticker_payload(
    ticker: str,
    *,
    row_normalizer,
) -> dict[str, Any]:
    return _owner._load_universe_ticker_payload(
        ticker,
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
        row_normalizer=row_normalizer,
    )


def load_universe_ticker_history_payload(ticker: str, *, years: int) -> dict[str, Any]:
    return _owner._load_universe_ticker_history_payload(
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
    return _owner._search_universe_payload(
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
