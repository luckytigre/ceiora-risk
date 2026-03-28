"""Explicit cUSE4 alias for factor-history route semantics."""

from __future__ import annotations

from pathlib import Path

from backend.services import factor_history_service as _legacy


FactorHistoryNotReady = _legacy.FactorHistoryNotReady
cache_get = _legacy.cache_get
config = _legacy.config
load_factor_return_history = _legacy.load_factor_return_history
load_runtime_payload = _legacy.load_runtime_payload
resolve_factor_history_factor = _legacy.resolve_factor_history_factor


def resolve_factor_identifier(factor_token: str, *, cache_db: Path | None = None) -> tuple[str, str]:
    return _legacy._resolve_factor_identifier(
        factor_token,
        cache_db=cache_db,
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
        factor_resolver=resolve_factor_history_factor,
        sqlite_path=config.SQLITE_PATH,
    )


def load_factor_history_response(
    *,
    factor_token: str,
    years: int,
    cache_db: Path | None = None,
) -> dict[str, object]:
    return _legacy._build_factor_history_response(
        factor_token=factor_token,
        years=int(years),
        cache_db=cache_db,
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
        factor_resolver=resolve_factor_history_factor,
        history_loader=load_factor_return_history,
        sqlite_path=config.SQLITE_PATH,
    )


__all__ = [
    "FactorHistoryNotReady",
    "cache_get",
    "config",
    "load_factor_history_response",
    "load_factor_return_history",
    "load_runtime_payload",
    "resolve_factor_history_factor",
    "resolve_factor_identifier",
]
