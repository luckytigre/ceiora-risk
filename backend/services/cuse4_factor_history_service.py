"""Explicit cUSE4 alias for factor-history route semantics."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backend.services import factor_history_service as _legacy


FactorHistoryNotReady = _legacy.FactorHistoryNotReady
cache_get = _legacy.cache_get
config = _legacy.config
load_factor_return_history = _legacy.load_factor_return_history
load_runtime_payload = _legacy.load_runtime_payload
resolve_factor_history_factor = _legacy.resolve_factor_history_factor


@dataclass(frozen=True)
class FactorHistoryDependencies:
    payload_loader: Callable[..., Any]
    fallback_loader: Callable[..., Any]
    factor_resolver: Callable[..., tuple[str, str]]
    history_loader: Callable[..., tuple[Any, list[tuple[Any, Any]]]]
    sqlite_path: str | Path


def get_factor_history_dependencies() -> FactorHistoryDependencies:
    return FactorHistoryDependencies(
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
        factor_resolver=resolve_factor_history_factor,
        history_loader=load_factor_return_history,
        sqlite_path=config.SQLITE_PATH,
    )


def resolve_factor_identifier(factor_token: str, *, cache_db: Path | None = None) -> tuple[str, str]:
    deps = get_factor_history_dependencies()
    return _legacy._resolve_factor_identifier(
        factor_token,
        cache_db=cache_db,
        payload_loader=deps.payload_loader,
        fallback_loader=deps.fallback_loader,
        factor_resolver=deps.factor_resolver,
        sqlite_path=deps.sqlite_path,
    )


def load_factor_history_response(
    *,
    factor_token: str,
    years: int,
    cache_db: Path | None = None,
) -> dict[str, object]:
    deps = get_factor_history_dependencies()
    return _legacy._build_factor_history_response(
        factor_token=factor_token,
        years=int(years),
        cache_db=cache_db,
        payload_loader=deps.payload_loader,
        fallback_loader=deps.fallback_loader,
        factor_resolver=deps.factor_resolver,
        history_loader=deps.history_loader,
        sqlite_path=deps.sqlite_path,
    )


__all__ = [
    "FactorHistoryNotReady",
    "FactorHistoryDependencies",
    "cache_get",
    "config",
    "get_factor_history_dependencies",
    "load_factor_history_response",
    "load_factor_return_history",
    "load_runtime_payload",
    "resolve_factor_history_factor",
    "resolve_factor_identifier",
]
