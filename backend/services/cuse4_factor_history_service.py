"""Concrete cUSE4 owner for factor-history route semantics."""

from __future__ import annotations

from collections.abc import Callable
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backend import config
from backend.data.history_queries import (
    load_factor_return_history,
    resolve_factor_history_factor,
)
from backend.data.serving_outputs import load_runtime_payload
from backend.data.sqlite import cache_get


PayloadLoader = Callable[..., Any]
FactorResolver = Callable[..., tuple[str, str]]
FactorHistoryLoader = Callable[..., tuple[Any, list[tuple[Any, Any]]]]


@dataclass(frozen=True)
class FactorHistoryNotReady(RuntimeError):
    cache_key: str
    message: str
    refresh_profile: str = "cold-core"


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


def _resolve_from_payload_catalog(
    clean: str,
    *,
    payload_loader: PayloadLoader,
    fallback_loader,
) -> tuple[str, str]:
    payload_names = ("universe_factors", "risk", "universe_loadings")
    for payload_name in payload_names:
        payload = payload_loader(payload_name, fallback_loader=fallback_loader)
        catalog = (payload or {}).get("factor_catalog") if isinstance(payload, dict) else None
        if not isinstance(catalog, list):
            continue
        for entry in catalog:
            if not isinstance(entry, dict):
                continue
            entry_id = str(entry.get("factor_id") or "").strip()
            entry_name = str(entry.get("factor_name") or "").strip()
            if clean == entry_id or clean == entry_name:
                return entry_id or clean, entry_name or clean
        return "", ""
    return "", ""


def _resolve_factor_identifier(
    factor_token: str,
    *,
    cache_db: Path | None,
    payload_loader: PayloadLoader,
    fallback_loader,
    factor_resolver: FactorResolver,
    sqlite_path: str | Path,
) -> tuple[str, str]:
    clean = str(factor_token or "").strip()
    if not clean:
        return "", ""
    payload_factor_id, payload_factor_name = _resolve_from_payload_catalog(
        clean,
        payload_loader=payload_loader,
        fallback_loader=fallback_loader,
    )
    if payload_factor_id or payload_factor_name:
        return payload_factor_id, payload_factor_name
    return factor_resolver(
        Path(cache_db or sqlite_path),
        factor_token=clean,
    )


def resolve_factor_identifier(factor_token: str, *, cache_db: Path | None = None) -> tuple[str, str]:
    deps = get_factor_history_dependencies()
    return _resolve_factor_identifier(
        factor_token,
        cache_db=cache_db,
        payload_loader=deps.payload_loader,
        fallback_loader=deps.fallback_loader,
        factor_resolver=deps.factor_resolver,
        sqlite_path=deps.sqlite_path,
    )


def _build_factor_history_response(
    *,
    factor_token: str,
    years: int,
    cache_db: Path | None,
    payload_loader: PayloadLoader,
    fallback_loader,
    factor_resolver: FactorResolver,
    history_loader: FactorHistoryLoader,
    sqlite_path: str | Path,
) -> dict[str, Any]:
    resolved_factor_id, factor_name = _resolve_factor_identifier(
        factor_token,
        cache_db=cache_db,
        payload_loader=payload_loader,
        fallback_loader=fallback_loader,
        factor_resolver=factor_resolver,
        sqlite_path=sqlite_path,
    )
    latest, rows = history_loader(
        Path(cache_db or sqlite_path),
        factor=str(factor_name),
        years=int(years),
    )
    if latest is None:
        raise FactorHistoryNotReady(
            cache_key="daily_factor_returns",
            message="Historical factor returns are not available yet.",
        )
    if not rows:
        return {
            "factor_id": resolved_factor_id,
            "factor_name": factor_name,
            "years": int(years),
            "points": [],
            "_cached": True,
        }

    points = []
    cumulative = 1.0
    for dt, raw_ret in rows:
        value = float(raw_ret or 0.0)
        if not math.isfinite(value):
            value = 0.0
        cumulative *= (1.0 + value)
        points.append(
            {
                "date": str(dt),
                "factor_return": round(value, 8),
                "cum_return": round(cumulative - 1.0, 8),
            }
        )

    return {
        "factor_id": resolved_factor_id,
        "factor_name": factor_name,
        "years": int(years),
        "points": points,
        "_cached": True,
    }


def load_factor_history_response(
    *,
    factor_token: str,
    years: int,
    cache_db: Path | None = None,
    payload_loader: Callable[..., Any] | None = None,
    fallback_loader: Callable[..., Any] | None = None,
    factor_resolver: Callable[..., tuple[str, str]] | None = None,
    history_loader: Callable[..., tuple[Any, list[tuple[Any, Any]]]] | None = None,
    sqlite_path: str | Path | None = None,
) -> dict[str, object]:
    deps = get_factor_history_dependencies()
    return _build_factor_history_response(
        factor_token=factor_token,
        years=int(years),
        cache_db=cache_db,
        payload_loader=payload_loader or deps.payload_loader,
        fallback_loader=fallback_loader or deps.fallback_loader,
        factor_resolver=factor_resolver or deps.factor_resolver,
        history_loader=history_loader or deps.history_loader,
        sqlite_path=sqlite_path or deps.sqlite_path,
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
