"""Explicit cUSE4 alias for default dashboard payload assembly."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from backend.services import dashboard_payload_service as _legacy


DashboardPayloadNotReady = _legacy.DashboardPayloadNotReady
cache_get = _legacy.cache_get
load_runtime_payload = _legacy.load_runtime_payload


@dataclass(frozen=True)
class DashboardPayloadReaders:
    payload_loader: Callable[..., Any]
    fallback_loader: Callable[..., Any]


def get_dashboard_payload_readers() -> DashboardPayloadReaders:
    return DashboardPayloadReaders(
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
    )


def load_exposures_response(
    *,
    mode: str,
    payload_loader: Callable[..., Any] | None = None,
    fallback_loader=None,
) -> dict[str, Any]:
    readers = get_dashboard_payload_readers()
    return _legacy.load_exposures_response(
        mode=mode,
        payload_loader=payload_loader or readers.payload_loader,
        fallback_loader=fallback_loader or readers.fallback_loader,
    )


def load_risk_response(
    *,
    payload_loader: Callable[..., Any] | None = None,
    fallback_loader=None,
) -> dict[str, Any]:
    readers = get_dashboard_payload_readers()
    return _legacy.load_risk_response(
        payload_loader=payload_loader or readers.payload_loader,
        fallback_loader=fallback_loader or readers.fallback_loader,
    )


def load_portfolio_response(
    *,
    position_normalizer=None,
    payload_loader: Callable[..., Any] | None = None,
    fallback_loader=None,
) -> dict[str, Any]:
    readers = get_dashboard_payload_readers()
    return _legacy.load_portfolio_response(
        position_normalizer=position_normalizer,
        payload_loader=payload_loader or readers.payload_loader,
        fallback_loader=fallback_loader or readers.fallback_loader,
    )


__all__ = [
    "DashboardPayloadNotReady",
    "DashboardPayloadReaders",
    "cache_get",
    "get_dashboard_payload_readers",
    "load_exposures_response",
    "load_portfolio_response",
    "load_risk_response",
    "load_runtime_payload",
]
