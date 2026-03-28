"""Explicit cUSE4 alias for health-diagnostics route semantics."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from backend.services import health_diagnostics_service as _legacy


HealthDiagnosticsNotReady = _legacy.HealthDiagnosticsNotReady
cache_get = _legacy.cache_get
load_runtime_payload = _legacy.load_runtime_payload


@dataclass(frozen=True)
class HealthDiagnosticsReaders:
    payload_loader: Callable[..., Any]
    fallback_loader: Callable[..., Any]


def get_health_diagnostics_readers() -> HealthDiagnosticsReaders:
    return HealthDiagnosticsReaders(
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
    )


def load_health_diagnostics_payload() -> dict[str, object]:
    readers = get_health_diagnostics_readers()
    data = readers.payload_loader(
        "health_diagnostics",
        fallback_loader=readers.fallback_loader,
    )
    if data is None:
        raise HealthDiagnosticsNotReady(
            cache_key="health_diagnostics",
            message="Health diagnostics are not ready yet. Run core-weekly, cold-core, or another diagnostics-producing lane.",
        )
    return {**data, "_cached": True}


__all__ = [
    "HealthDiagnosticsNotReady",
    "HealthDiagnosticsReaders",
    "cache_get",
    "get_health_diagnostics_readers",
    "load_health_diagnostics_payload",
    "load_runtime_payload",
]
