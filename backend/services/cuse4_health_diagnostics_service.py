"""Explicit cUSE4 alias for health-diagnostics route semantics."""

from __future__ import annotations

from backend.services import health_diagnostics_service as _legacy


HealthDiagnosticsNotReady = _legacy.HealthDiagnosticsNotReady
cache_get = _legacy.cache_get
load_runtime_payload = _legacy.load_runtime_payload


def load_health_diagnostics_payload() -> dict[str, object]:
    return _legacy.load_health_diagnostics_payload()


__all__ = [
    "HealthDiagnosticsNotReady",
    "cache_get",
    "load_health_diagnostics_payload",
    "load_runtime_payload",
]
