"""Compatibility owner for cUSE4 health-diagnostics route semantics.

Prefer importing `backend.services.cuse4_health_diagnostics_service` from the
default cUSE4 route family. Shared helpers live here so the alias module can
stay thin without losing its monkeypatch seams in route tests.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from backend.data.serving_outputs import load_runtime_payload
from backend.data.sqlite import cache_get


@dataclass(frozen=True)
class HealthDiagnosticsNotReady(RuntimeError):
    cache_key: str
    message: str
    refresh_profile: str = "cold-core"


def load_health_diagnostics_payload() -> dict[str, Any]:
    data = load_runtime_payload("health_diagnostics", fallback_loader=cache_get)
    if data is None:
        raise HealthDiagnosticsNotReady(
            cache_key="health_diagnostics",
            message="Health diagnostics are not ready yet. Run core-weekly, cold-core, or another diagnostics-producing lane.",
        )
    return {**data, "_cached": True}


__all__ = [
    "HealthDiagnosticsNotReady",
    "cache_get",
    "load_health_diagnostics_payload",
    "load_runtime_payload",
]
