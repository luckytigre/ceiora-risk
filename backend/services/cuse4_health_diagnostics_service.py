"""Concrete cUSE4 owner for health-diagnostics route semantics."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from backend.data.serving_outputs import load_runtime_payload
from backend.data.serving_outputs import load_runtime_payload_state
from backend.data.sqlite import cache_get


@dataclass(frozen=True)
class HealthDiagnosticsNotReady(RuntimeError):
    cache_key: str
    message: str
    refresh_profile: str = "cold-core"


@dataclass(frozen=True)
class HealthDiagnosticsReaders:
    payload_loader: Callable[..., Any]
    payload_state_loader: Callable[..., dict[str, Any]]
    fallback_loader: Callable[..., Any]


@dataclass(frozen=True)
class HealthDiagnosticsUnavailable(RuntimeError):
    message: str
    source: str = "unknown"
    error: dict[str, Any] | None = None


def get_health_diagnostics_readers() -> HealthDiagnosticsReaders:
    return HealthDiagnosticsReaders(
        payload_loader=load_runtime_payload,
        payload_state_loader=load_runtime_payload_state,
        fallback_loader=cache_get,
    )


def load_health_diagnostics_payload(
    *,
    payload_loader: Callable[..., Any] | None = None,
    payload_state_loader: Callable[..., dict[str, Any]] | None = None,
    fallback_loader: Callable[..., Any] | None = None,
) -> dict[str, object]:
    readers = get_health_diagnostics_readers()
    if payload_state_loader is None:
        if payload_loader is not None:
            data = payload_loader(
                "health_diagnostics",
                fallback_loader=fallback_loader or readers.fallback_loader,
            )
            state = {
                "status": "ok" if data is not None else "missing",
                "source": "compat",
                "value": data,
            }
        else:
            state = readers.payload_state_loader(
                "health_diagnostics",
                fallback_loader=fallback_loader or readers.fallback_loader,
            )
    else:
        state = payload_state_loader(
            "health_diagnostics",
            fallback_loader=fallback_loader or readers.fallback_loader,
        )

    if str(state.get("status") or "") == "missing":
        raise HealthDiagnosticsNotReady(
            cache_key="health_diagnostics",
            message="Health diagnostics are not ready yet. Run core-weekly, cold-core, or another diagnostics-producing lane.",
        )
    if str(state.get("status") or "") != "ok":
        source = str(state.get("source") or "unknown")
        error = state.get("error") if isinstance(state.get("error"), dict) else None
        error_summary = ""
        if error:
            error_type = str(error.get("type") or "").strip()
            error_message = str(error.get("message") or "").strip()
            if error_type or error_message:
                error_summary = f" ({error_type}: {error_message})".rstrip()
        raise HealthDiagnosticsUnavailable(
            message=f"Health diagnostics authority is unavailable from {source}.{error_summary}",
            source=source,
            error=error,
        )

    data = state.get("value")
    return {**dict(data or {}), "_cached": True}


__all__ = [
    "HealthDiagnosticsNotReady",
    "HealthDiagnosticsUnavailable",
    "HealthDiagnosticsReaders",
    "cache_get",
    "get_health_diagnostics_readers",
    "load_health_diagnostics_payload",
    "load_runtime_payload",
    "load_runtime_payload_state",
]
