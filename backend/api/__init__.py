"""API-layer package: route registration and HTTP-facing modules."""

from __future__ import annotations

__all__ = ["API_ROUTERS"]


def __getattr__(name: str):
    if name == "API_ROUTERS":
        from backend.api.router_registry import API_ROUTERS

        return API_ROUTERS
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
