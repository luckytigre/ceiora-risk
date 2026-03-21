"""Stateless serving FastAPI app entrypoint."""

from __future__ import annotations

from backend.app_factory import create_app

app = create_app(surface="serve")
