"""Legacy full FastAPI app entrypoint for local/all-in-one runtime."""

from __future__ import annotations

from backend.app_factory import create_app

app = create_app(surface="full")
