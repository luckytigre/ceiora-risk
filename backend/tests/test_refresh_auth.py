from __future__ import annotations

from fastapi.testclient import TestClient

from backend.main import app
from backend.routes import refresh as refresh_routes


def test_refresh_requires_token_when_configured(monkeypatch) -> None:
    monkeypatch.setattr(refresh_routes.config, "REFRESH_API_TOKEN", "secret-token")
    monkeypatch.setattr(
        refresh_routes,
        "start_refresh",
        lambda **_kwargs: (True, {"status": "running"}),
    )

    client = TestClient(app)
    res = client.post("/api/refresh")
    assert res.status_code == 401


def test_refresh_accepts_valid_token_when_configured(monkeypatch) -> None:
    monkeypatch.setattr(refresh_routes.config, "REFRESH_API_TOKEN", "secret-token")
    monkeypatch.setattr(
        refresh_routes,
        "start_refresh",
        lambda **_kwargs: (True, {"status": "running"}),
    )

    client = TestClient(app)
    res = client.post("/api/refresh", headers={"X-Refresh-Token": "secret-token"})
    assert res.status_code == 202
    body = res.json()
    assert body["status"] == "accepted"
