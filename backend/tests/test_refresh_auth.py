from __future__ import annotations

from fastapi.testclient import TestClient

from backend.main import app
from backend.api.routes import refresh as refresh_routes


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


def test_refresh_status_requires_operator_token_in_cloud_mode(monkeypatch) -> None:
    monkeypatch.setattr(refresh_routes.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_routes.config, "OPERATOR_API_TOKEN", "secret-token")
    monkeypatch.setattr(refresh_routes, "get_refresh_status", lambda: {"status": "idle"})

    client = TestClient(app)
    assert client.get("/api/refresh/status").status_code == 401
    res = client.get("/api/refresh/status", headers={"X-Refresh-Token": "secret-token"})
    assert res.status_code == 200
    assert res.json()["refresh"]["status"] == "idle"
