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


def test_refresh_rejects_invalid_force_core_stage_window(monkeypatch) -> None:
    monkeypatch.setattr(refresh_routes.config, "REFRESH_API_TOKEN", "secret-token")
    monkeypatch.setattr(
        refresh_routes,
        "start_refresh",
        lambda **_kwargs: (_ for _ in ()).throw(
            ValueError("force_core requires a stage window that includes factor_returns and risk_model")
        ),
    )

    client = TestClient(app)
    res = client.post(
        "/api/refresh?force_core=true&from_stage=serving_refresh&to_stage=serving_refresh",
        headers={"X-Refresh-Token": "secret-token"},
    )
    assert res.status_code == 400
    assert res.json()["status"] == "invalid_request"


def test_refresh_reports_failed_start_when_worker_creation_fails(monkeypatch) -> None:
    monkeypatch.setattr(refresh_routes.config, "REFRESH_API_TOKEN", "secret-token")
    monkeypatch.setattr(
        refresh_routes,
        "start_refresh",
        lambda **_kwargs: (
            False,
            {
                "status": "failed",
                "error": {"type": "RuntimeError", "message": "thread start failed"},
            },
        ),
    )

    client = TestClient(app)
    res = client.post("/api/refresh", headers={"X-Refresh-Token": "secret-token"})
    assert res.status_code == 500
    assert res.json()["status"] == "failed_to_start"
