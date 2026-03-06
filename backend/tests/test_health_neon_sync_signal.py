from __future__ import annotations

from fastapi.testclient import TestClient

import backend.data.cache as cache_mod
from backend.main import app


def test_health_reports_degraded_when_neon_sync_health_error(monkeypatch) -> None:
    monkeypatch.setattr(cache_mod, "get_cache_age", lambda: 12.5)
    monkeypatch.setattr(
        cache_mod,
        "cache_get",
        lambda key: {"status": "error", "message": "mirror mismatch"} if key == "neon_sync_health" else None,
    )

    client = TestClient(app)
    res = client.get("/api/health")
    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "degraded"
    assert body["cache_age_seconds"] == 12.5
    assert body["neon_sync_health"]["status"] == "error"


def test_health_reports_ok_when_neon_sync_health_ok(monkeypatch) -> None:
    monkeypatch.setattr(cache_mod, "get_cache_age", lambda: 7.0)
    monkeypatch.setattr(
        cache_mod,
        "cache_get",
        lambda key: {"status": "ok", "message": "healthy"} if key == "neon_sync_health" else None,
    )

    client = TestClient(app)
    res = client.get("/api/health")
    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "ok"
    assert body["cache_age_seconds"] == 7.0
    assert body["neon_sync_health"]["status"] == "ok"
