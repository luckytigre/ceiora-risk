from __future__ import annotations

from fastapi.testclient import TestClient

import backend.data.cache as cache_mod
import backend.data.runtime_state as runtime_state_mod
from backend.main import app


def test_health_reports_degraded_when_neon_sync_health_error(monkeypatch) -> None:
    monkeypatch.setattr(cache_mod, "get_cache_age", lambda: 12.5)
    monkeypatch.setattr(
        runtime_state_mod,
        "read_runtime_state",
        lambda key, fallback_loader=None: {"status": "error", "source": "neon", "value": {"status": "error", "message": "mirror mismatch"}} if key == "neon_sync_health" else {"status": "missing", "source": "none", "value": None},
    )
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
    assert body["runtime_state_status"]["neon_sync_health"]["status"] == "error"
    assert body["runtime_state_status"]["neon_sync_health"]["source"] == "neon"


def test_health_reports_ok_when_neon_sync_health_ok(monkeypatch) -> None:
    monkeypatch.setattr(cache_mod, "get_cache_age", lambda: 7.0)
    monkeypatch.setattr(
        runtime_state_mod,
        "read_runtime_state",
        lambda key, fallback_loader=None: {"status": "ok", "source": "neon", "value": {"status": "ok", "message": "healthy"}} if key == "neon_sync_health" else {"status": "missing", "source": "none", "value": None},
    )
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
    assert body["runtime_state_status"]["neon_sync_health"]["status"] == "ok"


def test_health_reports_degraded_when_runtime_state_truth_is_missing(monkeypatch) -> None:
    monkeypatch.setattr(cache_mod, "get_cache_age", lambda: 5.0)
    monkeypatch.setattr(
        runtime_state_mod,
        "read_runtime_state",
        lambda key, fallback_loader=None: {"status": "missing", "source": "neon", "value": None} if key == "neon_sync_health" else {"status": "missing", "source": "none", "value": None},
    )
    monkeypatch.setattr(cache_mod, "cache_get", lambda key: None)

    client = TestClient(app)
    res = client.get("/api/health")
    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "degraded"
    assert body["runtime_state_status"]["neon_sync_health"]["status"] == "missing"
