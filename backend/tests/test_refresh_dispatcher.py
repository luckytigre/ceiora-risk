from __future__ import annotations

import pytest

from backend.services import refresh_dispatcher


def test_request_serve_refresh_runs_in_process_when_local(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(refresh_dispatcher.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(
        refresh_dispatcher,
        "start_refresh",
        lambda **kwargs: captured.update(kwargs) or (True, {"status": "running"}),
    )

    out = refresh_dispatcher.request_serve_refresh(refresh_scope="holdings_only")

    assert out == {
        "started": True,
        "state": {"status": "running"},
        "dispatch": "in_process",
    }
    assert captured["profile"] == "serve-refresh"
    assert captured["force_risk_recompute"] is False
    assert captured["refresh_scope"] == "holdings_only"


def test_request_serve_refresh_requires_control_plane_in_cloud_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(refresh_dispatcher.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(
        refresh_dispatcher,
        "load_persisted_refresh_status",
        lambda: {"status": "idle", "profile": None},
    )

    out = refresh_dispatcher.request_serve_refresh(refresh_scope="holdings_only")

    assert out["started"] is False
    assert out["dispatch"] == "control_plane_required"
    assert out["action"] == {
        "method": "POST",
        "endpoint": "/api/refresh?profile=serve-refresh",
    }
    assert out["state"] == {"status": "idle", "profile": None}
    assert "control-plane app" in str(out["message"])
