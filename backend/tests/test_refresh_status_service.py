from __future__ import annotations

import pytest

from backend.services import refresh_status_service


def test_load_persisted_refresh_status_merges_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        refresh_status_service.runtime_state,
        "load_runtime_state",
        lambda state_key, fallback_loader=None: {
            "status": "running",
            "profile": "serve-refresh",
            "job_id": "api_123",
        },
    )

    out = refresh_status_service.load_persisted_refresh_status()

    assert out["status"] == "running"
    assert out["profile"] == "serve-refresh"
    assert out["job_id"] == "api_123"
    assert out["current_stage"] is None


def test_read_persisted_refresh_status_preserves_running_value_without_worker_assumptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        refresh_status_service.runtime_state,
        "read_runtime_state",
        lambda state_key, fallback_loader=None: {
            "status": "ok",
            "source": "neon",
            "value": {
                "status": "running",
                "profile": "serve-refresh",
                "pipeline_run_id": "api_run_1",
            },
        },
    )

    out = refresh_status_service.read_persisted_refresh_status()

    assert out["status"] == "ok"
    assert out["source"] == "neon"
    assert out["value"]["status"] == "running"
    assert out["value"]["pipeline_run_id"] == "api_run_1"
