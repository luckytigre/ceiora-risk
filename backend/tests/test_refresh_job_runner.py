from __future__ import annotations

import pytest

from backend.scripts import run_refresh_job


def test_run_refresh_job_main_executes_refresh_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    state_store: dict[str, object] = {}
    started_calls: list[dict[str, object]] = []

    monkeypatch.setenv("REFRESH_PROFILE", "serve-refresh")
    monkeypatch.setenv("REFRESH_PIPELINE_RUN_ID", "crj_abc")
    monkeypatch.setenv("REFRESH_SCOPE", "holdings_only")
    monkeypatch.setattr(run_refresh_job, "load_persisted_refresh_status", lambda: dict(state_store))
    monkeypatch.setattr(
        run_refresh_job,
        "persist_refresh_status",
        lambda state: state_store.update(state) or dict(state_store),
    )
    monkeypatch.setattr(
        run_refresh_job,
        "mark_refresh_started",
        lambda **kwargs: started_calls.append(kwargs),
    )
    monkeypatch.setattr(
        run_refresh_job,
        "run_refresh_execution",
        lambda **kwargs: {"status": "ok", "result": {"run_id": kwargs["pipeline_run_id"]}},
    )

    assert run_refresh_job.main() == 0
    assert state_store["status"] == "running"
    assert state_store["dispatch_backend"] == "cloud_run_job"
    assert started_calls[0]["run_id"] == "crj_abc"


def test_run_refresh_job_requires_explicit_profile_in_cloud_job_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("REFRESH_PROFILE", raising=False)
    monkeypatch.setattr(run_refresh_job.config, "APP_RUNTIME_ROLE", "cloud-job")

    with pytest.raises(RuntimeError, match="REFRESH_PROFILE"):
        run_refresh_job.main()
