from __future__ import annotations

import pytest

from backend.services import refresh_control_service


def test_start_refresh_delegates_to_refresh_manager_when_cloud_jobs_disabled_locally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_JOBS_ENABLED", False)
    monkeypatch.setattr(refresh_control_service.config, "APP_RUNTIME_ROLE", "local-ingest")

    captured: dict[str, object] = {}

    def _start_refresh(**kwargs):
        captured.update(kwargs)
        return True, {"status": "running"}

    monkeypatch.setattr(
        "backend.services.refresh_manager.start_refresh",
        _start_refresh,
    )

    started, state = refresh_control_service.start_refresh(
        force_risk_recompute=False,
        profile="serve-refresh",
    )

    assert started is True
    assert state["status"] == "running"
    assert captured["profile"] == "serve-refresh"


def test_start_refresh_fails_closed_when_cloud_jobs_disabled_in_cloud_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_JOBS_ENABLED", False)
    monkeypatch.setattr(refresh_control_service.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(
        refresh_control_service,
        "load_persisted_refresh_status",
        lambda: {"status": "idle"},
    )
    monkeypatch.setattr(
        "backend.services.refresh_manager.start_refresh",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("local refresh manager should not be used")),
    )

    started, state = refresh_control_service.start_refresh(
        force_risk_recompute=False,
        profile="serve-refresh",
    )

    assert started is False
    assert state["status"] == "idle"
    assert state["dispatch_backend"] == "cloud_run_job"
    assert state["error"]["type"] == "cloud_run_job_unconfigured"


def test_start_refresh_dispatches_cloud_run_job_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    state_store: dict[str, object] = {}
    started_calls: list[dict[str, object]] = []

    monkeypatch.setattr(refresh_control_service.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_JOBS_ENABLED", True)
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_PROJECT_ID", "proj")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_REGION", "us-east4")
    monkeypatch.setattr(refresh_control_service.config, "SERVE_REFRESH_CLOUD_RUN_JOB_NAME", "ceiora-prod-serve-refresh")
    monkeypatch.setattr(refresh_control_service, "load_persisted_refresh_status", lambda: dict(state_store))
    monkeypatch.setattr(
        refresh_control_service,
        "persist_refresh_status",
        lambda state: state_store.update(state) or dict(state_store),
    )
    monkeypatch.setattr(
        refresh_control_service,
        "try_claim_refresh_status",
        lambda updates: (state_store.update(updates) or True, dict(state_store)),
    )
    monkeypatch.setattr(
        refresh_control_service,
        "mark_refresh_started",
        lambda **kwargs: started_calls.append(kwargs),
    )
    monkeypatch.setattr(
        refresh_control_service.cloud_run_jobs,
        "dispatch_serve_refresh",
        lambda **kwargs: {"execution_name": "executions/abc", "metadata": kwargs},
    )

    started, state = refresh_control_service.start_refresh(
        force_risk_recompute=False,
        profile="serve-refresh",
        refresh_scope="holdings_only",
    )

    assert started is True
    assert state["status"] == "running"
    assert state["dispatch_backend"] == "cloud_run_job"
    assert state["dispatch_id"] == "executions/abc"
    assert state["profile"] == "serve-refresh"
    assert started_calls[0]["profile"] == "serve-refresh"
    assert str(started_calls[0]["run_id"]).startswith("crj_")


def test_start_refresh_reports_cloud_run_dispatch_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    state_store: dict[str, object] = {}
    finished_calls: list[dict[str, object]] = []

    monkeypatch.setattr(refresh_control_service.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_JOBS_ENABLED", True)
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_PROJECT_ID", "proj")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_REGION", "us-east4")
    monkeypatch.setattr(refresh_control_service.config, "SERVE_REFRESH_CLOUD_RUN_JOB_NAME", "ceiora-prod-serve-refresh")
    monkeypatch.setattr(refresh_control_service, "load_persisted_refresh_status", lambda: dict(state_store))
    monkeypatch.setattr(
        refresh_control_service,
        "persist_refresh_status",
        lambda state: state_store.update(state) or dict(state_store),
    )
    monkeypatch.setattr(
        refresh_control_service,
        "try_claim_refresh_status",
        lambda updates: (state_store.update(updates) or True, dict(state_store)),
    )
    monkeypatch.setattr(refresh_control_service, "mark_refresh_started", lambda **kwargs: None)
    monkeypatch.setattr(
        refresh_control_service,
        "mark_refresh_finished",
        lambda **kwargs: finished_calls.append(kwargs),
    )
    monkeypatch.setattr(
        refresh_control_service.cloud_run_jobs,
        "dispatch_serve_refresh",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("dispatch failed")),
    )

    started, state = refresh_control_service.start_refresh(
        force_risk_recompute=False,
        profile="serve-refresh",
    )

    assert started is False
    assert state["status"] == "failed"
    assert state["dispatch_backend"] == "cloud_run_job"
    assert state["error"]["message"] == "dispatch failed"
    assert finished_calls[0]["status"] == "failed"


def test_start_refresh_refuses_duplicate_cloud_run_dispatch_when_claim_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = {
        "status": "running",
        "dispatch_backend": "cloud_run_job",
        "pipeline_run_id": "crj_existing",
    }

    monkeypatch.setattr(refresh_control_service.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_JOBS_ENABLED", True)
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_PROJECT_ID", "proj")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_REGION", "us-east4")
    monkeypatch.setattr(refresh_control_service.config, "SERVE_REFRESH_CLOUD_RUN_JOB_NAME", "ceiora-prod-serve-refresh")
    monkeypatch.setattr(
        refresh_control_service,
        "try_claim_refresh_status",
        lambda updates: (False, dict(current)),
    )
    monkeypatch.setattr(
        refresh_control_service.cloud_run_jobs,
        "dispatch_serve_refresh",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("dispatch should not be called")),
    )

    started, state = refresh_control_service.start_refresh(
        force_risk_recompute=False,
        profile="serve-refresh",
    )

    assert started is False
    assert state == current


def test_get_refresh_status_reads_persisted_state_when_cloud_jobs_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(refresh_control_service.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_JOBS_ENABLED", True)
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_PROJECT_ID", "proj")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_REGION", "us-east4")
    monkeypatch.setattr(refresh_control_service.config, "SERVE_REFRESH_CLOUD_RUN_JOB_NAME", "ceiora-prod-serve-refresh")
    monkeypatch.setattr(
        refresh_control_service,
        "load_persisted_refresh_status",
        lambda: {"status": "running", "dispatch_backend": "cloud_run_job"},
    )
    monkeypatch.setattr(
        refresh_control_service.cloud_run_jobs,
        "describe_execution",
        lambda execution_name: {"conditions": []},
    )
    monkeypatch.setattr(
        refresh_control_service.cloud_run_jobs,
        "execution_terminal_summary",
        lambda execution: {"terminal": False, "status": "running", "finished_at": None, "message": None},
    )

    assert refresh_control_service.get_refresh_status() == {
        "status": "running",
        "dispatch_backend": "cloud_run_job",
    }


def test_get_refresh_status_fails_closed_when_cloud_jobs_disabled_in_cloud_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(refresh_control_service.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_JOBS_ENABLED", False)
    monkeypatch.setattr(
        refresh_control_service,
        "load_persisted_refresh_status",
        lambda: {"status": "idle"},
    )
    monkeypatch.setattr(
        "backend.services.refresh_manager.get_refresh_status",
        lambda: (_ for _ in ()).throw(AssertionError("local refresh manager should not be used")),
    )

    out = refresh_control_service.get_refresh_status()

    assert out["status"] == "idle"
    assert out["dispatch_backend"] == "cloud_run_job"
    assert out["error"]["type"] == "cloud_run_job_unconfigured"


def test_get_refresh_status_reconciles_failed_cloud_run_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    state_store = {
        "status": "running",
        "dispatch_backend": "cloud_run_job",
        "dispatch_id": "ceiora-prod-serve-refresh-96psv",
        "job_id": "ceiora-prod-serve-refresh-96psv",
        "profile": "serve-refresh",
        "pipeline_run_id": "crj_7f5fbebdf9d3",
    }
    finished_calls: list[dict[str, object]] = []

    monkeypatch.setattr(refresh_control_service.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_JOBS_ENABLED", True)
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_PROJECT_ID", "proj")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_REGION", "us-east4")
    monkeypatch.setattr(refresh_control_service.config, "SERVE_REFRESH_CLOUD_RUN_JOB_NAME", "ceiora-prod-serve-refresh")
    monkeypatch.setattr(refresh_control_service, "load_persisted_refresh_status", lambda: dict(state_store))
    monkeypatch.setattr(
        refresh_control_service,
        "persist_refresh_status",
        lambda state: state_store.update(state) or dict(state_store),
    )
    monkeypatch.setattr(
        refresh_control_service,
        "mark_refresh_finished",
        lambda **kwargs: finished_calls.append(kwargs),
    )
    monkeypatch.setattr(
        refresh_control_service.cloud_run_jobs,
        "describe_execution",
        lambda execution_name: {"name": execution_name},
    )
    monkeypatch.setattr(
        refresh_control_service.cloud_run_jobs,
        "execution_terminal_summary",
        lambda execution: {
            "terminal": True,
            "status": "failed",
            "finished_at": "2026-03-23T16:59:03.652825Z",
            "message": "The configured memory limit was reached.",
        },
    )

    out = refresh_control_service.get_refresh_status()

    assert out["status"] == "failed"
    assert out["finished_at"] == "2026-03-23T16:59:03.652825Z"
    assert out["error"]["type"] == "cloud_run_job_failed"
    assert "memory limit was reached" in out["error"]["message"]
    assert finished_calls[0]["status"] == "failed"
    assert finished_calls[0]["run_id"] == "crj_7f5fbebdf9d3"


def test_get_refresh_status_leaves_running_state_on_cloud_run_lookup_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_store = {
        "status": "running",
        "dispatch_backend": "cloud_run_job",
        "dispatch_id": "ceiora-prod-serve-refresh-96psv",
        "job_id": "ceiora-prod-serve-refresh-96psv",
        "profile": "serve-refresh",
        "pipeline_run_id": "crj_7f5fbebdf9d3",
    }

    monkeypatch.setattr(refresh_control_service.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_JOBS_ENABLED", True)
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_PROJECT_ID", "proj")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_REGION", "us-east4")
    monkeypatch.setattr(refresh_control_service.config, "SERVE_REFRESH_CLOUD_RUN_JOB_NAME", "ceiora-prod-serve-refresh")
    monkeypatch.setattr(refresh_control_service, "load_persisted_refresh_status", lambda: dict(state_store))
    monkeypatch.setattr(
        refresh_control_service.cloud_run_jobs,
        "describe_execution",
        lambda execution_name: (_ for _ in ()).throw(RuntimeError("transient google api error")),
    )

    out = refresh_control_service.get_refresh_status()

    assert out["status"] == "running"
    assert out["dispatch_id"] == "ceiora-prod-serve-refresh-96psv"


def test_get_refresh_status_reconciles_succeeded_cloud_run_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    state_store = {
        "status": "running",
        "dispatch_backend": "cloud_run_job",
        "dispatch_id": "ceiora-prod-serve-refresh-96psv",
        "job_id": "ceiora-prod-serve-refresh-96psv",
        "profile": "serve-refresh",
        "pipeline_run_id": "crj_7f5fbebdf9d3",
    }
    finished_calls: list[dict[str, object]] = []

    monkeypatch.setattr(refresh_control_service.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_JOBS_ENABLED", True)
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_PROJECT_ID", "proj")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_REGION", "us-east4")
    monkeypatch.setattr(refresh_control_service.config, "SERVE_REFRESH_CLOUD_RUN_JOB_NAME", "ceiora-prod-serve-refresh")
    monkeypatch.setattr(refresh_control_service, "load_persisted_refresh_status", lambda: dict(state_store))
    monkeypatch.setattr(
        refresh_control_service,
        "persist_refresh_status",
        lambda state: state_store.update(state) or dict(state_store),
    )
    monkeypatch.setattr(
        refresh_control_service,
        "mark_refresh_finished",
        lambda **kwargs: finished_calls.append(kwargs),
    )
    monkeypatch.setattr(
        refresh_control_service.cloud_run_jobs,
        "describe_execution",
        lambda execution_name: {"name": execution_name},
    )
    monkeypatch.setattr(
        refresh_control_service.cloud_run_jobs,
        "execution_terminal_summary",
        lambda execution: {
            "terminal": True,
            "status": "ok",
            "finished_at": "2026-03-23T17:05:00Z",
            "message": "Execution completed successfully.",
        },
    )

    out = refresh_control_service.get_refresh_status()

    assert out["status"] == "ok"
    assert out["error"] is None
    assert out["result"]["reconciled"] is True
    assert out["result"]["execution_name"] == "ceiora-prod-serve-refresh-96psv"
    assert finished_calls[0]["status"] == "ok"
    assert finished_calls[0]["clear_pending"] is True


def test_start_refresh_reconciles_terminal_cloud_run_execution_before_new_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_store = {
        "status": "running",
        "dispatch_backend": "cloud_run_job",
        "dispatch_id": "ceiora-prod-serve-refresh-96psv",
        "job_id": "ceiora-prod-serve-refresh-96psv",
        "profile": "serve-refresh",
        "pipeline_run_id": "crj_old",
    }
    finished_calls: list[dict[str, object]] = []
    started_calls: list[dict[str, object]] = []

    monkeypatch.setattr(refresh_control_service.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_JOBS_ENABLED", True)
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_PROJECT_ID", "proj")
    monkeypatch.setattr(refresh_control_service.config, "CLOUD_RUN_REGION", "us-east4")
    monkeypatch.setattr(refresh_control_service.config, "SERVE_REFRESH_CLOUD_RUN_JOB_NAME", "ceiora-prod-serve-refresh")
    monkeypatch.setattr(refresh_control_service, "load_persisted_refresh_status", lambda: dict(state_store))
    monkeypatch.setattr(
        refresh_control_service,
        "persist_refresh_status",
        lambda state: state_store.update(state) or dict(state_store),
    )
    monkeypatch.setattr(
        refresh_control_service,
        "mark_refresh_finished",
        lambda **kwargs: finished_calls.append(kwargs),
    )
    monkeypatch.setattr(
        refresh_control_service,
        "mark_refresh_started",
        lambda **kwargs: started_calls.append(kwargs),
    )
    monkeypatch.setattr(
        refresh_control_service.cloud_run_jobs,
        "describe_execution",
        lambda execution_name: {"name": execution_name},
    )
    monkeypatch.setattr(
        refresh_control_service.cloud_run_jobs,
        "execution_terminal_summary",
        lambda execution: {
            "terminal": True,
            "status": "failed",
            "finished_at": "2026-03-23T16:59:03.652825Z",
            "message": "The configured memory limit was reached.",
        },
    )
    monkeypatch.setattr(
        refresh_control_service,
        "try_claim_refresh_status",
        lambda updates: (state_store.update(updates) or True, dict(state_store)),
    )
    monkeypatch.setattr(
        refresh_control_service.cloud_run_jobs,
        "dispatch_serve_refresh",
        lambda **kwargs: {"execution_name": "executions/new-exec", "metadata": kwargs},
    )

    started, state = refresh_control_service.start_refresh(
        force_risk_recompute=False,
        profile="serve-refresh",
    )

    assert started is True
    assert state["status"] == "running"
    assert state["dispatch_id"] == "executions/new-exec"
    assert started_calls[0]["profile"] == "serve-refresh"
    assert finished_calls[0]["run_id"] == "crj_old"
