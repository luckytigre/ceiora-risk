from __future__ import annotations

import pytest

from backend.services import cpar_build_service


def test_dispatch_cpar_build_rejects_unknown_profile() -> None:
    with pytest.raises(ValueError, match="Unsupported cPAR profile"):
        cpar_build_service.dispatch_cpar_build(profile="not-a-profile")


def test_dispatch_cpar_build_reports_unconfigured_job(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cpar_build_service.config, "CLOUD_RUN_JOBS_ENABLED", False)

    started, result = cpar_build_service.dispatch_cpar_build(profile="cpar-weekly")

    assert started is False
    assert result["status"] == "unavailable"
    assert result["error"]["type"] == "cloud_run_job_unconfigured"


def test_dispatch_cpar_build_dispatches_cloud_run_job(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(cpar_build_service.config, "CLOUD_RUN_JOBS_ENABLED", True)
    monkeypatch.setattr(cpar_build_service.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(cpar_build_service.config, "CLOUD_RUN_PROJECT_ID", "proj")
    monkeypatch.setattr(cpar_build_service.config, "CLOUD_RUN_REGION", "us-east4")
    monkeypatch.setattr(cpar_build_service.config, "CPAR_BUILD_CLOUD_RUN_JOB_NAME", "ceiora-prod-cpar-build")
    monkeypatch.setattr(
        cpar_build_service.cloud_run_jobs,
        "dispatch_cpar_build",
        lambda **kwargs: captured.update(kwargs) or {
            "execution_name": "projects/p/locations/r/jobs/j/executions/e",
            "metadata": kwargs,
        },
    )

    started, result = cpar_build_service.dispatch_cpar_build(profile="cpar-weekly")

    assert started is True
    assert result["status"] == "dispatched"
    assert result["profile"] == "cpar-weekly"
    assert result["package_date"]
    assert result["dispatch_backend"] == "cloud_run_job"
    assert captured["as_of_date"] == result["package_date"]
