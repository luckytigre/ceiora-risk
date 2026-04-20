from __future__ import annotations

import pytest
from backend import config
from backend.app_factory import create_app
from backend.orchestration import run_model_pipeline
from pathlib import Path

def test_cloud_control_fails_startup_if_critical_dispatch_config_missing(monkeypatch):
    # Setup control-surface cloud mode with Neon but missing job names
    monkeypatch.setattr(config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(config, "CLOUD_RUN_JOBS_ENABLED", True)

    # Missing these:
    monkeypatch.setattr(config, "CLOUD_RUN_PROJECT_ID", "")
    monkeypatch.setattr(config, "CLOUD_RUN_REGION", "")
    monkeypatch.setattr(config, "SERVE_REFRESH_CLOUD_RUN_JOB_NAME", "")
    monkeypatch.setattr(config, "CORE_WEEKLY_CLOUD_RUN_JOB_NAME", "")
    monkeypatch.setattr(config, "COLD_CORE_CLOUD_RUN_JOB_NAME", "")
    monkeypatch.setattr(config, "CPAR_BUILD_CLOUD_RUN_JOB_NAME", "")

    with pytest.raises(RuntimeError) as excinfo:
        create_app(surface="control")

    assert "Step 3 Cutover Guardrail" in str(excinfo.value)
    assert "CLOUD_RUN_PROJECT_ID" in str(excinfo.value)
    assert "CLOUD_RUN_REGION" in str(excinfo.value)
    assert "SERVE_REFRESH_CLOUD_RUN_JOB_NAME" in str(excinfo.value)
    assert "CORE_WEEKLY_CLOUD_RUN_JOB_NAME" in str(excinfo.value)
    assert "COLD_CORE_CLOUD_RUN_JOB_NAME" in str(excinfo.value)
    assert "CPAR_BUILD_CLOUD_RUN_JOB_NAME" in str(excinfo.value)


def test_cloud_control_starts_if_config_present(monkeypatch):
    monkeypatch.setattr(config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(config, "CLOUD_RUN_JOBS_ENABLED", True)

    monkeypatch.setattr(config, "CLOUD_RUN_PROJECT_ID", "p")
    monkeypatch.setattr(config, "CLOUD_RUN_REGION", "r")
    monkeypatch.setattr(config, "SERVE_REFRESH_CLOUD_RUN_JOB_NAME", "serve")
    monkeypatch.setattr(config, "CORE_WEEKLY_CLOUD_RUN_JOB_NAME", "core")
    monkeypatch.setattr(config, "COLD_CORE_CLOUD_RUN_JOB_NAME", "cold")
    monkeypatch.setattr(config, "CPAR_BUILD_CLOUD_RUN_JOB_NAME", "cpar")

    app = create_app(surface="control")
    assert app is not None


def test_cloud_serve_surface_does_not_require_control_dispatch_contract(monkeypatch):
    monkeypatch.setattr(config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(config, "CLOUD_RUN_JOBS_ENABLED", True)
    monkeypatch.setattr(config, "CLOUD_RUN_PROJECT_ID", "")
    monkeypatch.setattr(config, "CLOUD_RUN_REGION", "")
    monkeypatch.setattr(config, "SERVE_REFRESH_CLOUD_RUN_JOB_NAME", "")
    monkeypatch.setattr(config, "CORE_WEEKLY_CLOUD_RUN_JOB_NAME", "")
    monkeypatch.setattr(config, "COLD_CORE_CLOUD_RUN_JOB_NAME", "")
    monkeypatch.setattr(config, "CPAR_BUILD_CLOUD_RUN_JOB_NAME", "")

    app = create_app(surface="serve")
    assert app is not None


def test_cloud_job_blocks_ingest_stage(monkeypatch):
    monkeypatch.setattr(config, "APP_RUNTIME_ROLE", "cloud-job")

    with pytest.raises(RuntimeError) as excinfo:
        run_model_pipeline.run_model_pipeline(
            profile="source-daily",
            from_stage="ingest",
            to_stage="ingest"
        )

    assert "Step 3 Cutover Guardrail" in str(excinfo.value)
    assert "forbidden from requesting stages: ['ingest']" in str(excinfo.value)


def test_cloud_job_blocks_source_sync_stage_by_default(monkeypatch):
    monkeypatch.setattr(config, "APP_RUNTIME_ROLE", "cloud-job")

    with pytest.raises(RuntimeError) as excinfo:
        run_model_pipeline.run_model_pipeline(
            profile="source-daily",
            from_stage="source_sync",
            to_stage="source_sync"
        )

    assert "Step 3 Cutover Guardrail" in str(excinfo.value)
    assert "forbidden from requesting stages: ['source_sync']" in str(excinfo.value)

def test_local_ingest_allows_ingest_stage(monkeypatch):
    monkeypatch.setattr(config, "APP_RUNTIME_ROLE", "local-ingest")

    monkeypatch.setattr(
        run_model_pipeline,
        "planned_stages_for_profile",
        lambda **kwargs: ("source-daily", {}, ["ingest"])
    )

    with pytest.raises(Exception) as excinfo:
        run_model_pipeline.run_model_pipeline(
            profile="source-daily",
            from_stage="ingest",
            to_stage="ingest",
            data_db=Path("/tmp/non-existent-db-path-123456789")
        )

    assert "Step 3 Cutover Guardrail" not in str(excinfo.value)
