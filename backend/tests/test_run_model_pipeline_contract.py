from __future__ import annotations

import importlib
from contextlib import nullcontext
from pathlib import Path

import pytest

run_model_pipeline_module = importlib.import_module("backend.orchestration.run_model_pipeline")


def test_run_model_pipeline_clears_pending_after_serving_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(run_model_pipeline_module.job_runs, "ensure_schema", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "fail_stale_running_stages", lambda *args, **kwargs: 0)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "completed_stages", lambda *args, **kwargs: set())
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "begin_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "finish_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "run_rows", lambda *args, **kwargs: [])
    monkeypatch.setattr(run_model_pipeline_module.sqlite, "cache_get", lambda key: {})
    monkeypatch.setattr(run_model_pipeline_module.sqlite, "cache_get_live_first", lambda key: {})
    monkeypatch.setattr(
        run_model_pipeline_module,
        "_run_stage",
        lambda **kwargs: {"status": "ok", "stage": kwargs.get("stage")},
    )
    monkeypatch.setattr(
        run_model_pipeline_module,
        "mark_refresh_finished",
        lambda **kwargs: captured.update(kwargs),
    )
    monkeypatch.setattr(run_model_pipeline_module.config, "NEON_AUTO_SYNC_ENABLED", False)

    out = run_model_pipeline_module.run_model_pipeline(profile="serve-refresh")

    assert out["status"] == "ok"
    assert captured["status"] == "ok"
    assert captured["clear_pending"] is True
    assert captured["profile"] == "serve-refresh"


def test_run_model_pipeline_serve_refresh_does_not_require_source_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "ensure_schema", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "fail_stale_running_stages", lambda *args, **kwargs: 0)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "completed_stages", lambda *args, **kwargs: set())
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "begin_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "finish_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "run_rows", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        run_model_pipeline_module.core_reads,
        "load_source_dates",
        lambda: (_ for _ in ()).throw(AssertionError("serve-refresh should not read source dates")),
    )
    monkeypatch.setattr(run_model_pipeline_module.sqlite, "cache_get", lambda key: {})
    monkeypatch.setattr(run_model_pipeline_module.sqlite, "cache_get_live_first", lambda key: {})
    monkeypatch.setattr(
        run_model_pipeline_module,
        "_run_stage",
        lambda **kwargs: {"status": "ok", "stage": kwargs.get("stage"), "as_of_date": kwargs.get("as_of_date")},
    )
    monkeypatch.setattr(
        run_model_pipeline_module,
        "mark_refresh_finished",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(run_model_pipeline_module.config, "NEON_AUTO_SYNC_ENABLED", False)

    out = run_model_pipeline_module.run_model_pipeline(profile="serve-refresh")

    assert out["status"] == "ok"
    assert out["stage_results"][0]["details"]["as_of_date"] is not None


def test_resolved_as_of_date_uses_local_source_archive_when_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _load_source_dates():
        captured["backend"] = run_model_pipeline_module.core_reads.core_read_backend_name()
        return {"fundamentals_asof": "2026-03-14"}

    monkeypatch.setattr(run_model_pipeline_module.core_reads, "load_source_dates", _load_source_dates)
    monkeypatch.setattr(
        run_model_pipeline_module.core_reads.config,
        "neon_surface_enabled",
        lambda surface: surface == "core_reads",
    )
    monkeypatch.setattr(run_model_pipeline_module.core_reads, "neon_core_read_session", lambda: nullcontext())

    out = run_model_pipeline_module.stage_planning.resolved_as_of_date(
        None,
        prefer_local_source_archive=True,
        current_xnys_session_resolver=lambda: "2026-03-14",
    )

    assert out == "2026-03-13"
    assert captured["backend"] == "local"


def test_source_daily_defaults_ingest_to_current_session_not_stored_source_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(run_model_pipeline_module.job_runs, "ensure_schema", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "fail_stale_running_stages", lambda *args, **kwargs: 0)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "completed_stages", lambda *args, **kwargs: set())
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "begin_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "finish_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "run_rows", lambda *args, **kwargs: [])
    monkeypatch.setattr(run_model_pipeline_module.sqlite, "cache_get", lambda key: {})
    monkeypatch.setattr(run_model_pipeline_module.sqlite, "cache_get_live_first", lambda key: {})
    monkeypatch.setattr(
        run_model_pipeline_module.core_reads,
        "load_source_dates",
        lambda: {"fundamentals_asof": "2026-03-04", "exposures_asof": "2026-03-04"},
    )
    monkeypatch.setattr(
        run_model_pipeline_module.stage_planning,
        "current_xnys_session",
        lambda **_kwargs: "2026-03-14",
    )
    monkeypatch.setattr(
        run_model_pipeline_module.runtime_support,
        "risk_recompute_due",
        lambda *_args, **_kwargs: (False, "within_interval"),
    )
    monkeypatch.setattr(run_model_pipeline_module, "mark_refresh_finished", lambda **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.config, "NEON_DATABASE_URL", "")
    monkeypatch.setattr(run_model_pipeline_module.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(run_model_pipeline_module.config, "NEON_AUTO_SYNC_ENABLED", False)
    monkeypatch.setattr(run_model_pipeline_module.config, "NEON_AUTO_PARITY_ENABLED", False)
    monkeypatch.setattr(run_model_pipeline_module.config, "NEON_AUTO_PRUNE_ENABLED", False)

    def _run_selected_stages(**kwargs):
        captured.setdefault("as_of_dates", []).append(kwargs["as_of"])
        return {
            "overall_status": "ok",
            "stage_results": [{"stage": "ingest", "status": "completed", "details": {"status": "ok"}}],
            "workspace_paths": None,
            "neon_mirror_sqlite_path": run_model_pipeline_module.DATA_DB,
            "neon_mirror_cache_path": run_model_pipeline_module.CACHE_DB,
        }

    monkeypatch.setattr(run_model_pipeline_module.stage_execution, "run_selected_stages", _run_selected_stages)

    out = run_model_pipeline_module.run_model_pipeline(profile="source-daily")

    assert out["status"] == "ok"
    assert captured["as_of_dates"]
    assert all(value == "2026-03-14" for value in captured["as_of_dates"])


def test_run_stage_serving_refresh_uses_local_source_archive_for_local_publish_profiles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        run_model_pipeline_module.runtime_support,
        "serving_refresh_skip_risk_engine",
        lambda **kwargs: (True, "risk_cache_current"),
    )
    monkeypatch.setattr(
        run_model_pipeline_module.core_reads.config,
        "neon_surface_enabled",
        lambda surface: surface == "core_reads",
    )
    monkeypatch.setattr(run_model_pipeline_module.core_reads, "neon_core_read_session", lambda: nullcontext())

    def _run_refresh(**kwargs):
        captured["backend"] = run_model_pipeline_module.core_reads.core_read_backend_name()
        captured["prefer_local_source_archive"] = kwargs.get("prefer_local_source_archive")
        return {"status": "ok"}

    monkeypatch.setattr(run_model_pipeline_module, "run_refresh", _run_refresh)

    out = run_model_pipeline_module._run_stage(
        profile="source-daily",
        stage="serving_refresh",
        as_of_date="2026-03-14",
        should_run_core=False,
        serving_mode="light",
        force_core=False,
        core_reason="within_interval",
        data_db=run_model_pipeline_module.DATA_DB,
        cache_db=run_model_pipeline_module.CACHE_DB,
        prefer_local_source_archive=True,
        refresh_scope=None,
    )

    assert out["status"] == "ok"
    assert captured["backend"] == "local"
    assert captured["prefer_local_source_archive"] is True


def test_run_stage_serving_refresh_keeps_neon_backend_for_canonical_serve_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        run_model_pipeline_module.runtime_support,
        "serving_refresh_skip_risk_engine",
        lambda **kwargs: (True, "risk_cache_current"),
    )
    monkeypatch.setattr(
        run_model_pipeline_module.core_reads.config,
        "neon_surface_enabled",
        lambda surface: surface == "core_reads",
    )
    monkeypatch.setattr(run_model_pipeline_module.core_reads, "neon_core_read_session", lambda: nullcontext())

    def _run_refresh(**kwargs):
        captured["backend"] = run_model_pipeline_module.core_reads.core_read_backend_name()
        captured["prefer_local_source_archive"] = kwargs.get("prefer_local_source_archive")
        return {"status": "ok"}

    monkeypatch.setattr(run_model_pipeline_module, "run_refresh", _run_refresh)

    out = run_model_pipeline_module._run_stage(
        profile="serve-refresh",
        stage="serving_refresh",
        as_of_date="2026-03-14",
        should_run_core=False,
        serving_mode="light",
        force_core=False,
        core_reason="within_interval",
        data_db=run_model_pipeline_module.DATA_DB,
        cache_db=run_model_pipeline_module.CACHE_DB,
        prefer_local_source_archive=False,
        refresh_scope=None,
    )

    assert out["status"] == "ok"
    assert captured["backend"] == "neon"
    assert captured["prefer_local_source_archive"] is False


def test_run_stage_serving_refresh_uses_local_backend_during_core_rebuild(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        run_model_pipeline_module.runtime_support,
        "serving_refresh_skip_risk_engine",
        lambda **kwargs: (False, "core_due"),
    )
    monkeypatch.setattr(
        run_model_pipeline_module.core_reads.config,
        "neon_surface_enabled",
        lambda surface: surface == "core_reads",
    )

    def _run_refresh(**kwargs):
        captured["backend"] = run_model_pipeline_module.core_reads.core_read_backend_name()
        return {"status": "ok"}

    monkeypatch.setattr(run_model_pipeline_module, "run_refresh", _run_refresh)

    out = run_model_pipeline_module._run_stage(
        profile="cold-core",
        stage="serving_refresh",
        as_of_date="2026-03-14",
        should_run_core=True,
        serving_mode="full",
        force_core=False,
        core_reason="method_version_change",
        data_db=run_model_pipeline_module.DATA_DB,
        cache_db=run_model_pipeline_module.CACHE_DB,
        prefer_local_source_archive=False,
        refresh_scope=None,
    )

    assert out["status"] == "ok"
    assert captured["backend"] == "local"


def test_run_stage_serving_refresh_passes_workspace_paths_without_mutating_core_reads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    new_data_db = tmp_path / "workspace.db"
    new_cache_db = tmp_path / "workspace_cache.db"
    new_data_db.touch()
    new_cache_db.touch()
    original = run_model_pipeline_module.core_reads.DATA_DB
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        run_model_pipeline_module.runtime_support,
        "serving_refresh_skip_risk_engine",
        lambda **kwargs: (True, "risk_cache_current"),
    )
    monkeypatch.setattr(run_model_pipeline_module.core_reads, "core_read_backend", lambda backend: nullcontext())
    monkeypatch.setattr(run_model_pipeline_module.core_reads, "neon_core_read_session", lambda: nullcontext())

    def _run_refresh(**kwargs):
        captured.update(kwargs)
        captured["core_reads_data_db"] = run_model_pipeline_module.core_reads.DATA_DB
        return {"status": "ok"}

    monkeypatch.setattr(run_model_pipeline_module, "run_refresh", _run_refresh)

    out = run_model_pipeline_module._run_stage(
        profile="cold-core",
        stage="serving_refresh",
        as_of_date="2026-03-14",
        should_run_core=True,
        serving_mode="full",
        force_core=False,
        core_reason="method_version_change",
        data_db=new_data_db,
        cache_db=new_cache_db,
        prefer_local_source_archive=False,
        refresh_scope=None,
    )

    assert out["status"] == "ok"
    assert captured["data_db"] == new_data_db
    assert captured["cache_db"] == new_cache_db
    assert captured["core_reads_data_db"] == original
    assert run_model_pipeline_module.core_reads.DATA_DB == original


def test_run_stage_serving_refresh_keeps_neon_backend_when_workspace_paths_are_present_but_core_is_not_running(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace_data_db = tmp_path / "workspace.db"
    workspace_cache_db = tmp_path / "workspace_cache.db"
    workspace_data_db.touch()
    workspace_cache_db.touch()
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        run_model_pipeline_module.runtime_support,
        "serving_refresh_skip_risk_engine",
        lambda **kwargs: (True, "risk_cache_current"),
    )
    monkeypatch.setattr(
        run_model_pipeline_module.core_reads.config,
        "neon_surface_enabled",
        lambda surface: surface == "core_reads",
    )
    monkeypatch.setattr(run_model_pipeline_module.core_reads, "neon_core_read_session", lambda: nullcontext())

    def _run_refresh(**kwargs):
        captured["backend"] = run_model_pipeline_module.core_reads.core_read_backend_name()
        captured["uses_workspace_paths"] = kwargs.get("uses_workspace_paths")
        captured["data_db"] = kwargs.get("data_db")
        captured["cache_db"] = kwargs.get("cache_db")
        return {"status": "ok"}

    monkeypatch.setattr(run_model_pipeline_module, "run_refresh", _run_refresh)

    out = run_model_pipeline_module._run_stage(
        profile="serve-refresh",
        stage="serving_refresh",
        as_of_date="2026-03-14",
        should_run_core=False,
        serving_mode="light",
        force_core=False,
        core_reason="risk_cache_current",
        data_db=workspace_data_db,
        cache_db=workspace_cache_db,
        prefer_local_source_archive=False,
        refresh_scope=None,
    )

    assert out["status"] == "ok"
    assert captured["backend"] == "neon"
    assert captured["uses_workspace_paths"] is True
    assert captured["data_db"] == workspace_data_db
    assert captured["cache_db"] == workspace_cache_db


def test_run_model_pipeline_reports_stage_runtime_details(monkeypatch: pytest.MonkeyPatch) -> None:
    finished: list[dict[str, object]] = []

    monkeypatch.setattr(run_model_pipeline_module.job_runs, "ensure_schema", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "fail_stale_running_stages", lambda *args, **kwargs: 0)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "completed_stages", lambda *args, **kwargs: set())
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "begin_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        run_model_pipeline_module.job_runs,
        "finish_stage",
        lambda **kwargs: finished.append(kwargs),
    )
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "run_rows", lambda *args, **kwargs: [])
    monkeypatch.setattr(run_model_pipeline_module.sqlite, "cache_get", lambda key: {})
    monkeypatch.setattr(run_model_pipeline_module.sqlite, "cache_get_live_first", lambda key: {})
    monkeypatch.setattr(
        run_model_pipeline_module,
        "_run_stage",
        lambda **kwargs: {"status": "ok", "stage": kwargs.get("stage")},
    )
    monkeypatch.setattr(
        run_model_pipeline_module,
        "mark_refresh_finished",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(run_model_pipeline_module.config, "NEON_AUTO_SYNC_ENABLED", False)

    out = run_model_pipeline_module.run_model_pipeline(profile="serve-refresh")

    assert out["status"] == "ok"
    assert finished
    details = finished[0]["details"]
    assert isinstance(details, dict)
    assert "duration_seconds" in details
    assert details["stage_index"] == 1
