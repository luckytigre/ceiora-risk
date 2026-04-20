from __future__ import annotations

import importlib
from pathlib import Path
from types import SimpleNamespace

stage_execution = importlib.import_module("backend.orchestration.stage_execution")
from backend.data import job_runs
from backend.services import neon_authority


def _job_runs_stub() -> SimpleNamespace:
    return SimpleNamespace(
        begin_stage=lambda **kwargs: None,
        heartbeat_stage=lambda **kwargs: None,
        finish_stage=lambda **kwargs: None,
        normalize_stage_metrics=job_runs.normalize_stage_metrics,
    )


def test_run_selected_stages_routes_neon_workspace_into_core_and_serving_refresh(tmp_path: Path) -> None:
    local_data_db = tmp_path / "local_data.db"
    local_cache_db = tmp_path / "local_cache.db"
    local_data_db.touch()
    local_cache_db.touch()

    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()
    workspace_data_db = workspace_root / "data.db"
    workspace_cache_db = workspace_root / "cache.db"
    workspace_data_db.touch()
    workspace_cache_db.touch()

    captured: list[dict[str, object]] = []

    def _run_stage(**kwargs):
        captured.append(
            {
                "run_id": kwargs["run_id"],
                "stage": kwargs["stage"],
                "data_db": kwargs["data_db"],
                "cache_db": kwargs["cache_db"],
                "workspace_root": kwargs.get("workspace_root"),
                "upstream_core_recomputed": kwargs.get("upstream_core_recomputed"),
            }
        )
        if kwargs["stage"] == "neon_readiness":
            return {
                "status": "ok",
                "workspace": {
                    "root_dir": str(workspace_root),
                    "data_db": str(workspace_data_db),
                    "cache_db": str(workspace_cache_db),
                },
            }
        return {"status": "ok"}

    out = stage_execution.run_selected_stages(
        selected=["source_sync", "neon_readiness", "factor_returns", "risk_model", "serving_refresh"],
        stages=["source_sync", "neon_readiness", "factor_returns", "risk_model", "serving_refresh"],
        db_path=local_data_db,
        cache_db=local_cache_db,
        profile_key="core-weekly",
        effective_run_id="run_1",
        as_of="2026-03-14",
        should_run_core=True,
        serving_mode="full",
        force_core=False,
        core_reason="due",
        raw_history_policy="none",
        reset_core_cache=False,
        enable_ingest=False,
        prefer_local_source_archive=False,
        refresh_scope=None,
        rebuild_backend="neon",
        app_data_dir=str(tmp_path),
        completed_stages=set(),
        stage_callback=None,
        run_stage_fn=_run_stage,
        job_runs_module=_job_runs_stub(),
        neon_authority_module=neon_authority,
    )

    assert out["overall_status"] == "ok"
    assert out["workspace_paths"] == neon_authority.WorkspacePaths(
        root_dir=workspace_root.resolve(),
        data_db=workspace_data_db.resolve(),
        cache_db=workspace_cache_db.resolve(),
    )
    assert out["neon_mirror_sqlite_path"] == workspace_data_db.resolve()
    assert out["neon_mirror_cache_path"] == workspace_cache_db.resolve()

    by_stage = {str(item["stage"]): item for item in captured}
    assert all(item["run_id"] == "run_1" for item in captured)
    assert by_stage["source_sync"]["data_db"] == local_data_db
    assert by_stage["source_sync"]["cache_db"] == local_cache_db
    assert by_stage["source_sync"]["upstream_core_recomputed"] is False
    assert by_stage["neon_readiness"]["workspace_root"] == tmp_path / "neon_rebuild_workspace" / "run_1"
    assert by_stage["neon_readiness"]["upstream_core_recomputed"] is False
    assert by_stage["factor_returns"]["data_db"] == workspace_data_db.resolve()
    assert by_stage["factor_returns"]["cache_db"] == workspace_cache_db.resolve()
    assert by_stage["factor_returns"]["upstream_core_recomputed"] is False
    assert by_stage["risk_model"]["data_db"] == workspace_data_db.resolve()
    assert by_stage["risk_model"]["cache_db"] == workspace_cache_db.resolve()
    assert by_stage["risk_model"]["upstream_core_recomputed"] is False
    assert by_stage["serving_refresh"]["data_db"] == workspace_data_db.resolve()
    assert by_stage["serving_refresh"]["cache_db"] == workspace_cache_db.resolve()
    assert by_stage["serving_refresh"]["upstream_core_recomputed"] is True


def test_run_selected_stages_fails_closed_when_neon_readiness_workspace_is_malformed(tmp_path: Path) -> None:
    local_data_db = tmp_path / "local_data.db"
    local_cache_db = tmp_path / "local_cache.db"
    local_data_db.touch()
    local_cache_db.touch()

    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()
    (workspace_root / "cache.db").touch()

    def _run_stage(**kwargs):
        if kwargs["stage"] == "neon_readiness":
            return {
                "status": "ok",
                "workspace": {
                    "root_dir": str(workspace_root),
                    "data_db": "",
                    "cache_db": str(workspace_root / "cache.db"),
                },
            }
        return {"status": "ok"}

    out = stage_execution.run_selected_stages(
        selected=["neon_readiness", "factor_returns"],
        stages=["neon_readiness", "factor_returns"],
        db_path=local_data_db,
        cache_db=local_cache_db,
        profile_key="core-weekly",
        effective_run_id="run_2",
        as_of="2026-03-14",
        should_run_core=True,
        serving_mode="full",
        force_core=False,
        core_reason="due",
        raw_history_policy="none",
        reset_core_cache=False,
        enable_ingest=False,
        prefer_local_source_archive=False,
        refresh_scope=None,
        rebuild_backend="neon",
        app_data_dir=str(tmp_path),
        completed_stages=set(),
        stage_callback=None,
        run_stage_fn=_run_stage,
        job_runs_module=_job_runs_stub(),
        neon_authority_module=neon_authority,
    )

    assert out["overall_status"] == "failed"
    assert out["workspace_paths"] is None
    assert out["stage_results"][0]["stage"] == "neon_readiness"
    assert out["stage_results"][0]["status"] == "failed"
    assert "workspace.data_db" in out["stage_results"][0]["error"]["message"]


def test_run_selected_stages_does_not_mark_upstream_core_recomputed_when_serving_refresh_runs_alone(
    tmp_path: Path,
) -> None:
    local_data_db = tmp_path / "local_data.db"
    local_cache_db = tmp_path / "local_cache.db"
    local_data_db.touch()
    local_cache_db.touch()

    captured: list[dict[str, object]] = []

    def _run_stage(**kwargs):
        captured.append(
            {
                "stage": kwargs["stage"],
                "upstream_core_recomputed": kwargs.get("upstream_core_recomputed"),
                "data_db": kwargs["data_db"],
                "cache_db": kwargs["cache_db"],
            }
        )
        return {"status": "ok"}

    out = stage_execution.run_selected_stages(
        selected=["serving_refresh"],
        stages=["source_sync", "factor_returns", "risk_model", "serving_refresh"],
        db_path=local_data_db,
        cache_db=local_cache_db,
        profile_key="core-weekly",
        effective_run_id="run_3",
        as_of="2026-03-14",
        should_run_core=True,
        serving_mode="full",
        force_core=False,
        core_reason="due",
        raw_history_policy="none",
        reset_core_cache=False,
        enable_ingest=False,
        prefer_local_source_archive=False,
        refresh_scope=None,
        rebuild_backend="neon",
        app_data_dir=str(tmp_path),
        completed_stages=set(),
        stage_callback=None,
        run_stage_fn=_run_stage,
        job_runs_module=_job_runs_stub(),
        neon_authority_module=neon_authority,
    )

    assert out["overall_status"] == "ok"
    assert len(captured) == 1
    assert captured[0]["stage"] == "serving_refresh"
    assert captured[0]["upstream_core_recomputed"] is False
    assert captured[0]["data_db"] == local_data_db
    assert captured[0]["cache_db"] == local_cache_db


def test_run_selected_stages_does_not_treat_resumed_risk_model_as_current_invocation_recompute(
    tmp_path: Path,
) -> None:
    local_data_db = tmp_path / "local_data.db"
    local_cache_db = tmp_path / "local_cache.db"
    local_data_db.touch()
    local_cache_db.touch()

    captured: list[dict[str, object]] = []

    def _run_stage(**kwargs):
        captured.append(
            {
                "stage": kwargs["stage"],
                "upstream_core_recomputed": kwargs.get("upstream_core_recomputed"),
            }
        )
        return {"status": "ok"}

    out = stage_execution.run_selected_stages(
        selected=["risk_model", "serving_refresh"],
        stages=["factor_returns", "risk_model", "serving_refresh"],
        db_path=local_data_db,
        cache_db=local_cache_db,
        profile_key="core-weekly",
        effective_run_id="run_4",
        as_of="2026-03-14",
        should_run_core=True,
        serving_mode="full",
        force_core=False,
        core_reason="due",
        raw_history_policy="none",
        reset_core_cache=False,
        enable_ingest=False,
        prefer_local_source_archive=False,
        refresh_scope=None,
        rebuild_backend="neon",
        app_data_dir=str(tmp_path),
        completed_stages={"risk_model"},
        stage_callback=None,
        run_stage_fn=_run_stage,
        job_runs_module=_job_runs_stub(),
        neon_authority_module=neon_authority,
    )

    assert out["overall_status"] == "ok"
    assert len(captured) == 1
    assert captured[0]["stage"] == "serving_refresh"
    assert captured[0]["upstream_core_recomputed"] is False


def test_run_selected_stages_persists_normalized_metrics(tmp_path: Path) -> None:
    local_data_db = tmp_path / "local_data.db"
    local_cache_db = tmp_path / "local_cache.db"
    local_data_db.touch()
    local_cache_db.touch()

    finished: list[dict[str, object]] = []

    def _finish_stage(**kwargs):
        finished.append(kwargs)

    job_runs_stub = SimpleNamespace(
        begin_stage=lambda **kwargs: None,
        heartbeat_stage=lambda **kwargs: None,
        finish_stage=_finish_stage,
        normalize_stage_metrics=job_runs.normalize_stage_metrics,
    )

    def _run_stage(**kwargs):
        return {
            "status": "ok",
            "rows_upserted": 123,
            "dates_processed": 7,
            "model_outputs_write": {
                "row_counts": {
                    "model_factor_returns_daily": 7,
                    "model_specific_risk_daily": 7,
                }
            },
            "items_processed": 7,
            "items_total": 7,
            "progress_pct": 100.0,
            "unit": "dates",
        }

    out = stage_execution.run_selected_stages(
        selected=["factor_returns"],
        stages=["factor_returns"],
        db_path=local_data_db,
        cache_db=local_cache_db,
        profile_key="core-weekly",
        effective_run_id="run_metrics",
        as_of="2026-03-14",
        should_run_core=True,
        serving_mode="full",
        force_core=False,
        core_reason="due",
        raw_history_policy="none",
        reset_core_cache=False,
        enable_ingest=False,
        prefer_local_source_archive=False,
        refresh_scope=None,
        rebuild_backend="sqlite",
        app_data_dir=str(tmp_path),
        completed_stages=set(),
        stage_callback=None,
        run_stage_fn=_run_stage,
        job_runs_module=job_runs_stub,
        neon_authority_module=neon_authority,
    )

    assert out["overall_status"] == "ok"
    assert len(finished) == 1
    details = finished[0]["details"]
    metrics = details["metrics"]
    assert metrics["duration_seconds"] >= 0.0
    assert metrics["row_counts"]["rows_upserted"] == 123
    assert metrics["row_counts"]["model_outputs_write.model_factor_returns_daily"] == 7
    assert metrics["row_counts"]["model_outputs_write.model_specific_risk_daily"] == 7
    assert metrics["counters"]["dates_processed"] == 7
    assert metrics["progress"] == {
        "items_processed": 7,
        "items_total": 7,
        "progress_pct": 100.0,
        "unit": "dates",
    }
