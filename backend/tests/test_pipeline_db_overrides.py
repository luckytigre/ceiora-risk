from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.orchestration import run_model_pipeline, stage_core


def test_run_core_stage_passes_as_of_date_to_raw_history_end_date(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"
    sqlite3.connect(str(data_db)).close()
    sqlite3.connect(str(cache_db)).close()
    captured: dict[str, object] = {}

    def _rebuild_raw_cross_section_history(
        db_path: Path,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        frequency: str = "weekly",
        progress_callback=None,
    ) -> dict[str, object]:
        captured["db_path"] = db_path
        captured["start_date"] = start_date
        captured["end_date"] = end_date
        captured["frequency"] = frequency
        return {"status": "ok", "rows_upserted": 1, "table": "barra_raw_cross_section_history"}

    out = stage_core.run_core_stage(
        profile="cold-core",
        run_id="job_test",
        stage="raw_history",
        as_of_date="2026-03-26",
        should_run_core=True,
        force_core=True,
        core_reason="force_core",
        data_db=data_db,
        cache_db=cache_db,
        raw_history_policy="full-daily",
        reset_core_cache=False,
        progress_callback=None,
        config_module=object(),
        core_reads_module=object(),
        sqlite_module=object(),
        persist_model_outputs_fn=lambda **kwargs: {},
        rebuild_raw_cross_section_history_fn=_rebuild_raw_cross_section_history,
        rebuild_cross_section_snapshot_fn=lambda *args, **kwargs: {},
        build_and_persist_estu_membership_fn=lambda **kwargs: {},
        reset_core_caches_fn=lambda _cache_db: {},
        compute_daily_factor_returns_fn=lambda *args, **kwargs: None,
        build_factor_covariance_from_cache_fn=lambda *args, **kwargs: None,
        build_specific_risk_from_cache_fn=lambda *args, **kwargs: None,
        latest_factor_return_date_fn=lambda *args, **kwargs: None,
        serialize_covariance_fn=lambda *args, **kwargs: {},
        previous_or_same_xnys_session_fn=lambda value: value,
        risk_engine_method_version="v1",
    )

    assert out["status"] == "ok"
    assert captured == {
        "db_path": data_db,
        "start_date": None,
        "end_date": "2026-03-26",
        "frequency": "daily",
    }


def test_run_core_stage_uses_recent_window_for_incremental_raw_history(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"
    sqlite3.connect(str(data_db)).close()
    sqlite3.connect(str(cache_db)).close()
    captured: dict[str, object] = {}

    def _rebuild_raw_cross_section_history(
        db_path: Path,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        frequency: str = "weekly",
        progress_callback=None,
    ) -> dict[str, object]:
        captured["db_path"] = db_path
        captured["start_date"] = start_date
        captured["end_date"] = end_date
        captured["frequency"] = frequency
        return {"status": "ok", "rows_upserted": 1, "table": "barra_raw_cross_section_history"}

    class _Config:
        RAW_HISTORY_RECENT_WINDOW_DAYS = 30

    out = stage_core.run_core_stage(
        profile="core-weekly",
        run_id="job_test",
        stage="raw_history",
        as_of_date="2026-03-26",
        should_run_core=True,
        force_core=False,
        core_reason="due",
        data_db=data_db,
        cache_db=cache_db,
        raw_history_policy="recent-daily",
        reset_core_cache=False,
        progress_callback=None,
        config_module=_Config(),
        core_reads_module=object(),
        sqlite_module=object(),
        persist_model_outputs_fn=lambda **kwargs: {},
        rebuild_raw_cross_section_history_fn=_rebuild_raw_cross_section_history,
        rebuild_cross_section_snapshot_fn=lambda *args, **kwargs: {},
        build_and_persist_estu_membership_fn=lambda **kwargs: {},
        reset_core_caches_fn=lambda _cache_db: {},
        compute_daily_factor_returns_fn=lambda *args, **kwargs: None,
        build_factor_covariance_from_cache_fn=lambda *args, **kwargs: None,
        build_specific_risk_from_cache_fn=lambda *args, **kwargs: None,
        latest_factor_return_date_fn=lambda *args, **kwargs: None,
        serialize_covariance_fn=lambda *args, **kwargs: {},
        previous_or_same_xnys_session_fn=lambda value: value,
        risk_engine_method_version="v1",
    )

    assert out["status"] == "ok"
    assert captured == {
        "db_path": data_db,
        "start_date": "2026-02-24",
        "end_date": "2026-03-26",
        "frequency": "daily",
    }


def test_run_model_pipeline_respects_data_db_and_cache_db_overrides(
    monkeypatch,
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "override_data.db"
    cache_db = tmp_path / "override_cache.db"
    sqlite3.connect(str(data_db)).close()
    sqlite3.connect(str(cache_db)).close()
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        run_model_pipeline,
        "planned_stages_for_profile",
        lambda **kwargs: (
            "publish-only",
            {
                "label": "Publish Only",
                "core_policy": "never",
                "serving_mode": "publish",
                "raw_history_policy": "none",
                "reset_core_cache": False,
            },
            [],
        ),
    )
    monkeypatch.setattr(run_model_pipeline.job_runs, "ensure_schema", lambda _db_path: None)
    monkeypatch.setattr(run_model_pipeline.job_runs, "fail_stale_running_stages", lambda **kwargs: None)
    monkeypatch.setattr(run_model_pipeline.job_runs, "run_rows", lambda **kwargs: [])
    monkeypatch.setattr(
        run_model_pipeline.runtime_support,
        "resolve_effective_risk_engine_meta",
        lambda **kwargs: ({}, None),
    )
    monkeypatch.setattr(
        run_model_pipeline.runtime_support,
        "risk_recompute_due",
        lambda *args, **kwargs: (False, "not_due"),
    )

    def _run_selected_stages(**kwargs):
        captured["db_path"] = kwargs["db_path"]
        captured["cache_db"] = kwargs["cache_db"]
        return {
            "stage_results": [],
            "overall_status": "ok",
            "workspace_paths": None,
            "neon_mirror_sqlite_path": data_db,
            "neon_mirror_cache_path": cache_db,
        }

    monkeypatch.setattr(run_model_pipeline.stage_execution, "run_selected_stages", _run_selected_stages)
    monkeypatch.setattr(
        run_model_pipeline.finalize_run,
        "finalize_pipeline_run",
        lambda **kwargs: {
            "overall_status": "ok",
            "neon_mirror": {"status": "skipped"},
            "local_mirror_sync": {"status": "skipped"},
            "workspace_prune": {"status": "skipped"},
        },
    )

    out = run_model_pipeline.run_model_pipeline(
        profile="publish-only",
        as_of_date="2026-03-26",
        data_db=data_db,
        cache_db=cache_db,
    )

    assert out["status"] == "ok"
    assert captured == {
        "db_path": data_db,
        "cache_db": cache_db,
    }
