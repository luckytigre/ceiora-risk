from __future__ import annotations

import importlib
from pathlib import Path

run_model_pipeline = importlib.import_module("backend.orchestration.run_model_pipeline")


def _patch_lightweight_pipeline(monkeypatch) -> None:
    monkeypatch.setattr(
        run_model_pipeline.stage_planning,
        "resolved_as_of_date",
        lambda _value, **_kwargs: "2026-03-04",
    )
    monkeypatch.setattr(run_model_pipeline.runtime_support, "risk_recompute_due", lambda *_args, **_kwargs: (False, "within_interval"))
    monkeypatch.setattr(
        run_model_pipeline.stage_execution,
        "run_selected_stages",
        lambda **_kwargs: {
            "overall_status": "ok",
            "stage_results": [{"stage": "feature_build", "status": "completed", "details": {"status": "ok"}}],
            "workspace_paths": None,
            "neon_mirror_sqlite_path": run_model_pipeline.DATA_DB,
            "neon_mirror_cache_path": run_model_pipeline.CACHE_DB,
        },
    )
    monkeypatch.setattr(run_model_pipeline.sqlite, "cache_get", lambda _k: {})
    monkeypatch.setattr(run_model_pipeline.sqlite, "cache_set", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(run_model_pipeline.job_runs, "ensure_schema", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(run_model_pipeline.job_runs, "fail_stale_running_stages", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(run_model_pipeline.job_runs, "completed_stages", lambda *_args, **_kwargs: set())
    monkeypatch.setattr(run_model_pipeline.job_runs, "begin_stage", lambda **_kwargs: None)
    monkeypatch.setattr(run_model_pipeline.job_runs, "finish_stage", lambda **_kwargs: None)
    monkeypatch.setattr(run_model_pipeline.job_runs, "run_rows", lambda **_kwargs: [])
    monkeypatch.setattr(
        run_model_pipeline.post_run_publish,
        "write_neon_mirror_artifact",
        lambda **_kwargs: "/tmp/neon_mirror_report.json",
    )


def test_run_model_pipeline_runs_optional_neon_mirror_on_source_profiles(monkeypatch) -> None:
    _patch_lightweight_pipeline(monkeypatch)
    health_cache: dict[str, object] = {}
    monkeypatch.setattr(run_model_pipeline.sqlite, "cache_set", lambda k, v: health_cache.__setitem__(k, v))
    monkeypatch.setattr(run_model_pipeline.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(run_model_pipeline.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_SYNC_ENABLED", True)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_SYNC_REQUIRED", False)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_PARITY_ENABLED", True)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_PRUNE_ENABLED", True)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_SYNC_MODE", "incremental")
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_SYNC_TABLES", [])
    monkeypatch.setattr(run_model_pipeline.config, "NEON_SOURCE_RETENTION_YEARS", 10)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_ANALYTICS_RETENTION_YEARS", 5)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_DATABASE_URL", "postgresql://example")
    monkeypatch.setattr(
        run_model_pipeline,
        "run_neon_mirror_cycle",
        lambda **_kwargs: {"status": "ok", "sync": {"status": "ok"}},
    )

    out = run_model_pipeline.run_model_pipeline(profile="source-daily")

    assert out["status"] == "ok"
    assert out["neon_mirror"]["status"] == "ok"
    assert out["neon_mirror"]["artifact_path"] == "/tmp/neon_mirror_report.json"
    assert "neon_sync_health" in health_cache
    payload = health_cache["neon_sync_health"]
    assert isinstance(payload, dict)
    assert payload.get("status") == "ok"


def test_run_model_pipeline_fails_if_required_neon_mirror_mismatch(monkeypatch) -> None:
    _patch_lightweight_pipeline(monkeypatch)
    health_cache: dict[str, object] = {}
    monkeypatch.setattr(run_model_pipeline.sqlite, "cache_set", lambda k, v: health_cache.__setitem__(k, v))
    monkeypatch.setattr(run_model_pipeline.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(run_model_pipeline.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_SYNC_ENABLED", True)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_SYNC_REQUIRED", True)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_PARITY_ENABLED", True)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_PRUNE_ENABLED", True)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_SYNC_MODE", "incremental")
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_SYNC_TABLES", [])
    monkeypatch.setattr(run_model_pipeline.config, "NEON_SOURCE_RETENTION_YEARS", 10)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_ANALYTICS_RETENTION_YEARS", 5)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_DATABASE_URL", "postgresql://example")
    monkeypatch.setattr(
        run_model_pipeline,
        "run_neon_mirror_cycle",
        lambda **_kwargs: {"status": "mismatch"},
    )

    out = run_model_pipeline.run_model_pipeline(profile="source-daily")

    assert out["neon_mirror"]["status"] == "mismatch"
    assert out["status"] == "failed"
    assert out["neon_mirror"]["artifact_path"] == "/tmp/neon_mirror_report.json"
    assert "neon_sync_health" in health_cache
    payload = health_cache["neon_sync_health"]
    assert isinstance(payload, dict)
    assert payload.get("status") == "error"


def test_run_model_pipeline_skips_broad_neon_mirror_for_serve_refresh(monkeypatch) -> None:
    _patch_lightweight_pipeline(monkeypatch)
    health_cache: dict[str, object] = {}
    monkeypatch.setattr(run_model_pipeline.sqlite, "cache_set", lambda k, v: health_cache.__setitem__(k, v))
    monkeypatch.setattr(run_model_pipeline.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(run_model_pipeline.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_SYNC_ENABLED", True)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_PARITY_ENABLED", True)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_PRUNE_ENABLED", True)
    monkeypatch.setattr(
        run_model_pipeline,
        "run_neon_mirror_cycle",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("serve-refresh should not run broad neon mirror")),
    )

    out = run_model_pipeline.run_model_pipeline(profile="serve-refresh")

    assert out["status"] == "ok"
    assert out["neon_mirror"]["status"] == "skipped"
    assert out["neon_mirror"]["reason"] == "profile_skips_broad_neon_mirror"
    assert "neon_sync_health" not in health_cache


def test_run_model_pipeline_still_runs_required_source_sync_when_neon_primary_even_if_auto_sync_flag_is_off(monkeypatch) -> None:
    _patch_lightweight_pipeline(monkeypatch)
    health_cache: dict[str, object] = {}
    monkeypatch.setattr(run_model_pipeline.sqlite, "cache_set", lambda k, v: health_cache.__setitem__(k, v))
    monkeypatch.setattr(run_model_pipeline.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(run_model_pipeline.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_SYNC_ENABLED", False)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_PARITY_ENABLED", True)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_PRUNE_ENABLED", True)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_DATABASE_URL", "postgresql://example")
    monkeypatch.setattr(
        run_model_pipeline,
        "run_neon_mirror_cycle",
        lambda **_kwargs: {"status": "ok", "sync": {"status": "ok"}},
    )

    out = run_model_pipeline.run_model_pipeline(profile="source-daily")

    assert out["status"] == "ok"
    assert out["neon_mirror"]["status"] == "ok"
    assert out["neon_mirror"]["artifact_path"] == "/tmp/neon_mirror_report.json"
    payload = health_cache["neon_sync_health"]
    assert isinstance(payload, dict)
    assert payload.get("status") == "ok"


def test_run_model_pipeline_publishes_health_when_required_serving_write_fails(monkeypatch) -> None:
    _patch_lightweight_pipeline(monkeypatch)
    health_cache: dict[str, object] = {}
    monkeypatch.setattr(run_model_pipeline.sqlite, "cache_set", lambda k, v: health_cache.__setitem__(k, v))
    monkeypatch.setattr(run_model_pipeline.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(run_model_pipeline.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_SYNC_ENABLED", True)
    monkeypatch.setattr(
        run_model_pipeline.stage_execution,
        "run_selected_stages",
        lambda **_kwargs: {
            "overall_status": "failed",
            "stage_results": [
                {
                    "stage": "serving_refresh",
                    "status": "failed",
                    "details": {},
                    "error": {
                        "type": "RuntimeError",
                        "message": "Serving payload persistence failed: RuntimeError: Serving payload Neon write failed: {'status': 'error'}",
                    },
                }
            ],
            "workspace_paths": None,
            "neon_mirror_sqlite_path": run_model_pipeline.DATA_DB,
            "neon_mirror_cache_path": run_model_pipeline.CACHE_DB,
        },
    )

    out = run_model_pipeline.run_model_pipeline(profile="serve-refresh")

    assert out["status"] == "failed"
    payload = health_cache["neon_sync_health"]
    assert isinstance(payload, dict)
    assert payload.get("status") == "error"
    assert payload.get("sync_status") == "serving_payload_write_failed"
    assert payload.get("health_scope") == "serving_payload_write"


def test_run_model_pipeline_reports_workspace_and_local_mirror_sync_for_neon_core_lane(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _patch_lightweight_pipeline(monkeypatch)
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()
    workspace_data_db = workspace_root / "data.db"
    workspace_cache_db = workspace_root / "cache.db"
    workspace_data_db.touch()
    workspace_cache_db.touch()
    captured: dict[str, object] = {}
    prune_calls: list[dict[str, object]] = []

    monkeypatch.setattr(run_model_pipeline.sqlite, "cache_get_live_first", lambda _k: {})
    monkeypatch.setattr(run_model_pipeline.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(run_model_pipeline.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTHORITATIVE_REBUILDS", True)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_SYNC_ENABLED", False)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_SYNC_REQUIRED", False)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_DATABASE_URL", "")
    monkeypatch.setattr(run_model_pipeline, "mark_refresh_finished", lambda **kwargs: None)
    monkeypatch.setattr(run_model_pipeline.runtime_support, "profile_runs_broad_neon_mirror", lambda profile: False)
    monkeypatch.setattr(
        run_model_pipeline.stage_execution,
        "run_selected_stages",
        lambda **_kwargs: {
            "overall_status": "ok",
            "stage_results": [
                {"stage": "source_sync", "status": "completed", "details": {"status": "ok"}},
                {"stage": "neon_readiness", "status": "completed", "details": {"status": "ok"}},
                {"stage": "factor_returns", "status": "completed", "details": {"status": "ok"}},
                {"stage": "risk_model", "status": "completed", "details": {"status": "ok"}},
                {"stage": "serving_refresh", "status": "completed", "details": {"status": "ok"}},
            ],
            "workspace_paths": run_model_pipeline.neon_authority.WorkspacePaths(
                root_dir=workspace_root.resolve(),
                data_db=workspace_data_db.resolve(),
                cache_db=workspace_cache_db.resolve(),
            ),
            "neon_mirror_sqlite_path": workspace_data_db.resolve(),
            "neon_mirror_cache_path": workspace_cache_db.resolve(),
        },
    )
    monkeypatch.setattr(
        run_model_pipeline.neon_authority,
        "sync_workspace_derivatives_to_local_mirror",
        lambda **kwargs: captured.update(kwargs) or {"status": "ok", "copied_tables": 3},
    )
    monkeypatch.setattr(
        run_model_pipeline.neon_authority,
        "prune_rebuild_workspaces",
        lambda **kwargs: prune_calls.append(dict(kwargs)) or {"status": "ok", "removed": []},
    )

    out = run_model_pipeline.run_model_pipeline(profile="core-weekly", force_core=True)

    assert out["status"] == "ok"
    assert out["workspace"] == {
        "root_dir": str(workspace_root.resolve()),
        "data_db": str(workspace_data_db.resolve()),
        "cache_db": str(workspace_cache_db.resolve()),
    }
    assert out["local_mirror_sync"]["status"] == "ok"
    assert captured["workspace_data_db"] == workspace_data_db.resolve()
    assert captured["workspace_cache_db"] == workspace_cache_db.resolve()
    assert captured["local_data_db"] == run_model_pipeline.DATA_DB
    assert captured["local_cache_db"] == run_model_pipeline.CACHE_DB
    assert out["workspace_prune"]["status"] == "ok"
    assert prune_calls[0]["workspaces_root"] == workspace_root.resolve().parent
    assert prune_calls[0]["preserve"] == workspace_root.resolve()


def test_run_model_pipeline_fails_closed_when_workspace_local_mirror_sync_fails(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _patch_lightweight_pipeline(monkeypatch)
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()
    workspace_data_db = workspace_root / "data.db"
    workspace_cache_db = workspace_root / "cache.db"
    workspace_data_db.touch()
    workspace_cache_db.touch()

    monkeypatch.setattr(run_model_pipeline.sqlite, "cache_get_live_first", lambda _k: {})
    monkeypatch.setattr(run_model_pipeline.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(run_model_pipeline.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTHORITATIVE_REBUILDS", True)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_SYNC_ENABLED", False)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTO_SYNC_REQUIRED", False)
    monkeypatch.setattr(run_model_pipeline.config, "NEON_DATABASE_URL", "")
    monkeypatch.setattr(run_model_pipeline, "mark_refresh_finished", lambda **kwargs: None)
    monkeypatch.setattr(run_model_pipeline.runtime_support, "profile_runs_broad_neon_mirror", lambda profile: False)
    monkeypatch.setattr(
        run_model_pipeline.stage_execution,
        "run_selected_stages",
        lambda **_kwargs: {
            "overall_status": "ok",
            "stage_results": [
                {"stage": "neon_readiness", "status": "completed", "details": {"status": "ok"}},
                {"stage": "factor_returns", "status": "completed", "details": {"status": "ok"}},
                {"stage": "risk_model", "status": "completed", "details": {"status": "ok"}},
                {"stage": "serving_refresh", "status": "completed", "details": {"status": "ok"}},
            ],
            "workspace_paths": run_model_pipeline.neon_authority.WorkspacePaths(
                root_dir=workspace_root.resolve(),
                data_db=workspace_data_db.resolve(),
                cache_db=workspace_cache_db.resolve(),
            ),
            "neon_mirror_sqlite_path": workspace_data_db.resolve(),
            "neon_mirror_cache_path": workspace_cache_db.resolve(),
        },
    )
    monkeypatch.setattr(
        run_model_pipeline.neon_authority,
        "sync_workspace_derivatives_to_local_mirror",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("mirror sync exploded")),
    )

    out = run_model_pipeline.run_model_pipeline(profile="core-weekly", force_core=True)

    assert out["status"] == "failed"
    assert out["workspace"] == {
        "root_dir": str(workspace_root.resolve()),
        "data_db": str(workspace_data_db.resolve()),
        "cache_db": str(workspace_cache_db.resolve()),
    }
    assert out["local_mirror_sync"]["status"] == "error"
    assert out["local_mirror_sync"]["error"]["message"] == "mirror sync exploded"
