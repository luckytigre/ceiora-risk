from __future__ import annotations

import importlib

run_model_pipeline = importlib.import_module("backend.orchestration.run_model_pipeline")


def _patch_lightweight_pipeline(monkeypatch) -> None:
    monkeypatch.setattr(run_model_pipeline, "_resolved_as_of_date", lambda _: "2026-03-04")
    monkeypatch.setattr(run_model_pipeline, "_risk_recompute_due", lambda *_args, **_kwargs: (False, "within_interval"))
    monkeypatch.setattr(run_model_pipeline, "_stage_window", lambda *_args, **_kwargs: ["feature_build"])
    monkeypatch.setattr(run_model_pipeline, "_run_stage", lambda **_kwargs: {"status": "ok"})
    monkeypatch.setattr(run_model_pipeline.sqlite, "cache_get", lambda _k: {})
    monkeypatch.setattr(run_model_pipeline.sqlite, "cache_set", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(run_model_pipeline.job_runs, "ensure_schema", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(run_model_pipeline.job_runs, "completed_stages", lambda *_args, **_kwargs: set())
    monkeypatch.setattr(run_model_pipeline.job_runs, "begin_stage", lambda **_kwargs: None)
    monkeypatch.setattr(run_model_pipeline.job_runs, "finish_stage", lambda **_kwargs: None)
    monkeypatch.setattr(run_model_pipeline.job_runs, "run_rows", lambda **_kwargs: [])
    monkeypatch.setattr(
        run_model_pipeline,
        "_write_neon_mirror_artifact",
        lambda **_kwargs: "/tmp/neon_mirror_report.json",
    )


def test_run_model_pipeline_runs_optional_neon_mirror(monkeypatch) -> None:
    _patch_lightweight_pipeline(monkeypatch)
    health_cache: dict[str, object] = {}
    monkeypatch.setattr(run_model_pipeline.sqlite, "cache_set", lambda k, v: health_cache.__setitem__(k, v))
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

    out = run_model_pipeline.run_model_pipeline(profile="serve-refresh")

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

    out = run_model_pipeline.run_model_pipeline(profile="serve-refresh")

    assert out["neon_mirror"]["status"] == "mismatch"
    assert out["status"] == "failed"
    assert out["neon_mirror"]["artifact_path"] == "/tmp/neon_mirror_report.json"
    assert "neon_sync_health" in health_cache
    payload = health_cache["neon_sync_health"]
    assert isinstance(payload, dict)
    assert payload.get("status") == "error"
