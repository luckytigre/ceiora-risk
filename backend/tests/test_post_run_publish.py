from __future__ import annotations

import json
from pathlib import Path

from backend.orchestration import post_run_publish


def test_latest_neon_mirror_artifact_for_run_selects_newest(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(post_run_publish.config, "APP_DATA_DIR", str(tmp_path))
    reports = tmp_path / "audit_reports" / "neon_parity"
    reports.mkdir(parents=True)
    older = reports / "neon_mirror_20260322T170000Z_job_1.json"
    newer = reports / "neon_mirror_20260322T180000Z_job_1.json"
    older.write_text("{}", encoding="utf-8")
    newer.write_text("{}", encoding="utf-8")

    out = post_run_publish.latest_neon_mirror_artifact_for_run(run_id="job_1")

    assert out == newer


def test_repair_neon_sync_health_from_existing_workspace_republishes_clean_parity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(post_run_publish.config, "APP_DATA_DIR", str(tmp_path))
    reports = tmp_path / "audit_reports" / "neon_parity"
    reports.mkdir(parents=True)
    workspace = tmp_path / "neon_rebuild_workspace" / "job_1"
    workspace.mkdir(parents=True)
    (workspace / "data.db").write_text("", encoding="utf-8")
    (workspace / "cache.db").write_text("", encoding="utf-8")

    prior_artifact = reports / "neon_mirror_20260322T170000Z_job_1.json"
    prior_artifact.write_text(
        json.dumps(
            {
                "run_id": "job_1",
                "profile": "core-weekly",
                "as_of_date": "2026-03-20",
                "overall_status": "failed",
                "neon_mirror": {
                    "status": "mismatch",
                    "sync": {"status": "ok"},
                    "prune": {"status": "ok"},
                    "parity": {"status": "mismatch", "issues": ["mismatch:model_factor_returns_daily"]},
                },
            }
        ),
        encoding="utf-8",
    )

    captured: dict[str, object] = {}
    monkeypatch.setattr(
        post_run_publish.neon_mirror_service,
        "run_bounded_parity_audit",
        lambda **_kwargs: {"status": "ok", "issues": []},
    )
    monkeypatch.setattr(
        post_run_publish,
        "publish_neon_sync_health",
        lambda **kwargs: captured.update(kwargs),
    )

    out = post_run_publish.repair_neon_sync_health_from_existing_workspace(
        run_id="job_1",
        profile="core-weekly",
        as_of_date="2026-03-20",
        workspace_sqlite_path=workspace / "data.db",
        workspace_cache_path=workspace / "cache.db",
    )

    assert out["status"] == "ok"
    assert out["parity_status"] == "ok"
    assert Path(out["artifact_path"]).exists()
    assert captured["run_id"] == "job_1"
    assert captured["profile"] == "core-weekly"
    assert captured["as_of_date"] == "2026-03-20"
    assert isinstance(captured["neon_mirror"], dict)
    assert captured["neon_mirror"]["status"] == "ok"
    assert captured["neon_mirror"]["parity"] == {"status": "ok", "issues": []}
