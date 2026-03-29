from __future__ import annotations

import json
from pathlib import Path

from backend.services import neon_mirror_reporting


def test_write_neon_mirror_artifact_persists_report_files(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(neon_mirror_reporting.config, "APP_DATA_DIR", str(tmp_path))

    artifact_path = neon_mirror_reporting.write_neon_mirror_artifact(
        run_id="job_1",
        profile="source-daily",
        as_of_date="2026-03-20",
        overall_status="ok",
        neon_mirror={"status": "ok", "sync": {"status": "ok"}},
    )

    payload = json.loads(Path(artifact_path).read_text(encoding="utf-8"))
    latest_path = tmp_path / "audit_reports" / "neon_parity" / "latest_neon_mirror_report.json"

    assert payload["run_id"] == "job_1"
    assert payload["profile"] == "source-daily"
    assert payload["neon_mirror"]["status"] == "ok"
    assert latest_path.exists()


def test_extract_neon_mirror_error_reads_nested_step_error() -> None:
    out = neon_mirror_reporting.extract_neon_mirror_error(
        {
            "status": "mismatch",
            "parity": {
                "status": "mismatch",
                "error": {
                    "type": "RuntimeError",
                    "message": "parity mismatch",
                },
            },
        }
    )

    assert out == {
        "type": "RuntimeError",
        "message": "parity mismatch",
    }


def test_publish_neon_sync_health_reports_warning_for_skipped_mirror(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        neon_mirror_reporting,
        "persist_runtime_health_payload",
        lambda key, payload: captured.update({"key": key, "payload": payload}) or {"status": "ok"},
    )

    neon_mirror_reporting.publish_neon_sync_health(
        run_id="job_1",
        profile="serve-refresh",
        as_of_date="2026-03-20",
        neon_mirror={"status": "skipped", "reason": "profile_skips_broad_neon_mirror"},
        artifact_path=None,
    )

    assert captured["key"] == "neon_sync_health"
    payload = captured["payload"]
    assert isinstance(payload, dict)
    assert payload["status"] == "warning"
    assert payload["mirror_status"] == "skipped"
    assert payload["artifact_path"] is None
