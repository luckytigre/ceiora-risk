from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from backend.scripts import neon_registry_first_cutover as cutover


def test_write_json_artifact_serializes_path_values(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "artifacts"
    payload = {
        "status": "ok",
        "workspace": {
            "data_db": tmp_path / "data.db",
        },
    }

    artifact_path = cutover._write_json_artifact(artifact_dir, "cuse latest 2026-03-25", payload)

    assert artifact_path.exists()
    assert artifact_path.name == "cuse_latest_2026-03-25.json"
    written = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert written["workspace"]["data_db"] == str(tmp_path / "data.db")


def test_summarize_pipeline_result_returns_compact_summary(tmp_path: Path) -> None:
    artifact_path = tmp_path / "cuse_latest.json"
    result = {
        "status": "ok",
        "run_id": "job_123",
        "profile": "cold-core",
        "profile_label": "Cold Core",
        "as_of_date": "2026-03-25",
        "core_will_run": True,
        "selected_stages": ["neon_readiness", "serving_refresh"],
        "stage_results": [
            {"stage": "neon_readiness", "status": "completed", "details": {"rows": 1}},
            {
                "stage": "serving_refresh",
                "status": "failed",
                "details": {"rows": 2},
                "error": {"type": "RuntimeError", "message": "boom"},
            },
        ],
        "run_rows": [{"stage_name": "neon_readiness"}, {"stage_name": "serving_refresh"}],
        "neon_mirror": {"status": "ok"},
        "local_mirror_sync": {"status": "ok"},
        "workspace": {"data_db": "workspace.db"},
    }

    summary = cutover._summarize_pipeline_result(result, artifact_path=artifact_path)

    assert summary["status"] == "ok"
    assert summary["run_id"] == "job_123"
    assert summary["profile"] == "cold-core"
    assert summary["artifact_path"] == str(artifact_path)
    assert summary["selected_stage_count"] == 2
    assert summary["run_row_count"] == 2
    assert summary["stage_statuses"] == [
        {"stage": "neon_readiness", "status": "completed"},
        {
            "stage": "serving_refresh",
            "status": "failed",
            "error_type": "RuntimeError",
            "error_message": "boom",
        },
    ]
    assert "stage_results" not in summary
    assert "run_rows" not in summary


def test_validate_required_snapshot_tables_checks_missing_and_empty(tmp_path: Path) -> None:
    db_path = tmp_path / "snapshot.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE security_registry (ric TEXT PRIMARY KEY)")
    conn.execute("CREATE TABLE security_taxonomy_current (ric TEXT PRIMARY KEY)")
    conn.execute("INSERT INTO security_taxonomy_current (ric) VALUES ('AAPL.OQ')")
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match="missing tables: security_policy_current; empty required tables: security_registry"):
        cutover._validate_required_snapshot_tables(
            db_path,
            required_tables=("security_registry", "security_taxonomy_current", "security_policy_current"),
            required_nonempty_tables=("security_registry",),
        )


def test_run_post_cleanup_checks_includes_sync_probe_and_cleanliness(monkeypatch, tmp_path: Path) -> None:
    sqlite_path = tmp_path / "snapshot.db"
    sqlite_path.write_text("", encoding="utf-8")

    monkeypatch.setattr(
        cutover,
        "_pg_table_exists",
        lambda _dsn, table: table
        in {
            "security_registry",
            "security_policy_current",
            "security_taxonomy_current",
            "security_master_compat_current",
            "source_sync_runs",
            "source_sync_watermarks",
            "security_source_status_current",
        },
    )
    monkeypatch.setattr(
        cutover,
        "_run_post_cleanup_sync_probe",
        lambda *, sqlite_path, dsn: {
            "status": "ok",
            "sync_run_id": "source_sync_probe_1",
            "watermark_rows_updated": 1,
            "security_source_status_current_rows": 5,
        },
    )
    monkeypatch.setattr(
        cutover,
        "_probe_live_legacy_cleanliness",
        lambda *, dsn: {"status": "ok", "issues": [], "legacy_columns": [], "legacy_indexes": []},
    )
    monkeypatch.setattr(
        cutover.core_reads,
        "load_latest_prices",
        lambda: type("Frame", (), {"index": [1, 2, 3]})(),
    )
    monkeypatch.setattr(
        cutover.core_reads,
        "load_latest_fundamentals",
        lambda: type("Frame", (), {"index": [1, 2]})(),
    )
    monkeypatch.setattr(cutover.cpar_source_reads, "load_build_universe_rows", lambda: [{"ric": "AAA.OQ"}])
    monkeypatch.setattr(cutover.cpar_source_reads, "resolve_factor_proxy_rows", lambda _tickers: [{"ticker": "SPY"}])
    monkeypatch.setattr(cutover, "load_runtime_payload", lambda name: {"payload": name})
    monkeypatch.setattr(cutover.holdings_reads, "load_holdings_accounts", lambda: [{"account_id": "ibkr_multistrat"}])
    monkeypatch.setattr(
        cutover.holdings_reads,
        "load_holdings_positions",
        lambda *, account_id: [{"account_id": account_id, "ric": "AAA.OQ"}],
    )

    out = cutover._run_post_cleanup_checks(
        dsn="postgresql://example",
        include_holdings=True,
        sqlite_path=sqlite_path,
    )

    assert out["status"] == "ok"
    assert out["post_cleanup_sync_probe"]["sync_run_id"] == "source_sync_probe_1"
    assert out["legacy_schema_cleanliness"]["status"] == "ok"
    assert out["universe_payload_keys"] == 1
    assert out["holdings_positions_rows"] == 1


def test_run_post_cleanup_checks_fails_when_legacy_schema_artifacts_remain(monkeypatch, tmp_path: Path) -> None:
    sqlite_path = tmp_path / "snapshot.db"
    sqlite_path.write_text("", encoding="utf-8")

    monkeypatch.setattr(
        cutover,
        "_pg_table_exists",
        lambda _dsn, table: table
        in {
            "security_registry",
            "security_policy_current",
            "security_taxonomy_current",
            "security_master_compat_current",
            "source_sync_runs",
            "source_sync_watermarks",
            "security_source_status_current",
        },
    )
    monkeypatch.setattr(
        cutover,
        "_run_post_cleanup_sync_probe",
        lambda *, sqlite_path, dsn: {
            "status": "ok",
            "sync_run_id": "source_sync_probe_1",
            "watermark_rows_updated": 1,
            "security_source_status_current_rows": 5,
        },
    )
    monkeypatch.setattr(
        cutover,
        "_probe_live_legacy_cleanliness",
        lambda *, dsn: {
            "status": "failed",
            "issues": ["legacy_columns_present"],
            "legacy_columns": [{"table_name": "foo", "column_name": "sid"}],
            "legacy_indexes": [],
        },
    )

    with pytest.raises(RuntimeError, match="legacy schema/index artifacts remain"):
        cutover._run_post_cleanup_checks(
            dsn="postgresql://example",
            include_holdings=False,
            sqlite_path=sqlite_path,
        )
