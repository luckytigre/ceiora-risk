from __future__ import annotations

import argparse
from contextlib import contextmanager
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


def test_cpar_runner_kwargs_use_snapshot_archive() -> None:
    snapshot_path = Path("/tmp/snapshot.db")

    out = cutover._cpar_runner_kwargs(package_date="2026-03-20", data_db=snapshot_path)

    assert out == {
        "profile": "cpar-package-date",
        "as_of_date": "2026-03-20",
        "data_db": snapshot_path,
    }


def test_cuse_runner_kwargs_use_snapshot_archive() -> None:
    snapshot_path = Path("/tmp/snapshot.db")
    resolved_snapshot_path = snapshot_path.resolve()

    out = cutover._cuse_runner_kwargs(as_of_date="2026-03-26", data_db=snapshot_path)

    assert out == {
        "profile": "cold-core",
        "as_of_date": "2026-03-26",
        "from_stage": "neon_readiness",
        "to_stage": "serving_refresh",
        "force_core": True,
        "data_db": snapshot_path,
        "cache_db": resolved_snapshot_path.with_name(
            f"{resolved_snapshot_path.stem}.2026-03-26.cache{resolved_snapshot_path.suffix}"
        ),
    }


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


def test_run_post_cleanup_checks_forces_explicit_neon_authority(monkeypatch, tmp_path: Path) -> None:
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

    observed: dict[str, object] = {}

    @contextmanager
    def fake_core_backend(name: str):
        observed["backend_name"] = name
        observed["dsn_inside"] = cutover.config.NEON_DATABASE_URL
        observed["data_backend_inside"] = cutover.config.DATA_BACKEND
        observed["serving_reads_inside"] = cutover.config.SERVING_OUTPUTS_PRIMARY_READS
        yield

    monkeypatch.setattr(cutover.core_reads, "core_read_backend", fake_core_backend)
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

    monkeypatch.setattr(cutover.config, "NEON_DATABASE_URL", "")
    monkeypatch.setattr(cutover.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(cutover.config, "SERVING_OUTPUTS_PRIMARY_READS", False)

    out = cutover._run_post_cleanup_checks(
        dsn="postgresql://example",
        include_holdings=False,
        sqlite_path=sqlite_path,
    )

    assert out["status"] == "ok"
    assert observed == {
        "backend_name": "neon",
        "dsn_inside": "postgresql://example",
        "data_backend_inside": "neon",
        "serving_reads_inside": True,
    }
    assert cutover.config.NEON_DATABASE_URL == ""
    assert cutover.config.DATA_BACKEND == "sqlite"
    assert cutover.config.SERVING_OUTPUTS_PRIMARY_READS is False


def test_run_post_cleanup_checks_fails_when_runtime_probe_is_empty(monkeypatch, tmp_path: Path) -> None:
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
        lambda: type("Frame", (), {"index": []})(),
    )
    monkeypatch.setattr(
        cutover.core_reads,
        "load_latest_fundamentals",
        lambda: type("Frame", (), {"index": [1, 2]})(),
    )
    monkeypatch.setattr(cutover.cpar_source_reads, "load_build_universe_rows", lambda: [{"ric": "AAA.OQ"}])
    monkeypatch.setattr(cutover.cpar_source_reads, "resolve_factor_proxy_rows", lambda _tickers: [{"ticker": "SPY"}])
    monkeypatch.setattr(cutover, "load_runtime_payload", lambda name: {"payload": name})

    with pytest.raises(RuntimeError, match="latest price read returned no rows"):
        cutover._run_post_cleanup_checks(
            dsn="postgresql://example",
            include_holdings=False,
            sqlite_path=sqlite_path,
        )


def test_main_routes_cpar_validation_runs_to_snapshot_archive(monkeypatch, tmp_path: Path, capsys) -> None:
    source_db = tmp_path / "source.db"
    sqlite3.connect(str(source_db)).close()
    seed_path = tmp_path / "security_registry_seed.csv"
    seed_path.write_text("ric,ticker\n", encoding="utf-8")
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    captured: dict[str, dict[str, object]] = {}

    monkeypatch.setattr(
        cutover,
        "_parse_args",
        lambda: argparse.Namespace(
            dsn="postgresql://example",
            db_path=source_db,
            seed_path=seed_path,
            snapshot_dir=snapshot_dir,
            artifact_dir=None,
            canonical_schema=tmp_path / "canonical.sql",
            cpar_schema=tmp_path / "cpar.sql",
            holdings_schema=tmp_path / "holdings.sql",
            cleanup_schema=tmp_path / "cleanup.sql",
            sync_mode="incremental",
            historical_cuse_samples=0,
            cpar_max_backfill=1,
            include_holdings=False,
            include_cleanup=False,
            json=True,
        ),
    )
    monkeypatch.setattr(cutover, "resolve_dsn", lambda dsn: dsn or "postgresql://example")
    monkeypatch.setattr(cutover, "bootstrap_cuse4_source_tables", lambda **kwargs: {"status": "ok"})

    def fake_backup(_source: Path, target: Path) -> None:
        sqlite3.connect(str(target)).close()

    monkeypatch.setattr(cutover, "_sqlite_backup", fake_backup)
    monkeypatch.setattr(cutover, "_validate_required_snapshot_tables", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(cutover, "inspect_sqlite_source_integrity", lambda **kwargs: {"status": "ok", "issues": []})
    monkeypatch.setattr(cutover, "_apply_schema_stack", lambda **kwargs: [{"status": "ok"}])
    monkeypatch.setattr(cutover, "sync_from_sqlite_to_neon", lambda **kwargs: {"status": "ok"})
    monkeypatch.setattr(cutover, "_latest_source_date", lambda _sqlite_path: "2026-03-26")
    monkeypatch.setattr(cutover, "validate_neon_rebuild_readiness", lambda **kwargs: {"status": "ok"})
    monkeypatch.setattr(cutover, "_historical_cuse_sample_dates", lambda *args, **kwargs: ["2025-12-03"])
    monkeypatch.setattr(cutover, "resolve_package_date", lambda **kwargs: "2026-03-20")
    monkeypatch.setattr(cutover, "_historical_cpar_package_dates", lambda *args, **kwargs: ["2026-03-20", "2026-03-13"])

    def fake_run_and_record(*, run_key: str, artifact_dir: Path, runner, runner_kwargs: dict[str, object]) -> dict[str, object]:
        captured[run_key] = dict(runner_kwargs)
        return {"status": "ok", "run_id": run_key, "selected_stages": [], "stage_results": [], "run_rows": []}

    monkeypatch.setattr(cutover, "_run_and_record", fake_run_and_record)

    rc = cutover.main()

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "ok"
    snapshot_path = Path(out["snapshot_path"])
    assert captured["cuse_latest_2026-03-26"]["data_db"] == snapshot_path
    assert captured["cuse_latest_2026-03-26"]["cache_db"] == snapshot_path.with_name(
        f"{snapshot_path.stem}.2026-03-26.cache{snapshot_path.suffix}"
    )
    assert captured["cuse_historical_2025-12-03"]["data_db"] == snapshot_path
    assert captured["cuse_historical_2025-12-03"]["cache_db"] == snapshot_path.with_name(
        f"{snapshot_path.stem}.2025-12-03.cache{snapshot_path.suffix}"
    )
    assert captured["cpar_latest_2026-03-20"]["data_db"] == snapshot_path
    assert captured["cpar_historical_2026-03-13"]["data_db"] == snapshot_path


def test_main_runs_cleanup_branch_when_enabled(monkeypatch, tmp_path: Path, capsys) -> None:
    source_db = tmp_path / "source.db"
    sqlite3.connect(str(source_db)).close()
    seed_path = tmp_path / "security_registry_seed.csv"
    seed_path.write_text("ric,ticker\n", encoding="utf-8")
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()

    calls: dict[str, object] = {}

    monkeypatch.setattr(
        cutover,
        "_parse_args",
        lambda: argparse.Namespace(
            dsn="postgresql://example",
            db_path=source_db,
            seed_path=seed_path,
            snapshot_dir=snapshot_dir,
            artifact_dir=None,
            canonical_schema=tmp_path / "canonical.sql",
            cpar_schema=tmp_path / "cpar.sql",
            holdings_schema=tmp_path / "holdings.sql",
            cleanup_schema=tmp_path / "cleanup.sql",
            sync_mode="incremental",
            historical_cuse_samples=0,
            cpar_max_backfill=0,
            include_holdings=True,
            include_cleanup=True,
            json=True,
        ),
    )
    monkeypatch.setattr(cutover, "resolve_dsn", lambda dsn: dsn or "postgresql://example")
    monkeypatch.setattr(cutover, "bootstrap_cuse4_source_tables", lambda **kwargs: {"status": "ok"})
    monkeypatch.setattr(
        cutover,
        "_sqlite_backup",
        lambda _source, target: sqlite3.connect(str(target)).close(),
    )
    monkeypatch.setattr(cutover, "_validate_required_snapshot_tables", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(cutover, "inspect_sqlite_source_integrity", lambda **kwargs: {"status": "ok", "issues": []})
    monkeypatch.setattr(cutover, "_apply_schema_stack", lambda **kwargs: [{"status": "ok"}])
    monkeypatch.setattr(cutover, "sync_from_sqlite_to_neon", lambda **kwargs: {"status": "ok"})
    monkeypatch.setattr(cutover, "_latest_source_date", lambda _sqlite_path: "2026-03-26")
    monkeypatch.setattr(cutover, "validate_neon_rebuild_readiness", lambda **kwargs: {"status": "ok"})
    monkeypatch.setattr(cutover, "_historical_cuse_sample_dates", lambda *args, **kwargs: [])
    monkeypatch.setattr(cutover, "resolve_package_date", lambda **kwargs: "2026-03-20")
    monkeypatch.setattr(cutover, "_historical_cpar_package_dates", lambda *args, **kwargs: ["2026-03-20"])
    monkeypatch.setattr(
        cutover,
        "_run_and_record",
        lambda **kwargs: {"status": "ok", "run_id": "run", "selected_stages": [], "stage_results": [], "run_rows": []},
    )
    monkeypatch.setattr(
        cutover,
        "_apply_cleanup",
        lambda **kwargs: _record_and_return(calls, "cleanup_kwargs", kwargs),
    )
    monkeypatch.setattr(
        cutover,
        "_run_post_cleanup_checks",
        lambda **kwargs: _record_and_return(calls, "post_cleanup_kwargs", kwargs),
    )

    rc = cutover.main()

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "ok"
    assert calls["cleanup_kwargs"]["dsn"] == "postgresql://example"
    assert Path(str(calls["cleanup_kwargs"]["cleanup_schema"])).name == "cleanup.sql"
    assert calls["post_cleanup_kwargs"]["dsn"] == "postgresql://example"
    assert calls["post_cleanup_kwargs"]["include_holdings"] is True
    assert Path(str(calls["post_cleanup_kwargs"]["sqlite_path"])).exists()
    assert out["cleanup"]["status"] == "ok"
    assert out["post_cleanup_checks"]["status"] == "ok"


def _record_and_return(calls: dict[str, object], key: str, kwargs: dict[str, object]) -> dict[str, object]:
    calls[key] = dict(kwargs)
    return {"status": "ok"}
