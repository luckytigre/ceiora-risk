from __future__ import annotations

import sqlite3
import importlib
import sys
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

run_model_pipeline = importlib.import_module("backend.orchestration.run_model_pipeline")
from backend.services import refresh_manager

_UNUSED_DATA_DB = Path("__unused_test_data__.db")
_UNUSED_CACHE_DB = Path("__unused_test_cache__.db")


def test_run_model_pipeline_import_does_not_require_lseg_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in [
        "backend.orchestration.run_model_pipeline",
        "backend.scripts.download_data_lseg",
        "backend.scripts.backfill_prices_range_lseg",
        "backend.scripts.backfill_pit_history_lseg",
        "lseg",
        "lseg.data",
    ]:
        sys.modules.pop(name, None)

    imported = importlib.import_module("backend.orchestration.run_model_pipeline")

    assert callable(imported._download_from_lseg)
    assert callable(imported._backfill_prices)
    assert callable(imported._backfill_pit_history)


def test_refresh_manager_import_does_not_require_lseg_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in [
        "backend.services.refresh_manager",
        "backend.orchestration.run_model_pipeline",
        "backend.scripts.download_data_lseg",
        "backend.scripts.backfill_prices_range_lseg",
        "backend.scripts.backfill_pit_history_lseg",
        "lseg",
        "lseg.data",
    ]:
        sys.modules.pop(name, None)

    imported = importlib.import_module("backend.services.refresh_manager")

    assert callable(imported.start_refresh)
    assert callable(imported.get_refresh_status)


def test_default_profile_is_local_daily_plus_core(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(refresh_manager.config, "APP_RUNTIME_ROLE", "local-ingest")
    assert refresh_manager._resolve_profile(None) == "source-daily-plus-core-if-due"


def test_default_profile_is_cloud_serve_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(refresh_manager.config, "APP_RUNTIME_ROLE", "cloud-serve")
    assert refresh_manager._resolve_profile(None) == "serve-refresh"


def test_local_serve_refresh_prefers_local_source_archive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_model_pipeline.config, "APP_RUNTIME_ROLE", "local-ingest")

    assert run_model_pipeline.runtime_support.profile_prefers_local_source_archive("serve-refresh") is True


def test_cloud_serve_refresh_does_not_prefer_local_source_archive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_model_pipeline.config, "APP_RUNTIME_ROLE", "cloud-serve")

    assert run_model_pipeline.runtime_support.profile_prefers_local_source_archive("serve-refresh") is False


def test_unknown_profile_is_rejected() -> None:
    with pytest.raises(ValueError, match="Invalid profile"):
        refresh_manager._resolve_profile("daily-with-core-if-due")


def test_invalid_stage_window_is_rejected_before_worker_start() -> None:
    with pytest.raises(ValueError, match="--from-stage must be before or equal to --to-stage"):
        refresh_manager.start_refresh(
            force_risk_recompute=False,
            profile="serve-refresh",
            from_stage="risk_model",
            to_stage="ingest",
        )


def test_force_core_conflict_is_rejected_before_worker_start() -> None:
    with pytest.raises(ValueError, match="force_core requires a stage window"):
        refresh_manager.start_refresh(
            force_risk_recompute=False,
            force_core=True,
            profile="serve-refresh",
            from_stage="serving_refresh",
            to_stage="serving_refresh",
        )


def test_force_core_adds_core_stages_for_serve_refresh_defaults() -> None:
    selected = run_model_pipeline._apply_force_core_stage_selection(
        selected=["serving_refresh"],
        force_core=True,
        from_stage=None,
        to_stage=None,
    )

    assert selected == ["factor_returns", "risk_model", "serving_refresh"]


def test_force_core_rejects_explicit_stage_window_without_core_stages() -> None:
    with pytest.raises(ValueError, match="force_core requires a stage window"):
        run_model_pipeline._apply_force_core_stage_selection(
            selected=["serving_refresh"],
            force_core=True,
            from_stage="serving_refresh",
            to_stage="serving_refresh",
        )


def test_planned_stages_for_profile_rejects_invalid_force_core_window() -> None:
    with pytest.raises(ValueError, match="force_core requires a stage window"):
        run_model_pipeline.planned_stages_for_profile(
            profile="serve-refresh",
            from_stage="serving_refresh",
            to_stage="serving_refresh",
            force_core=True,
        )


def test_cold_profile_config_enables_full_rebuild_and_cache_reset() -> None:
    cfg = run_model_pipeline.PROFILE_CONFIG["cold-core"]
    assert cfg["core_policy"] == "always"
    assert cfg["serving_mode"] == "full"
    assert cfg["raw_history_policy"] == "full-daily"
    assert bool(cfg["reset_core_cache"]) is True


def test_publish_only_profile_config_reuses_cached_payloads() -> None:
    cfg = run_model_pipeline.PROFILE_CONFIG["publish-only"]
    assert cfg["core_policy"] == "never"
    assert cfg["serving_mode"] == "publish"
    assert cfg["raw_history_policy"] == "none"
    assert cfg["default_stages"] == ["serving_refresh"]


def test_source_daily_profile_enables_ingest_without_core() -> None:
    cfg = run_model_pipeline.PROFILE_CONFIG["source-daily"]
    assert cfg["core_policy"] == "never"
    assert cfg["enable_ingest"] is True
    assert cfg["default_stages"] == ["ingest", "serving_refresh"]


def test_orchestrator_ingest_stage_runs_single_full_universe_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[dict[str, object]] = []
    monkeypatch.setattr(run_model_pipeline.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(run_model_pipeline.config, "ORCHESTRATOR_ENABLE_INGEST", True)
    monkeypatch.setattr(
        run_model_pipeline,
        "bootstrap_cuse4_source_tables",
        lambda **_kwargs: {"status": "ok"},
    )

    def _fake_download(**kwargs):
        captured.append(dict(kwargs))
        return {"status": "ok"}

    monkeypatch.setattr(run_model_pipeline, "download_from_lseg", _fake_download)

    out = run_model_pipeline._run_stage(
        profile="source-daily",
        stage="ingest",
        as_of_date="2026-03-14",
        should_run_core=False,
        serving_mode="light",
        force_core=False,
        core_reason="test",
        data_db=_UNUSED_DATA_DB,
        cache_db=_UNUSED_CACHE_DB,
        enable_ingest=True,
    )

    assert out["status"] == "ok"
    assert len(captured) == 1
    assert captured[0]["shard_count"] == 1
    assert captured[0]["shard_index"] == 0
    assert captured[0]["write_prices"] is True
    assert captured[0]["write_fundamentals"] is False
    assert captured[0]["write_classification"] is False


def test_orchestrator_ingest_stage_repairs_price_gap_after_latest_pull(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    conn.execute("CREATE TABLE security_prices_eod (ric TEXT, date TEXT, close REAL, updated_at TEXT)")
    conn.execute("INSERT INTO security_prices_eod VALUES ('AAPL.OQ', '2026-03-04', 100.0, '2026-03-04T00:00:00+00:00')")
    conn.commit()
    conn.close()

    monkeypatch.setattr(run_model_pipeline.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(run_model_pipeline.config, "ORCHESTRATOR_ENABLE_INGEST", True)
    monkeypatch.setattr(run_model_pipeline, "bootstrap_cuse4_source_tables", lambda **_kwargs: {"status": "ok"})
    monkeypatch.setattr(run_model_pipeline, "download_from_lseg", lambda **_kwargs: {"status": "ok", "as_of": "2026-03-14"})

    def _fake_backfill_prices(**kwargs):
        captured.update(kwargs)
        return {"status": "ok", "rows_upserted": 123, "start_date": kwargs["start_date"], "end_date": kwargs["end_date"]}

    monkeypatch.setattr(run_model_pipeline, "backfill_prices", _fake_backfill_prices)

    out = run_model_pipeline._run_stage(
        profile="source-daily",
        stage="ingest",
        as_of_date="2026-03-14",
        should_run_core=False,
        serving_mode="light",
        force_core=False,
        core_reason="test",
        data_db=data_db,
        cache_db=tmp_path / "cache.db",
        enable_ingest=True,
    )

    assert out["status"] == "ok"
    assert out["price_gap_repair"]["status"] == "ok"
    assert captured["start_date"] == "2026-03-05"
    assert captured["end_date"] == "2026-03-14"


def test_orchestrator_ingest_stage_repairs_price_gap_from_pre_ingest_latest_date(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    conn.execute("CREATE TABLE security_prices_eod (ric TEXT, date TEXT, close REAL, updated_at TEXT)")
    conn.execute("CREATE TABLE security_fundamentals_pit (ric TEXT, as_of_date TEXT, stat_date TEXT)")
    conn.execute("CREATE TABLE security_classification_pit (ric TEXT, as_of_date TEXT)")
    conn.execute("INSERT INTO security_prices_eod VALUES ('AAPL.OQ', '2026-03-04', 100.0, '2026-03-04T00:00:00+00:00')")
    conn.execute("INSERT INTO security_fundamentals_pit VALUES ('AAPL.OQ', '2026-03-04', '2026-03-04')")
    conn.execute("INSERT INTO security_classification_pit VALUES ('AAPL.OQ', '2026-03-04')")
    conn.commit()
    conn.close()

    monkeypatch.setattr(run_model_pipeline.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(run_model_pipeline.config, "ORCHESTRATOR_ENABLE_INGEST", True)
    monkeypatch.setattr(run_model_pipeline, "bootstrap_cuse4_source_tables", lambda **_kwargs: {"status": "ok"})

    def _fake_download(**kwargs):
        conn = sqlite3.connect(str(data_db))
        conn.execute(
            "INSERT INTO security_prices_eod VALUES ('AAPL.OQ', ?, 101.0, '2026-03-14T00:00:00+00:00')",
            (kwargs["as_of_date"],),
        )
        conn.commit()
        conn.close()
        return {"status": "ok", "as_of": kwargs["as_of_date"]}

    def _fake_backfill_prices(**kwargs):
        captured.update(kwargs)
        return {"status": "ok", "rows_upserted": 123}

    monkeypatch.setattr(run_model_pipeline, "download_from_lseg", _fake_download)
    monkeypatch.setattr(run_model_pipeline, "backfill_prices", _fake_backfill_prices)

    out = run_model_pipeline._run_stage(
        profile="source-daily",
        stage="ingest",
        as_of_date="2026-03-14",
        should_run_core=False,
        serving_mode="light",
        force_core=False,
        core_reason="test",
        data_db=data_db,
        cache_db=tmp_path / "cache.db",
        enable_ingest=True,
    )

    assert out["status"] == "ok"
    assert captured["start_date"] == "2026-03-05"
    assert captured["end_date"] == "2026-03-14"


def test_orchestrator_ingest_stage_skips_price_gap_repair_when_latest_is_current(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    conn.execute("CREATE TABLE security_prices_eod (ric TEXT, date TEXT, close REAL, updated_at TEXT)")
    conn.execute("INSERT INTO security_prices_eod VALUES ('AAPL.OQ', '2026-03-14', 100.0, '2026-03-14T00:00:00+00:00')")
    conn.commit()
    conn.close()

    monkeypatch.setattr(run_model_pipeline.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(run_model_pipeline.config, "ORCHESTRATOR_ENABLE_INGEST", True)
    monkeypatch.setattr(run_model_pipeline, "bootstrap_cuse4_source_tables", lambda **_kwargs: {"status": "ok"})
    monkeypatch.setattr(run_model_pipeline, "download_from_lseg", lambda **_kwargs: {"status": "ok", "as_of": "2026-03-14"})

    def _unexpected_backfill(**_kwargs):
        raise AssertionError("backfill_prices should not be called when latest price date is current")

    monkeypatch.setattr(run_model_pipeline, "backfill_prices", _unexpected_backfill)

    out = run_model_pipeline._run_stage(
        profile="source-daily",
        stage="ingest",
        as_of_date="2026-03-14",
        should_run_core=False,
        serving_mode="light",
        force_core=False,
        core_reason="test",
        data_db=data_db,
        cache_db=tmp_path / "cache.db",
        enable_ingest=True,
    )

    assert out["status"] == "ok"
    assert out["price_gap_repair"]["status"] == "skipped"
    assert out["price_gap_repair"]["reason"] == "latest_price_date_current"


def test_orchestrator_ingest_stage_purges_open_period_pit_rows_under_closed_month_policy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[dict[str, object]] = []
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    conn.execute("CREATE TABLE security_prices_eod (ric TEXT, date TEXT, close REAL, updated_at TEXT)")
    conn.execute("CREATE TABLE security_fundamentals_pit (ric TEXT, as_of_date TEXT, stat_date TEXT)")
    conn.execute("CREATE TABLE security_classification_pit (ric TEXT, as_of_date TEXT)")
    conn.execute("INSERT INTO security_prices_eod VALUES ('AAPL.OQ', '2026-03-14', 100.0, '2026-03-14T00:00:00+00:00')")
    conn.execute("INSERT INTO security_fundamentals_pit VALUES ('AAPL.OQ', '2026-02-27', '2026-02-27')")
    conn.execute("INSERT INTO security_fundamentals_pit VALUES ('AAPL.OQ', '2026-03-04', '2026-03-04')")
    conn.execute("INSERT INTO security_classification_pit VALUES ('AAPL.OQ', '2026-02-27')")
    conn.execute("INSERT INTO security_classification_pit VALUES ('AAPL.OQ', '2026-03-04')")
    conn.commit()
    conn.close()

    monkeypatch.setattr(run_model_pipeline.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(run_model_pipeline.config, "ORCHESTRATOR_ENABLE_INGEST", True)
    monkeypatch.setattr(run_model_pipeline.config, "SOURCE_DAILY_PIT_FREQUENCY", "monthly")
    monkeypatch.setattr(run_model_pipeline, "bootstrap_cuse4_source_tables", lambda **_kwargs: {"status": "ok"})
    monkeypatch.setattr(
        run_model_pipeline,
        "backfill_pit_history",
        lambda **_kwargs: {"status": "skipped", "reason": "no-dates"},
    )

    def _fake_download(**kwargs):
        calls.append(dict(kwargs))
        return {"status": "ok", "as_of": kwargs["as_of_date"], "fundamental_rows_inserted": 1, "classification_rows_inserted": 1}

    monkeypatch.setattr(run_model_pipeline, "download_from_lseg", _fake_download)

    out = run_model_pipeline._run_stage(
        profile="source-daily",
        stage="ingest",
        as_of_date="2026-03-14",
        should_run_core=False,
        serving_mode="light",
        force_core=False,
        core_reason="test",
        data_db=data_db,
        cache_db=tmp_path / "cache.db",
        enable_ingest=True,
    )

    assert out["status"] == "ok"
    assert len(calls) == 1
    assert out["pit_gap_repair"]["status"] == "ok"
    assert out["pit_gap_repair"]["latest_closed_anchor"] == "2026-02-27"
    assert out["pit_gap_repair"]["open_period_cleanup"]["deleted_rows"]["security_fundamentals_pit"] == 1
    assert out["pit_gap_repair"]["open_period_cleanup"]["deleted_rows"]["security_classification_pit"] == 1
    assert out["pit_gap_repair"]["current_period_repair"]["reason"] == "closed_period_only_policy"
    conn = sqlite3.connect(str(data_db))
    try:
        fund_dates = [row[0] for row in conn.execute("SELECT DISTINCT as_of_date FROM security_fundamentals_pit ORDER BY as_of_date").fetchall()]
        class_dates = [row[0] for row in conn.execute("SELECT DISTINCT as_of_date FROM security_classification_pit ORDER BY as_of_date").fetchall()]
    finally:
        conn.close()
    assert fund_dates == ["2026-02-27"]
    assert class_dates == ["2026-02-27"]


def test_orchestrator_ingest_stage_backfills_missing_closed_month_pit_anchors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[dict[str, object]] = []
    captured: dict[str, object] = {}
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    conn.execute("CREATE TABLE security_prices_eod (ric TEXT, date TEXT, close REAL, updated_at TEXT)")
    conn.execute("CREATE TABLE security_fundamentals_pit (ric TEXT, as_of_date TEXT, stat_date TEXT)")
    conn.execute("CREATE TABLE security_classification_pit (ric TEXT, as_of_date TEXT)")
    conn.execute("INSERT INTO security_prices_eod VALUES ('AAPL.OQ', '2026-03-14', 100.0, '2026-03-14T00:00:00+00:00')")
    conn.execute("INSERT INTO security_fundamentals_pit VALUES ('AAPL.OQ', '2026-01-30', '2026-01-30')")
    conn.execute("INSERT INTO security_classification_pit VALUES ('AAPL.OQ', '2026-01-30')")
    conn.commit()
    conn.close()

    monkeypatch.setattr(run_model_pipeline.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(run_model_pipeline.config, "ORCHESTRATOR_ENABLE_INGEST", True)
    monkeypatch.setattr(run_model_pipeline.config, "SOURCE_DAILY_PIT_FREQUENCY", "monthly")
    monkeypatch.setattr(run_model_pipeline, "bootstrap_cuse4_source_tables", lambda **_kwargs: {"status": "ok"})

    def _fake_download(**kwargs):
        calls.append(dict(kwargs))
        return {"status": "ok", "as_of": kwargs["as_of_date"]}

    def _fake_backfill_pit_history(**kwargs):
        captured.update(kwargs)
        return {"status": "ok", "start_date": kwargs["start_date"], "end_date": kwargs["end_date"]}

    monkeypatch.setattr(run_model_pipeline, "download_from_lseg", _fake_download)
    monkeypatch.setattr(run_model_pipeline, "backfill_pit_history", _fake_backfill_pit_history)

    out = run_model_pipeline._run_stage(
        profile="source-daily",
        stage="ingest",
        as_of_date="2026-03-14",
        should_run_core=False,
        serving_mode="light",
        force_core=False,
        core_reason="test",
        data_db=data_db,
        cache_db=tmp_path / "cache.db",
        enable_ingest=True,
    )

    assert out["status"] == "ok"
    assert len(calls) == 1
    assert out["pit_gap_repair"]["status"] == "ok"
    assert captured["start_date"] == "2026-02-01"
    assert captured["end_date"] == "2026-02-27"
    assert captured["frequency"] == "monthly"
    assert captured["write_fundamentals"] is True
    assert captured["write_classification"] is True
    assert out["pit_gap_repair"]["current_period_repair"]["reason"] == "closed_period_only_policy"


def test_current_xnys_session_uses_latest_completed_session_before_evening_cutoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_datetime = run_model_pipeline.datetime

    class _FakeDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return real_datetime(2026, 3, 16, 0, 9, 0, tzinfo=ZoneInfo("America/New_York"))

    monkeypatch.setattr(run_model_pipeline, "datetime", _FakeDateTime)

    assert run_model_pipeline.stage_planning.current_xnys_session(datetime_cls=run_model_pipeline.datetime) == "2026-03-13"


def test_cli_profile_choices_are_canonical_only() -> None:
    choices = sorted(run_model_pipeline.PROFILE_CONFIG.keys())
    assert "serve-refresh" in choices
    assert "daily-fast" not in choices


def test_profile_catalog_has_only_canonical_profile_fields() -> None:
    catalog = run_model_pipeline.profile_catalog()
    assert catalog
    assert all("aliases" not in item for item in catalog)
    assert all("rebuild_backend" in item for item in catalog)
    assert all("requires_neon_sync_before_core" in item for item in catalog)
    assert all("source_sync_required" in item for item in catalog)
    assert all("neon_readiness_required" in item for item in catalog)


def test_core_profiles_default_to_local_rebuild_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTHORITATIVE_REBUILDS", False)
    monkeypatch.setattr(run_model_pipeline.config, "DATA_BACKEND", "neon")

    assert run_model_pipeline.profile_rebuild_backend("core-weekly") == "local"
    assert run_model_pipeline.profile_requires_neon_sync_before_core("core-weekly") is False


def test_core_profiles_switch_to_neon_rebuild_backend_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTHORITATIVE_REBUILDS", True)
    monkeypatch.setattr(run_model_pipeline.config, "DATA_BACKEND", "neon")

    assert run_model_pipeline.profile_rebuild_backend("core-weekly") == "neon"
    assert run_model_pipeline.profile_requires_neon_sync_before_core("core-weekly") is True
    assert run_model_pipeline.profile_source_sync_required("core-weekly") is True
    assert run_model_pipeline.profile_neon_readiness_required("core-weekly") is True


def test_planned_stages_insert_source_sync_and_neon_readiness_for_neon_core_profiles(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTHORITATIVE_REBUILDS", True)
    monkeypatch.setattr(run_model_pipeline.config, "DATA_BACKEND", "neon")

    _, _, selected = run_model_pipeline.planned_stages_for_profile(profile="core-weekly")

    assert selected == ["source_sync", "neon_readiness", "factor_returns", "risk_model", "serving_refresh"]


def test_planned_stages_insert_source_sync_for_source_daily_when_neon_is_primary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTHORITATIVE_REBUILDS", False)
    monkeypatch.setattr(run_model_pipeline.config, "DATA_BACKEND", "neon")

    _, _, selected = run_model_pipeline.planned_stages_for_profile(profile="source-daily")

    assert selected == ["ingest", "source_sync", "serving_refresh"]


def test_source_sync_stage_pushes_source_tables_only(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(run_model_pipeline.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(run_model_pipeline.config, "NEON_DATABASE_URL", "postgres://example")
    monkeypatch.setattr(
        run_model_pipeline.core_reads,
        "load_source_dates",
        lambda: {"prices_asof": "2026-03-14", "fundamentals_asof": "2026-03-14", "classification_asof": "2026-03-14"},
    )

    def _fake_mirror(**kwargs):
        captured.update(kwargs)
        return {"status": "ok"}

    monkeypatch.setattr(run_model_pipeline, "run_neon_mirror_cycle", _fake_mirror)

    out = run_model_pipeline._run_stage(
        profile="source-daily",
        stage="source_sync",
        as_of_date="2026-03-14",
        should_run_core=False,
        serving_mode="light",
        force_core=False,
        core_reason="test",
        data_db=_UNUSED_DATA_DB,
        cache_db=_UNUSED_CACHE_DB,
    )

    assert out["status"] == "ok"
    assert captured["tables"] == [
        "security_master",
        "security_prices_eod",
        "security_fundamentals_pit",
        "security_classification_pit",
    ]


def test_source_sync_stage_refuses_to_downgrade_neon_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_model_pipeline.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(run_model_pipeline.config, "NEON_DATABASE_URL", "postgres://example")

    def _load_source_dates(**kwargs):
        backend = run_model_pipeline.core_reads.core_read_backend_name()
        if backend == "local":
            return {"prices_asof": "2026-03-13", "fundamentals_asof": "2026-03-13", "classification_asof": "2026-03-13"}
        return {"prices_asof": "2026-03-14", "fundamentals_asof": "2026-03-14", "classification_asof": "2026-03-14"}

    monkeypatch.setattr(run_model_pipeline.core_reads, "load_source_dates", _load_source_dates)

    with pytest.raises(RuntimeError, match="source_sync refused to overwrite newer Neon source tables"):
        run_model_pipeline._run_stage(
            profile="core-weekly",
            stage="source_sync",
            as_of_date="2026-03-14",
            should_run_core=True,
            serving_mode="full",
            force_core=False,
            core_reason="due",
            data_db=_UNUSED_DATA_DB,
            cache_db=_UNUSED_CACHE_DB,
        )


def test_source_sync_stage_allows_healing_neon_dates_newer_than_target(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(run_model_pipeline.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(run_model_pipeline.config, "NEON_DATABASE_URL", "postgres://example")

    def _load_source_dates(**kwargs):
        backend = run_model_pipeline.core_reads.core_read_backend_name()
        if backend == "local":
            return {"prices_asof": "2026-03-13", "fundamentals_asof": "2026-03-04", "classification_asof": "2026-03-04"}
        return {"prices_asof": "2026-03-16", "fundamentals_asof": "2026-03-04", "classification_asof": "2026-03-04"}

    def _fake_mirror(**kwargs):
        captured.update(kwargs)
        return {"status": "ok"}

    monkeypatch.setattr(run_model_pipeline.core_reads, "load_source_dates", _load_source_dates)
    monkeypatch.setattr(run_model_pipeline, "run_neon_mirror_cycle", _fake_mirror)

    out = run_model_pipeline._run_stage(
        profile="source-daily",
        stage="source_sync",
        as_of_date="2026-03-13",
        should_run_core=False,
        serving_mode="light",
        force_core=False,
        core_reason="within_interval",
        data_db=_UNUSED_DATA_DB,
        cache_db=_UNUSED_CACHE_DB,
    )

    assert out["status"] == "ok"
    assert sorted(out["ignored_newer_than_target"]) == [
        "classification_asof",
        "fundamentals_asof",
        "prices_asof",
    ]
    assert captured["tables"] == [
        "security_master",
        "security_prices_eod",
        "security_fundamentals_pit",
        "security_classification_pit",
    ]


def test_source_sync_stage_allows_healing_open_period_pit_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(run_model_pipeline.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(run_model_pipeline.config, "NEON_DATABASE_URL", "postgres://example")
    monkeypatch.setattr(run_model_pipeline.config, "SOURCE_DAILY_PIT_FREQUENCY", "monthly")

    def _load_source_dates(**kwargs):
        backend = run_model_pipeline.core_reads.core_read_backend_name()
        if backend == "local":
            return {"prices_asof": "2026-03-13", "fundamentals_asof": "2026-02-27", "classification_asof": "2026-02-27"}
        return {"prices_asof": "2026-03-13", "fundamentals_asof": "2026-03-04", "classification_asof": "2026-03-04"}

    def _fake_mirror(**kwargs):
        captured.update(kwargs)
        return {"status": "ok"}

    monkeypatch.setattr(run_model_pipeline.core_reads, "load_source_dates", _load_source_dates)
    monkeypatch.setattr(run_model_pipeline, "run_neon_mirror_cycle", _fake_mirror)

    out = run_model_pipeline._run_stage(
        profile="source-daily",
        stage="source_sync",
        as_of_date="2026-03-13",
        should_run_core=False,
        serving_mode="light",
        force_core=False,
        core_reason="within_interval",
        data_db=_UNUSED_DATA_DB,
        cache_db=_UNUSED_CACHE_DB,
    )

    assert out["status"] == "ok"
    assert sorted(out["ignored_newer_than_target"]) == ["classification_asof", "fundamentals_asof"]
    assert captured["tables"] == [
        "security_master",
        "security_prices_eod",
        "security_fundamentals_pit",
        "security_classification_pit",
    ]


def test_neon_readiness_stage_skips_when_core_is_not_running(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTHORITATIVE_REBUILDS", True)
    monkeypatch.setattr(run_model_pipeline.config, "DATA_BACKEND", "neon")

    out = run_model_pipeline._run_stage(
        profile="source-daily-plus-core-if-due",
        stage="neon_readiness",
        as_of_date="2026-03-14",
        should_run_core=False,
        serving_mode="light",
        force_core=False,
        core_reason="within_interval",
        data_db=_UNUSED_DATA_DB,
        cache_db=_UNUSED_CACHE_DB,
    )

    assert out["status"] == "skipped"
    assert out["reason"] == "core_policy_skip_within_interval"


def test_neon_readiness_stage_prepares_workspace(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTHORITATIVE_REBUILDS", True)
    monkeypatch.setattr(run_model_pipeline.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(
        run_model_pipeline.neon_authority,
        "prepare_neon_rebuild_workspace",
        lambda **kwargs: {
            "status": "ok",
            "workspace": {
                "root_dir": str(tmp_path / "workspace"),
                "data_db": str(tmp_path / "workspace" / "data.db"),
                "cache_db": str(tmp_path / "workspace" / "cache.db"),
            },
            "readiness": {"status": "ok"},
            "copied_tables": [],
        },
    )

    out = run_model_pipeline._run_stage(
        profile="core-weekly",
        stage="neon_readiness",
        as_of_date="2026-03-14",
        should_run_core=True,
        serving_mode="full",
        force_core=False,
        core_reason="due",
        data_db=_UNUSED_DATA_DB,
        cache_db=_UNUSED_CACHE_DB,
        workspace_root=tmp_path / "workspace",
    )

    assert out["status"] == "ok"
    assert out["workspace"]["data_db"].endswith("data.db")


def test_neon_readiness_stage_surfaces_workspace_preparation_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTHORITATIVE_REBUILDS", True)
    monkeypatch.setattr(run_model_pipeline.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(
        run_model_pipeline.neon_authority,
        "prepare_neon_rebuild_workspace",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("Neon rebuild readiness failed: missing_table:security_master")),
    )

    with pytest.raises(RuntimeError, match="Neon rebuild readiness failed: missing_table:security_master"):
        run_model_pipeline._run_stage(
            profile="core-weekly",
            stage="neon_readiness",
            as_of_date="2026-03-14",
            should_run_core=True,
            serving_mode="full",
            force_core=False,
            core_reason="due",
            data_db=_UNUSED_DATA_DB,
            cache_db=_UNUSED_CACHE_DB,
            workspace_root=tmp_path / "workspace",
        )


@pytest.mark.parametrize(
    ("should_run_core", "expected_recompute"),
    [
        (False, False),
        (True, True),
    ],
)
def test_serving_refresh_stage_only_requests_deep_diagnostics_for_core_lanes(
    monkeypatch: pytest.MonkeyPatch,
    should_run_core: bool,
    expected_recompute: bool,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(run_model_pipeline.runtime_support, "serving_refresh_skip_risk_engine", lambda **kwargs: (True, "cached"))
    monkeypatch.setattr(run_model_pipeline.core_reads, "core_read_backend", lambda backend: nullcontext())
    monkeypatch.setattr(
        run_model_pipeline,
        "run_refresh",
        lambda **kwargs: captured.update(kwargs) or {"status": "ok"},
    )

    out = run_model_pipeline._run_stage(
        profile="serve-refresh",
        stage="serving_refresh",
        as_of_date="2026-03-14",
        should_run_core=should_run_core,
        serving_mode="light",
        force_core=False,
        core_reason="within_interval",
        data_db=run_model_pipeline.DATA_DB,
        cache_db=run_model_pipeline.CACHE_DB,
    )

    assert out["status"] == "ok"
    assert captured["data_db"] == run_model_pipeline.DATA_DB
    assert captured["cache_db"] == run_model_pipeline.CACHE_DB
    assert captured["enforce_stable_core_package"] is (not should_run_core)
    assert captured["refresh_deep_health_diagnostics"] is expected_recompute


def test_serve_refresh_stage_fails_closed_when_current_core_package_is_not_reusable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        run_model_pipeline.runtime_support,
        "serving_refresh_skip_risk_engine",
        lambda **kwargs: (False, "core_due_within_interval"),
    )
    monkeypatch.setattr(run_model_pipeline.core_reads, "core_read_backend", lambda backend: nullcontext())
    monkeypatch.setattr(
        run_model_pipeline,
        "run_refresh",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("serve-refresh should fail before run_refresh")),
    )

    with pytest.raises(RuntimeError, match="serve-refresh requires a current stable core package"):
        run_model_pipeline._run_stage(
            profile="serve-refresh",
            stage="serving_refresh",
            as_of_date="2026-03-14",
            should_run_core=False,
            serving_mode="light",
            force_core=False,
            core_reason="within_interval",
            data_db=run_model_pipeline.DATA_DB,
            cache_db=run_model_pipeline.CACHE_DB,
        )


def test_risk_model_stage_writes_workspace_cache_without_global_path_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace_cache_db = tmp_path / "workspace_cache.db"
    workspace_data_db = tmp_path / "workspace_data.db"
    original_cache_path = str(run_model_pipeline.config.SQLITE_PATH)
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        run_model_pipeline,
        "build_factor_covariance_from_cache",
        lambda cache_db, **kwargs: (
            pd.DataFrame([[0.04]], index=["Market"], columns=["Market"]),
            0.88,
        ),
    )
    monkeypatch.setattr(
        run_model_pipeline,
        "build_specific_risk_from_cache",
        lambda cache_db, **kwargs: {"AAPL.OQ": {"specific_var": 0.01}},
    )
    monkeypatch.setattr(
        run_model_pipeline.runtime_support,
        "latest_factor_return_date",
        lambda cache_db: "2026-03-13",
    )
    monkeypatch.setattr(
        run_model_pipeline.core_reads,
        "load_source_dates",
        lambda **_kwargs: {
            "prices_asof": "2026-03-14",
            "fundamentals_asof": "2026-02-28",
            "classification_asof": "2026-02-28",
        },
    )
    monkeypatch.setattr(
        run_model_pipeline.model_outputs,
        "persist_model_outputs",
        lambda **kwargs: captured.update(kwargs) or {"status": "ok", "authority_store": "sqlite"},
    )

    out = run_model_pipeline._run_stage(
        profile="cold-core",
        stage="risk_model",
        as_of_date="2026-03-14",
        should_run_core=True,
        serving_mode="full",
        force_core=False,
        core_reason="due",
        data_db=workspace_data_db,
        cache_db=workspace_cache_db,
    )

    assert out["status"] == "ok"
    assert str(run_model_pipeline.config.SQLITE_PATH) == original_cache_path
    assert run_model_pipeline.sqlite.cache_get_live("risk_engine_meta", db_path=workspace_cache_db)["status"] == "ok"
    assert run_model_pipeline.sqlite.cache_get_live("risk_engine_cov", db_path=workspace_cache_db)["factors"] == ["Market"]
    assert out["model_outputs_write"]["status"] == "ok"
    assert captured["data_db"] == workspace_data_db
    assert captured["cache_db"] == workspace_cache_db
    assert captured["refresh_mode"] == "cold-core"
    assert captured["source_dates"]["prices_asof"] == "2026-03-14"


def test_explicit_neon_core_window_fails_without_neon_readiness(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_model_pipeline.config, "NEON_AUTHORITATIVE_REBUILDS", True)
    monkeypatch.setattr(run_model_pipeline.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(run_model_pipeline, "DATA_DB", tmp_path / "data.db")
    monkeypatch.setattr(run_model_pipeline, "CACHE_DB", tmp_path / "cache.db")
    monkeypatch.setattr(run_model_pipeline.job_runs, "ensure_schema", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline.job_runs, "fail_stale_running_stages", lambda *args, **kwargs: 0)
    monkeypatch.setattr(run_model_pipeline.job_runs, "completed_stages", lambda *args, **kwargs: set())
    monkeypatch.setattr(run_model_pipeline.job_runs, "begin_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline.job_runs, "heartbeat_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline.job_runs, "finish_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline.job_runs, "run_rows", lambda *args, **kwargs: [])
    monkeypatch.setattr(run_model_pipeline, "mark_refresh_finished", lambda **kwargs: None)

    out = run_model_pipeline.run_model_pipeline(
        profile="core-weekly",
        as_of_date="2026-03-14",
        from_stage="factor_returns",
        to_stage="risk_model",
        force_core=True,
    )

    assert out["status"] == "failed"
    assert out["stage_results"][0]["stage"] == "factor_returns"
    assert "neon_readiness" in out["stage_results"][0]["error"]["message"]


def test_reset_core_caches_clears_core_tables(tmp_path: Path) -> None:
    cache_db = tmp_path / "cache.db"
    conn = sqlite3.connect(str(cache_db))
    conn.execute("CREATE TABLE daily_factor_returns (date TEXT, factor_name TEXT, factor_return REAL)")
    conn.execute("CREATE TABLE daily_specific_residuals (date TEXT, ric TEXT, residual REAL)")
    conn.execute("CREATE TABLE daily_universe_eligibility_summary (date TEXT, exposure_n INTEGER)")
    conn.execute("CREATE TABLE daily_factor_returns_meta (key TEXT, value TEXT)")
    conn.execute("CREATE TABLE cache (key TEXT PRIMARY KEY, value TEXT, updated_at REAL)")
    conn.execute("INSERT INTO daily_factor_returns VALUES ('2026-03-03', 'Liquidity', 0.0)")
    conn.execute("INSERT INTO daily_specific_residuals VALUES ('2026-03-03', 'AAPL.OQ', 0.01)")
    conn.execute("INSERT INTO daily_universe_eligibility_summary VALUES ('2026-03-03', 100)")
    conn.execute("INSERT INTO daily_factor_returns_meta VALUES ('method_version', 'v1')")
    conn.execute("INSERT INTO cache VALUES ('risk_engine_cov', '{}', 0)")
    conn.execute("INSERT INTO cache VALUES ('risk_engine_specific_risk', '{}', 0)")
    conn.execute("INSERT INTO cache VALUES ('risk_engine_meta', '{}', 0)")
    conn.execute("INSERT INTO cache VALUES ('unrelated', '{}', 0)")
    conn.commit()
    conn.close()

    summary = run_model_pipeline.runtime_support.reset_core_caches(cache_db)

    assert summary["daily_factor_returns"] == 1
    assert summary["daily_specific_residuals"] == 1
    assert summary["daily_universe_eligibility_summary"] == 1
    assert summary["daily_factor_returns_meta"] == 1
    assert summary["cache_risk_engine_keys"] == 3

    conn = sqlite3.connect(str(cache_db))
    assert conn.execute("SELECT COUNT(*) FROM daily_factor_returns").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM daily_specific_residuals").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM daily_universe_eligibility_summary").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM daily_factor_returns_meta").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM cache WHERE key='unrelated'").fetchone()[0] == 1
    conn.close()


def test_serving_refresh_skip_risk_engine_requires_current_method(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_model_pipeline.runtime_support, "risk_cache_ready", lambda **_kwargs: True)
    monkeypatch.setattr(
        run_model_pipeline.runtime_support.analytics_pipeline,
        "_resolve_effective_risk_engine_meta",
        lambda **kwargs: ({"method_version": "stale", "last_recompute_date": "2026-03-01"}, "runtime_state"),
    )
    monkeypatch.setattr(
        run_model_pipeline.runtime_support,
        "risk_recompute_due",
        lambda meta, **_kwargs: (True, "method_version_change"),
    )

    skip, reason = run_model_pipeline.runtime_support.serving_refresh_skip_risk_engine(
        today_utc=run_model_pipeline.date(2026, 3, 14)
    )

    assert skip is False
    assert reason == "core_due_method_version_change"


def test_serving_refresh_skip_risk_engine_allows_current_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_model_pipeline.runtime_support, "risk_cache_ready", lambda **_kwargs: True)
    monkeypatch.setattr(
        run_model_pipeline.runtime_support.analytics_pipeline,
        "_resolve_effective_risk_engine_meta",
        lambda **kwargs: ({"method_version": "current", "last_recompute_date": "2026-03-13"}, "model_run_metadata"),
    )
    monkeypatch.setattr(
        run_model_pipeline.runtime_support,
        "risk_recompute_due",
        lambda meta, **_kwargs: (False, "within_interval"),
    )

    skip, reason = run_model_pipeline.runtime_support.serving_refresh_skip_risk_engine(
        today_utc=run_model_pipeline.date(2026, 3, 14)
    )

    assert skip is True
    assert reason == "risk_cache_current"


def test_serving_refresh_skip_risk_engine_prefers_persisted_model_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_model_pipeline.runtime_support, "risk_cache_ready", lambda **_kwargs: True)
    monkeypatch.setattr(
        run_model_pipeline.runtime_support.analytics_pipeline,
        "_resolve_effective_risk_engine_meta",
        lambda **kwargs: (
            {
                "method_version": "current",
                "last_recompute_date": "2026-03-16",
                "factor_returns_latest_date": "2026-03-13",
            },
            "model_run_metadata",
        ),
    )
    monkeypatch.setattr(
        run_model_pipeline.runtime_support,
        "risk_recompute_due",
        lambda meta, **_kwargs: (False, "within_interval"),
    )

    skip, reason = run_model_pipeline.runtime_support.serving_refresh_skip_risk_engine(
        today_utc=run_model_pipeline.date(2026, 3, 16)
    )

    assert skip is True
    assert reason == "risk_cache_current"


def test_get_refresh_status_reconciles_orphaned_running_state(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class _FakeLock:
        def __init__(self) -> None:
            self._locked = True

        def locked(self) -> bool:
            return self._locked

        def release(self) -> None:
            self._locked = False

    monkeypatch.setattr(refresh_manager, "_persist_state", lambda *_args, **_kwargs: {"status": "ok"})
    monkeypatch.setattr(refresh_manager, "mark_refresh_finished", lambda **kwargs: captured.update(kwargs))
    monkeypatch.setattr(refresh_manager, "_RUN_LOCK", _FakeLock())
    monkeypatch.setattr(refresh_manager, "_ACTIVE_WORKER", None)
    monkeypatch.setattr(refresh_manager, "_STATE_LOADED", True)
    monkeypatch.setattr(
        refresh_manager,
        "_STATE",
        {
            **refresh_manager._default_state(),
            "status": "running",
            "profile": "serve-refresh",
            "pipeline_run_id": "api_orphaned",
        },
    )

    status = refresh_manager.get_refresh_status()

    assert status["status"] == "unknown"
    assert status["error"]["type"] == "refresh_worker_missing"
    assert captured["status"] == "unknown"
    assert captured["run_id"] == "api_orphaned"
