from __future__ import annotations

import importlib
from pathlib import Path

import pandas as pd
import pytest

from backend.analytics import pipeline
from backend.portfolio import positions_store
from backend.services import holdings_runtime_state
from backend.services import refresh_manager

run_model_pipeline_module = importlib.import_module("backend.orchestration.run_model_pipeline")


class _StopRefresh(Exception):
    pass


def test_positions_store_prefers_neon_when_dsn_present(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(positions_store.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(positions_store.config, "NEON_DATABASE_URL", "postgres://example")
    monkeypatch.setattr(
        positions_store,
        "_load_positions_from_neon",
        lambda: ({"AAPL": 10.0}, {"AAPL": {"account": "MAIN", "sleeve": "NEON HOLDINGS", "source": "NEON"}}),
    )

    shares, meta = positions_store._load_positions()

    assert shares == {"AAPL": 10.0}
    assert meta["AAPL"]["source"] == "NEON"


def test_positions_store_mock_fallback_without_neon(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(positions_store.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(positions_store.config, "NEON_DATABASE_URL", "")
    monkeypatch.setattr(
        positions_store,
        "_load_positions_from_neon",
        lambda: (_ for _ in ()).throw(RuntimeError("should not call neon")),
    )

    shares, meta = positions_store._load_positions()

    assert shares == positions_store.PORTFOLIO_POSITIONS
    assert meta == positions_store.POSITION_META


def test_holdings_runtime_state_round_trip(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(holdings_runtime_state, "cache_get", lambda key: None if key != "holdings_sync_state" else None)
    recorded: dict[str, object] = {}

    def _capture(_key: str, value: object) -> None:
        recorded["value"] = value

    monkeypatch.setattr(holdings_runtime_state, "cache_set", _capture)

    dirty = holdings_runtime_state.mark_holdings_dirty(
        action="holdings_import:replace_account",
        account_id="main",
        summary="replace import applied",
        import_batch_id="batch_1",
        change_count=3,
    )
    assert dirty["pending"] is True
    assert dirty["pending_count"] == 3
    assert dirty["last_import_batch_id"] == "batch_1"

    monkeypatch.setattr(holdings_runtime_state, "cache_get", lambda key: recorded.get("value"))
    holdings_runtime_state.mark_refresh_started(profile="serve-refresh", run_id="run_1")
    clean = holdings_runtime_state.mark_refresh_finished(
        profile="serve-refresh",
        run_id="run_1",
        status="ok",
        message="Serving outputs refreshed",
        clear_pending=True,
    )
    assert clean["pending"] is False
    assert clean["pending_count"] == 0
    assert clean["dirty_since"] is None
    assert clean["last_refresh_profile"] == "serve-refresh"


def test_holdings_runtime_state_does_not_clear_newer_dirty_revision(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: dict[str, object] = {}

    monkeypatch.setattr(holdings_runtime_state, "cache_get", lambda key: recorded.get("value"))
    monkeypatch.setattr(holdings_runtime_state, "cache_set", lambda _key, value: recorded.update({"value": value}))

    holdings_runtime_state.mark_holdings_dirty(
        action="holdings_position_edit",
        account_id="main",
        summary="first edit",
        import_batch_id="batch_1",
        change_count=1,
    )
    holdings_runtime_state.mark_refresh_started(profile="serve-refresh", run_id="run_1")
    second_dirty = holdings_runtime_state.mark_holdings_dirty(
        action="holdings_position_edit",
        account_id="main",
        summary="second edit",
        import_batch_id="batch_2",
        change_count=1,
    )
    finished = holdings_runtime_state.mark_refresh_finished(
        profile="serve-refresh",
        run_id="run_1",
        status="ok",
        message="Serving outputs refreshed",
        clear_pending=True,
    )

    assert second_dirty["dirty_revision"] == 2
    assert finished["pending"] is True
    assert finished["pending_count"] == 2


def test_pipeline_prefers_fundamentals_asof(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        pipeline,
        "rebuild_cross_section_snapshot",
        lambda *args, **kwargs: {"status": "ok", "mode": "current"},
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_source_dates",
        lambda: {
            "fundamentals_asof": "2026-02-27",
            "classification_asof": "2026-03-01",
            "prices_asof": "2026-03-07",
            "exposures_asof": "2026-03-07",
        },
    )
    monkeypatch.setattr(pipeline.core_reads, "load_latest_prices", lambda: pd.DataFrame())

    def _load_latest_fundamentals(*, as_of_date: str | None = None, tickers=None):
        captured["as_of_date"] = as_of_date
        return pd.DataFrame()

    monkeypatch.setattr(pipeline.core_reads, "load_latest_fundamentals", _load_latest_fundamentals)
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_raw_cross_section_latest",
        lambda *args, **kwargs: (_ for _ in ()).throw(_StopRefresh()),
    )
    monkeypatch.setattr(pipeline.config, "CUSE4_ENABLE_ESTU_AUDIT", False)

    with pytest.raises(_StopRefresh):
        pipeline.run_refresh(mode="light")

    assert captured["as_of_date"] == "2026-02-27"


def test_run_model_pipeline_clears_pending_after_serving_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(run_model_pipeline_module.job_runs, "ensure_schema", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "completed_stages", lambda *args, **kwargs: set())
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "begin_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "finish_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "run_rows", lambda *args, **kwargs: [])
    monkeypatch.setattr(run_model_pipeline_module.sqlite, "cache_get", lambda key: {})
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


def test_refresh_manager_marks_holdings_failure_on_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(refresh_manager, "run_model_pipeline", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(refresh_manager, "_set_state", lambda **kwargs: kwargs)
    monkeypatch.setattr(refresh_manager, "mark_refresh_finished", lambda **kwargs: captured.update(kwargs))

    class _FakeLock:
        def release(self) -> None:
            return None

    monkeypatch.setattr(refresh_manager, "_RUN_LOCK", _FakeLock())

    refresh_manager._run_in_background(
        job_id="abc123",
        profile="serve-refresh",
        mode="light",
        as_of_date=None,
        resume_run_id=None,
        from_stage=None,
        to_stage=None,
        force_core=False,
    )

    assert captured["status"] == "failed"
    assert captured["profile"] == "serve-refresh"
    assert captured["clear_pending"] is False


def test_start_refresh_marks_holdings_failure_if_worker_start_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(refresh_manager, "_set_state", lambda **kwargs: kwargs)
    monkeypatch.setattr(refresh_manager, "mark_refresh_started", lambda **kwargs: None)
    monkeypatch.setattr(refresh_manager, "mark_refresh_finished", lambda **kwargs: captured.update(kwargs))

    class _FakeLock:
        def acquire(self, blocking: bool = False) -> bool:
            return True

        def release(self) -> None:
            return None

    class _BrokenThread:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def start(self) -> None:
            raise RuntimeError("thread start failed")

    monkeypatch.setattr(refresh_manager, "_RUN_LOCK", _FakeLock())
    monkeypatch.setattr(refresh_manager.threading, "Thread", _BrokenThread)

    started, state = refresh_manager.start_refresh(
        mode="light",
        force_risk_recompute=False,
    )

    assert started is False
    assert state["status"] == "failed"
    assert captured["status"] == "failed"
    assert captured["profile"] == "serve-refresh"
    assert captured["clear_pending"] is False
