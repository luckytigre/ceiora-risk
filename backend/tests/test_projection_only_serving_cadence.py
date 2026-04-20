from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path

import pandas as pd
import pytest
import sqlite3

from backend.analytics import pipeline
from backend.data import model_outputs
from backend.orchestration.stage_serving import run_serving_stage
from backend.risk_model.projected_loadings import ProjectedLoadingResult


class _StopRefresh(RuntimeError):
    pass


class _CoreReadsModule:
    @staticmethod
    def core_read_backend(_backend: str):
        return nullcontext()


def test_run_serving_stage_requests_projection_refresh_on_core_lane() -> None:
    captured: dict[str, object] = {}

    out = run_serving_stage(
        stage="serving_refresh",
        should_run_core=True,
        serving_mode="light",
        data_db=Path("/tmp/data.db"),
        cache_db=Path("/tmp/cache.db"),
        progress_callback=None,
        core_reads_module=_CoreReadsModule,
        serving_refresh_skip_risk_engine_fn=lambda **kwargs: (True, "orchestrator_precomputed"),
        run_refresh_fn=lambda **kwargs: captured.update(kwargs) or {"status": "ok"},
        previous_or_same_xnys_session_fn=lambda s: s,
        canonical_data_db=Path("/tmp/data.db"),
        canonical_cache_db=Path("/tmp/cache.db"),
    )

    assert out["status"] == "ok"
    assert captured["refresh_projected_loadings"] is True
    assert out["metrics"]["compute_seconds"] >= 0.0
    assert out["metrics"]["rows_written"] == 0


def test_run_serving_stage_does_not_request_projection_refresh_on_serving_only_lane() -> None:
    captured: dict[str, object] = {}

    out = run_serving_stage(
        stage="serving_refresh",
        should_run_core=False,
        serving_mode="light",
        data_db=Path("/tmp/data.db"),
        cache_db=Path("/tmp/cache.db"),
        progress_callback=None,
        core_reads_module=_CoreReadsModule,
        serving_refresh_skip_risk_engine_fn=lambda **kwargs: (True, "stable_core_package_reused"),
        run_refresh_fn=lambda **kwargs: captured.update(kwargs) or {"status": "ok"},
        previous_or_same_xnys_session_fn=lambda s: s,
        canonical_data_db=Path("/tmp/data.db"),
        canonical_cache_db=Path("/tmp/cache.db"),
    )

    assert out["status"] == "ok"
    assert captured["refresh_projected_loadings"] is False


def test_run_serving_stage_exposes_published_payload_metrics() -> None:
    out = run_serving_stage(
        stage="serving_refresh",
        should_run_core=False,
        serving_mode="light",
        data_db=Path("/tmp/data.db"),
        cache_db=Path("/tmp/cache.db"),
        progress_callback=None,
        core_reads_module=_CoreReadsModule,
        serving_refresh_skip_risk_engine_fn=lambda **kwargs: (True, "stable_core_package_reused"),
        run_refresh_fn=lambda **kwargs: {"status": "ok", "published_payload_count": 42},
        previous_or_same_xnys_session_fn=lambda s: s,
        canonical_data_db=Path("/tmp/data.db"),
        canonical_cache_db=Path("/tmp/cache.db"),
    )

    assert out["status"] == "ok"
    assert out["metrics"]["rows_written"] == 42
    assert out["metrics"]["compute_seconds"] >= 0.0


def test_projection_active_asof_prefers_refresh_meta_projection_package_asof() -> None:
    payloads = {
        "refresh_meta": {
            "projection_package_asof": "2026-04-13",
            "risk_engine": {
                "core_state_through_date": "2026-03-31",
            },
        },
        "universe_loadings": {
            "by_ticker": {
                "SPY": {
                    "ticker": "SPY",
                    "projection_asof": "2026-03-31",
                }
            }
        },
    }

    assert pipeline._projection_active_asof_from_payloads(payloads) == "2026-04-13"


def test_run_refresh_uses_persisted_projection_outputs_on_serving_rebuild(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"
    source_dates = {
        "fundamentals_asof": "2026-03-13",
        "classification_asof": "2026-03-13",
        "prices_asof": "2026-03-13",
        "exposures_asof": "2026-03-13",
        "exposures_latest_available_asof": "2026-03-13",
    }
    risk_meta = {
        "status": "ok",
        "method_version": pipeline.RISK_ENGINE_METHOD_VERSION,
        "last_recompute_date": "2026-03-16",
        "factor_returns_latest_date": "2026-03-13",
        "core_state_through_date": "2026-03-13",
        "cross_section_min_age_days": 7,
        "lookback_days": 504,
        "specific_risk_ticker_count": 1,
        "recompute_interval_days": 7,
        "latest_r2": 0.4,
    }

    monkeypatch.setattr(pipeline.core_reads, "load_source_dates", lambda **kwargs: dict(source_dates))
    monkeypatch.setattr(pipeline.config, "CUSE4_ENABLE_ESTU_AUDIT", False)
    monkeypatch.setattr(
        pipeline,
        "_resolve_effective_risk_engine_meta",
        lambda **kwargs: (dict(risk_meta), "model_run_metadata"),
    )
    monkeypatch.setattr(
        pipeline.sqlite,
        "cache_get_live_first",
        lambda key, **kwargs: {
            "risk_engine_cov": {"factors": ["market"], "matrix": [[1.0]]},
            "risk_engine_specific_risk": {"AAPL.OQ": {"ticker": "AAPL", "specific_var": 0.01, "specific_vol": 0.1}},
        }.get(key),
    )
    monkeypatch.setattr(pipeline.sqlite, "cache_get", lambda key, **kwargs: None)
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_prices",
        lambda **kwargs: pd.DataFrame(
            [
                {"ticker": "AAPL", "ric": "AAPL.OQ", "close": 100.0},
                {"ticker": "SPY", "ric": "SPY.P", "close": 500.0},
            ]
        ),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_fundamentals",
        lambda **kwargs: pd.DataFrame([{"ticker": "AAPL", "ric": "AAPL.OQ", "market_cap": 1_000_000.0}]),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_raw_cross_section_latest",
        lambda **kwargs: pd.DataFrame([{"ticker": "AAPL", "ric": "AAPL.OQ", "as_of_date": "2026-03-13", "beta_score": 1.0}]),
    )
    monkeypatch.setattr(
        pipeline,
        "load_projection_only_universe_rows",
        lambda _conn: [{"ric": "SPY.P", "ticker": "SPY"}],
    )
    monkeypatch.setattr(
        pipeline,
        "latest_persisted_projection_asof",
        lambda **kwargs: "2026-03-13",
    )
    monkeypatch.setattr(
        pipeline,
        "compute_projected_loadings",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("ordinary serving rebuild should not recompute projections")),
    )
    monkeypatch.setattr(
        pipeline,
        "load_persisted_projected_loadings",
        lambda **kwargs: {
            "SPY": ProjectedLoadingResult(
                ric="SPY.P",
                ticker="SPY",
                exposures={"Market": 1.0},
                specific_var=0.01,
                specific_vol=0.1,
                r_squared=0.95,
                obs_count=252,
                lookback_days=252,
                projection_asof="2026-03-13",
                status="ok",
            )
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_build_universe_ticker_loadings",
        lambda *args, **kwargs: captured.update(kwargs) or (_ for _ in ()).throw(_StopRefresh()),
    )

    with pytest.raises(_StopRefresh):
        pipeline.run_refresh(
            data_db=data_db,
            cache_db=cache_db,
            mode="light",
            skip_snapshot_rebuild=True,
            skip_cuse4_foundation=True,
            skip_risk_engine=True,
            prefer_local_source_archive=True,
        )

    assert captured["projection_core_state_through_date"] == "2026-03-13"
    assert captured["projection_universe_rows"] == [{"ric": "SPY.P", "ticker": "SPY"}]
    assert "SPY" in captured["projected_loadings"]


def test_run_refresh_reuses_latest_persisted_projection_package_when_newer_than_core_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"
    source_dates = {
        "fundamentals_asof": "2026-03-13",
        "classification_asof": "2026-03-13",
        "prices_asof": "2026-03-13",
        "exposures_asof": "2026-03-13",
        "exposures_latest_available_asof": "2026-03-13",
    }
    risk_meta = {
        "status": "ok",
        "method_version": pipeline.RISK_ENGINE_METHOD_VERSION,
        "last_recompute_date": "2026-03-13",
        "factor_returns_latest_date": "2026-03-13",
        "core_state_through_date": "2026-03-13",
        "cross_section_min_age_days": 7,
        "lookback_days": 504,
        "specific_risk_ticker_count": 1,
        "recompute_interval_days": 7,
        "latest_r2": 0.4,
    }
    projection_rows = [{"ric": "SPY.P", "ticker": "SPY"}]

    monkeypatch.setattr(pipeline.core_reads, "load_source_dates", lambda **kwargs: dict(source_dates))
    monkeypatch.setattr(pipeline.config, "CUSE4_ENABLE_ESTU_AUDIT", False)
    monkeypatch.setattr(
        pipeline,
        "_resolve_effective_risk_engine_meta",
        lambda **kwargs: (dict(risk_meta), "model_run_metadata"),
    )
    monkeypatch.setattr(
        pipeline.sqlite,
        "cache_get_live_first",
        lambda key, **kwargs: {
            "risk_engine_cov": {"factors": ["market"], "matrix": [[1.0]]},
            "risk_engine_specific_risk": {"AAPL.OQ": {"ticker": "AAPL", "specific_var": 0.01, "specific_vol": 0.1}},
        }.get(key),
    )
    monkeypatch.setattr(pipeline.sqlite, "cache_get", lambda key, **kwargs: None)
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_prices",
        lambda **kwargs: pd.DataFrame(
            [
                {"ticker": "AAPL", "ric": "AAPL.OQ", "close": 100.0},
                {"ticker": "SPY", "ric": "SPY.P", "close": 500.0},
            ]
        ),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_fundamentals",
        lambda **kwargs: pd.DataFrame([{"ticker": "AAPL", "ric": "AAPL.OQ", "market_cap": 1_000_000.0}]),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_raw_cross_section_latest",
        lambda **kwargs: pd.DataFrame([{"ticker": "AAPL", "ric": "AAPL.OQ", "as_of_date": "2026-03-20", "beta_score": 1.0}]),
    )
    monkeypatch.setattr(pipeline, "load_projection_only_universe_rows", lambda _conn: projection_rows)
    monkeypatch.setattr(
        pipeline,
        "latest_persisted_projection_asof",
        lambda **kwargs: "2026-03-20",
    )
    monkeypatch.setattr(
        pipeline,
        "compute_projected_loadings",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("serving-only rebuild should reuse persisted projection package")),
    )

    def _load_persisted_projection_package(**kwargs):
        assert kwargs["as_of_date"] == "2026-03-20"
        return {
            "SPY": ProjectedLoadingResult(
                ric="SPY.P",
                ticker="SPY",
                exposures={"Market": 1.0},
                specific_var=0.01,
                specific_vol=0.1,
                r_squared=0.95,
                obs_count=252,
                lookback_days=252,
                projection_asof="2026-03-20",
                status="ok",
            )
        }

    monkeypatch.setattr(
        pipeline,
        "load_persisted_projected_loadings",
        _load_persisted_projection_package,
    )
    monkeypatch.setattr(
        pipeline,
        "_build_universe_ticker_loadings",
        lambda *args, **kwargs: captured.update(kwargs) or (_ for _ in ()).throw(_StopRefresh()),
    )

    with pytest.raises(_StopRefresh):
        pipeline.run_refresh(
            data_db=data_db,
            cache_db=cache_db,
            mode="light",
            skip_snapshot_rebuild=True,
            skip_cuse4_foundation=True,
            skip_risk_engine=True,
            prefer_local_source_archive=True,
        )

    assert captured["projection_core_state_through_date"] == "2026-03-20"
    assert "SPY" in captured["projected_loadings"]


def test_run_refresh_uses_authoritative_projection_scope_when_sqlite_selector_is_empty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"
    source_dates = {
        "fundamentals_asof": "2026-03-13",
        "classification_asof": "2026-03-13",
        "prices_asof": "2026-03-13",
        "exposures_asof": "2026-03-13",
        "exposures_latest_available_asof": "2026-03-13",
    }
    risk_meta = {
        "status": "ok",
        "method_version": pipeline.RISK_ENGINE_METHOD_VERSION,
        "last_recompute_date": "2026-04-13",
        "factor_returns_latest_date": "2026-04-13",
        "core_state_through_date": "2026-04-13",
        "cross_section_min_age_days": 7,
        "lookback_days": 504,
        "specific_risk_ticker_count": 1,
        "recompute_interval_days": 7,
        "latest_r2": 0.4,
    }

    monkeypatch.setattr(pipeline.core_reads, "load_source_dates", lambda **kwargs: dict(source_dates))
    monkeypatch.setattr(pipeline.config, "CUSE4_ENABLE_ESTU_AUDIT", False)
    monkeypatch.setattr(
        pipeline,
        "_resolve_effective_risk_engine_meta",
        lambda **kwargs: (dict(risk_meta), "model_run_metadata"),
    )
    monkeypatch.setattr(
        pipeline.sqlite,
        "cache_get_live_first",
        lambda key, **kwargs: {
            "risk_engine_cov": {"factors": ["market"], "matrix": [[1.0]]},
            "risk_engine_specific_risk": {"AAPL.OQ": {"ticker": "AAPL", "specific_var": 0.01, "specific_vol": 0.1}},
        }.get(key),
    )
    monkeypatch.setattr(pipeline.sqlite, "cache_get", lambda key, **kwargs: None)
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_prices",
        lambda **kwargs: pd.DataFrame(
            [
                {"ticker": "AAPL", "ric": "AAPL.OQ", "close": 100.0},
                {"ticker": "SPY", "ric": "SPY.P", "close": 500.0},
            ]
        ),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_fundamentals",
        lambda **kwargs: pd.DataFrame([{"ticker": "AAPL", "ric": "AAPL.OQ", "market_cap": 1_000_000.0}]),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_raw_cross_section_latest",
        lambda **kwargs: pd.DataFrame([{"ticker": "AAPL", "ric": "AAPL.OQ", "as_of_date": "2026-03-13", "beta_score": 1.0}]),
    )
    monkeypatch.setattr(pipeline, "load_projection_only_universe_rows", lambda _conn: [])
    monkeypatch.setattr(
        pipeline,
        "load_latest_returns_projection_scope_rows",
        lambda **kwargs: [{"ric": "SPY.P", "ticker": "SPY"}],
    )
    monkeypatch.setattr(
        pipeline,
        "latest_persisted_projection_asof",
        lambda **kwargs: "2026-04-13",
    )
    monkeypatch.setattr(
        pipeline,
        "compute_projected_loadings",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("serving-only rebuild should reuse persisted projection package")),
    )
    monkeypatch.setattr(
        pipeline,
        "load_persisted_projected_loadings",
        lambda **kwargs: {
            "SPY": ProjectedLoadingResult(
                ric="SPY.P",
                ticker="SPY",
                exposures={"Market": 1.0},
                specific_var=0.01,
                specific_vol=0.1,
                r_squared=0.95,
                obs_count=252,
                lookback_days=252,
                projection_asof="2026-04-13",
                status="ok",
            )
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_build_universe_ticker_loadings",
        lambda *args, **kwargs: captured.update(kwargs) or (_ for _ in ()).throw(_StopRefresh()),
    )

    with pytest.raises(_StopRefresh):
        pipeline.run_refresh(
            data_db=data_db,
            cache_db=cache_db,
            mode="light",
            skip_snapshot_rebuild=True,
            skip_cuse4_foundation=True,
            skip_risk_engine=True,
            prefer_local_source_archive=True,
        )

    assert captured["projection_universe_rows"] == [{"ric": "SPY.P", "ticker": "SPY"}]
    assert captured["projection_core_state_through_date"] == "2026-04-13"
    assert "SPY" in captured["projected_loadings"]


def test_run_refresh_uses_canonical_projection_rows_when_workspace_has_none(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}
    canonical_data_db = tmp_path / "canonical.db"
    workspace_data_db = tmp_path / "workspace.db"
    cache_db = tmp_path / "cache.db"
    source_dates = {
        "fundamentals_asof": "2026-03-20",
        "classification_asof": "2026-03-20",
        "prices_asof": "2026-03-20",
        "exposures_asof": "2026-03-20",
        "exposures_latest_available_asof": "2026-03-20",
    }
    risk_meta = {
        "status": "ok",
        "method_version": pipeline.RISK_ENGINE_METHOD_VERSION,
        "last_recompute_date": "2026-03-20",
        "factor_returns_latest_date": "2026-03-20",
        "core_state_through_date": "2026-03-20",
        "cross_section_min_age_days": 7,
        "lookback_days": 504,
        "specific_risk_ticker_count": 1,
        "recompute_interval_days": 7,
        "latest_r2": 0.4,
    }
    projection_rows = [{"ric": "SPY.P", "ticker": "SPY"}]

    for db_path, rows in (
        (workspace_data_db, []),
        (canonical_data_db, [("SPY.P", "SPY", "projection_only")]),
    ):
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute(
                """
                CREATE TABLE security_master (
                    ric TEXT,
                    ticker TEXT,
                    coverage_role TEXT
                )
                """
            )
            if rows:
                conn.executemany(
                    "INSERT INTO security_master (ric, ticker, coverage_role) VALUES (?, ?, ?)",
                    rows,
                )
            conn.commit()
        finally:
            conn.close()

    monkeypatch.setattr(
        pipeline,
        "_resolve_data_db",
        lambda db: workspace_data_db if db is not None else canonical_data_db,
    )
    monkeypatch.setattr(pipeline.core_reads, "load_source_dates", lambda **kwargs: dict(source_dates))
    monkeypatch.setattr(pipeline.config, "CUSE4_ENABLE_ESTU_AUDIT", False)
    monkeypatch.setattr(
        pipeline,
        "_resolve_effective_risk_engine_meta",
        lambda **kwargs: (dict(risk_meta), "model_run_metadata"),
    )
    monkeypatch.setattr(
        pipeline.sqlite,
        "cache_get_live_first",
        lambda key, **kwargs: {
            "risk_engine_cov": {"factors": ["market"], "matrix": [[1.0]]},
            "risk_engine_specific_risk": {"AAPL.OQ": {"ticker": "AAPL", "specific_var": 0.01, "specific_vol": 0.1}},
        }.get(key),
    )
    monkeypatch.setattr(pipeline.sqlite, "cache_get", lambda key, **kwargs: None)
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_prices",
        lambda **kwargs: pd.DataFrame(
            [
                {"ticker": "AAPL", "ric": "AAPL.OQ", "close": 100.0},
                {"ticker": "SPY", "ric": "SPY.P", "close": 500.0},
            ]
        ),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_fundamentals",
        lambda **kwargs: pd.DataFrame([{"ticker": "AAPL", "ric": "AAPL.OQ", "market_cap": 1_000_000.0}]),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_raw_cross_section_latest",
        lambda **kwargs: pd.DataFrame([{"ticker": "AAPL", "ric": "AAPL.OQ", "as_of_date": "2026-03-20", "beta_score": 1.0}]),
    )
    monkeypatch.setattr(model_outputs, "load_latest_rebuild_authority_risk_engine_state", lambda: {})
    monkeypatch.setattr(model_outputs, "load_latest_rebuild_authority_covariance_payload", lambda: {})
    monkeypatch.setattr(model_outputs, "load_latest_rebuild_authority_specific_risk_payload", lambda: {})
    monkeypatch.setattr(pipeline, "latest_persisted_projection_asof", lambda **kwargs: "2026-03-20")
    monkeypatch.setattr(
        pipeline,
        "load_persisted_projected_loadings",
        lambda **kwargs: (
            {}
            if kwargs.get("data_db") == workspace_data_db
            else {
                "SPY": ProjectedLoadingResult(
                    ric="SPY.P",
                    ticker="SPY",
                    exposures={"Market": 1.0},
                    specific_var=0.01,
                    specific_vol=0.1,
                    r_squared=0.95,
                    obs_count=252,
                    lookback_days=252,
                    projection_asof="2026-03-20",
                    status="ok",
                )
            }
        ),
    )
    monkeypatch.setattr(pipeline, "compute_projected_loadings", lambda **kwargs: {})

    monkeypatch.setattr(
        pipeline,
        "_build_universe_ticker_loadings",
        lambda *args, **kwargs: captured.update(kwargs) or (_ for _ in ()).throw(_StopRefresh()),
    )

    with pytest.raises(_StopRefresh):
        pipeline.run_refresh(
            data_db=workspace_data_db,
            cache_db=cache_db,
            mode="light",
            skip_snapshot_rebuild=True,
            skip_cuse4_foundation=True,
            skip_risk_engine=True,
            prefer_local_source_archive=True,
        )

    assert captured["projection_universe_rows"] == projection_rows
    assert "SPY" in captured["projected_loadings"]


def test_validate_projection_only_serving_outputs_raises_on_native_downgrade() -> None:
    with pytest.raises(RuntimeError, match="Projection-only serving payload integrity failed"):
        pipeline._validate_projection_only_serving_outputs(
            projection_ok_tickers={"SPY"},
            universe_by_ticker={
                "SPY": {
                    "ticker": "SPY",
                    "model_status": "ineligible",
                    "exposure_origin": "native",
                }
            },
            positions=[
                {
                    "ticker": "SPY",
                    "model_status": "ineligible",
                    "exposure_origin": "native",
                }
            ],
        )


def test_publish_only_refresh_fails_when_projection_only_ticker_is_downgraded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"
    payloads = {
        "portfolio": {
            "positions": [
                {
                    "ticker": "SPY",
                    "model_status": "ineligible",
                    "exposure_origin": "native",
                }
            ],
            "position_count": 1,
            "total_value": 100.0,
        },
        "risk": {
            "risk_engine": {
                "core_state_through_date": "2026-03-20",
            }
        },
        "refresh_meta": {
            "risk_engine": {
                "core_state_through_date": "2026-03-20",
            }
        },
        "health_diagnostics": {"diagnostics_refresh_state": "carried_forward"},
        "universe_loadings": {
            "by_ticker": {
                "SPY": {
                    "ticker": "SPY",
                    "model_status": "ineligible",
                    "exposure_origin": "native",
                }
            }
        },
    }

    monkeypatch.setattr(
        pipeline.publish_payloads,
        "load_publishable_payloads",
        lambda **kwargs: (dict(payloads), []),
    )
    monkeypatch.setattr(
        pipeline.publish_payloads,
        "restamp_publishable_payloads",
        lambda payloads, **kwargs: dict(payloads),
    )
    monkeypatch.setattr(
        pipeline,
        "load_projection_only_universe_rows",
        lambda _conn: [{"ric": "SPY.P", "ticker": "SPY"}],
    )
    monkeypatch.setattr(
        pipeline,
        "load_persisted_projected_loadings",
        lambda **kwargs: {
            "SPY": ProjectedLoadingResult(
                ric="SPY.P",
                ticker="SPY",
                exposures={"Market": 1.0},
                specific_var=0.01,
                specific_vol=0.1,
                r_squared=0.95,
                obs_count=252,
                lookback_days=252,
                projection_asof="2026-03-20",
                status="ok",
            )
        },
    )
    monkeypatch.setattr(
        pipeline.serving_outputs,
        "persist_current_payloads",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("publish should fail before persistence")),
    )

    with pytest.raises(RuntimeError, match="Projection-only serving payload integrity failed"):
        pipeline.run_refresh(
            data_db=data_db,
            cache_db=cache_db,
            mode="publish",
        )
