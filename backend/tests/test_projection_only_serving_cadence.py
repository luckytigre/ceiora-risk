from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path

import pandas as pd
import pytest

from backend.analytics import pipeline
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


def test_run_refresh_recomputes_projection_outputs_when_persisted_asof_is_stale(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}
    data_db = tmp_path / "data.db"
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
    recompute_called = {"value": False}

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
        lambda **kwargs: "2026-03-13",
    )

    def _compute_projected_loadings(**kwargs):
        recompute_called["value"] = True
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

    monkeypatch.setattr(pipeline, "compute_projected_loadings", _compute_projected_loadings)
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

    assert recompute_called["value"] is True
    assert captured["projection_core_state_through_date"] == "2026-03-20"
    assert "SPY" in captured["projected_loadings"]
