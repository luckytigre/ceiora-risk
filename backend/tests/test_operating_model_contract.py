from __future__ import annotations

import importlib
from contextlib import nullcontext
from pathlib import Path

import pandas as pd
import pytest

from backend.analytics import pipeline
from backend.services import refresh_manager

refresh_execution_module = importlib.import_module("backend.orchestration.refresh_execution")
run_model_pipeline_module = importlib.import_module("backend.orchestration.run_model_pipeline")


class _StopRefresh(Exception):
    pass


@pytest.fixture(autouse=True)
def _stub_persisted_model_state_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        pipeline.model_outputs,
        "load_latest_rebuild_authority_risk_engine_state",
        lambda: {},
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "load_latest_rebuild_authority_covariance_payload",
        lambda: {},
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "load_latest_rebuild_authority_specific_risk_payload",
        lambda: {},
    )


def test_pipeline_universe_loadings_wrapper_forwards_factor_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    sentinel_catalog = {"Market": object()}

    monkeypatch.setattr(
        pipeline,
        "_build_universe_ticker_loadings_impl",
        lambda exposures_df, fundamentals_df, prices_df, cov, **kwargs: captured.update(kwargs) or {
            "ticker_count": 0,
            "eligible_ticker_count": 0,
            "by_ticker": {},
        },
    )

    out = pipeline._build_universe_ticker_loadings(
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        specific_risk_by_ticker={"AAPL.OQ": {"specific_var": 0.01}},
        factor_catalog_by_name=sentinel_catalog,
    )

    assert out["ticker_count"] == 0
    assert captured["data_db"] == pipeline.DATA_DB
    assert captured["specific_risk_by_ticker"] == {"AAPL.OQ": {"specific_var": 0.01}}
    assert captured["factor_catalog_by_name"] is sentinel_catalog
def test_pipeline_prefers_fundamentals_asof(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}
    risk_meta = {
        "status": "ok",
        "method_version": pipeline.RISK_ENGINE_METHOD_VERSION,
        "last_recompute_date": "2026-03-07",
        "factor_returns_latest_date": "2026-03-07",
        "cross_section_min_age_days": 7,
        "lookback_days": 504,
        "specific_risk_ticker_count": 1,
        "recompute_interval_days": 7,
        "latest_r2": 0.4,
    }

    monkeypatch.setattr(
        pipeline,
        "rebuild_cross_section_snapshot",
        lambda *args, **kwargs: {"status": "ok", "mode": "current"},
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_source_dates",
        lambda **kwargs: {
            "fundamentals_asof": "2026-02-27",
            "classification_asof": "2026-03-01",
            "prices_asof": "2026-03-07",
            "exposures_asof": "2026-03-07",
        },
    )
    monkeypatch.setattr(pipeline.core_reads, "load_latest_prices", lambda **kwargs: pd.DataFrame())

    def _load_latest_fundamentals(*, as_of_date: str | None = None, tickers=None, data_db=None):
        captured["as_of_date"] = as_of_date
        return pd.DataFrame()

    monkeypatch.setattr(pipeline.core_reads, "load_latest_fundamentals", _load_latest_fundamentals)
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_raw_cross_section_latest",
        lambda *args, **kwargs: (_ for _ in ()).throw(_StopRefresh()),
    )
    monkeypatch.setattr(pipeline.config, "CUSE4_ENABLE_ESTU_AUDIT", False)

    def _cache_get(key: str):
        payloads = {
            "risk_engine_meta": dict(risk_meta),
            "risk_engine_cov": {"factors": ["style_beta_score"], "matrix": [[1.0]]},
            "risk_engine_specific_risk": {
                "AAPL.OQ": {
                    "ticker": "AAPL",
                    "specific_var": 0.01,
                    "specific_vol": 0.1,
                }
            },
        }
        return payloads.get(key)

    monkeypatch.setattr(pipeline.sqlite, "cache_get_live_first", lambda key, **kwargs: _cache_get(key))
    monkeypatch.setattr(pipeline.sqlite, "cache_get", lambda key, **kwargs: _cache_get(key))
    monkeypatch.setattr(
        pipeline.runtime_state,
        "load_runtime_state",
        lambda key, fallback_loader=None: _cache_get(key),
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "load_latest_rebuild_authority_risk_engine_state",
        lambda: dict(risk_meta),
    )
    monkeypatch.setattr(
        pipeline,
        "compute_daily_factor_returns",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not recompute risk engine")),
    )
    monkeypatch.setattr(
        pipeline,
        "build_factor_covariance_from_cache",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not recompute risk engine")),
    )
    monkeypatch.setattr(
        pipeline,
        "build_specific_risk_from_cache",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not recompute risk engine")),
    )

    with pytest.raises(_StopRefresh):
        pipeline.run_refresh(mode="light")

    assert captured["as_of_date"] == "2026-02-27"


def test_pipeline_can_reuse_cached_universe_loadings_for_holdings_only_light_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    source_dates = {
        "fundamentals_asof": "2026-02-27",
        "classification_asof": "2026-03-01",
        "prices_asof": "2026-03-07",
        "exposures_asof": "2026-03-07",
    }
    risk_meta = {
        "status": "ok",
        "method_version": pipeline.RISK_ENGINE_METHOD_VERSION,
        "last_recompute_date": "2026-03-07",
        "factor_returns_latest_date": "2026-03-07",
        "cross_section_min_age_days": 7,
        "lookback_days": 504,
        "specific_risk_ticker_count": 1,
        "recompute_interval_days": 7,
        "latest_r2": 0.4,
    }
    cached_universe_loadings = {
        "factors": ["style_beta_score"],
        "factor_vols": {"style_beta_score": 0.05},
        "ticker_count": 1,
        "eligible_ticker_count": 1,
        "source_dates": dict(source_dates),
        "risk_engine": {
            "status": "ok",
            "method_version": pipeline.RISK_ENGINE_METHOD_VERSION,
            "last_recompute_date": "2026-03-07",
            "factor_returns_latest_date": "2026-03-07",
            "cross_section_min_age_days": 7,
            "lookback_days": 504,
            "specific_risk_ticker_count": 1,
        },
        "by_ticker": {
            "AAPL": {
                "ticker": "AAPL",
                "price": 100.0,
                "weight": 1.0,
                "name": "Apple",
                "exposures": {"style_beta_score": 1.1},
                "specific_var": 0.01,
                "specific_vol": 0.1,
                "model_status": "core_estimated",
            }
        },
    }

    monkeypatch.setattr(
        pipeline.core_reads,
        "load_source_dates",
        lambda **kwargs: dict(source_dates),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_prices",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not load latest prices")),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_fundamentals",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not load latest fundamentals")),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_raw_cross_section_latest",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not load latest exposures")),
    )
    monkeypatch.setattr(pipeline.config, "CUSE4_ENABLE_ESTU_AUDIT", False)

    def _cache_get(key: str):
        payloads = {
            "risk_engine_meta": dict(risk_meta),
            "risk_engine_cov": {"factors": ["style_beta_score"], "matrix": [[1.0]]},
            "risk_engine_specific_risk": {
                "AAPL.OQ": {
                    "ticker": "AAPL",
                    "specific_var": 0.01,
                    "specific_vol": 0.1,
                }
            },
            "risk": {
                "cov_matrix": {"factors": ["style_beta_score"], "correlation": [[1.0]]},
            },
            "universe_loadings": dict(cached_universe_loadings),
        }
        return payloads.get(key)

    monkeypatch.setattr(pipeline.sqlite, "cache_get_live", lambda key, **kwargs: _cache_get(key))
    monkeypatch.setattr(pipeline.sqlite, "cache_get", lambda key, **kwargs: _cache_get(key))
    monkeypatch.setattr(pipeline.sqlite, "cache_get_live_first", lambda key, **kwargs: _cache_get(key))
    monkeypatch.setattr(
        pipeline.runtime_state,
        "load_runtime_state",
        lambda key, fallback_loader=None: _cache_get(key),
    )
    monkeypatch.setattr(
        pipeline,
        "_build_positions_from_universe",
        lambda by_ticker: ([{"ticker": "AAPL", "weight": 1.0, "exposures": {"style_beta_score": 1.1}}], 100.0),
    )
    monkeypatch.setattr(
        pipeline,
        "risk_decomposition",
        lambda **kwargs: (
            {"market": 0.0, "industry": 10.0, "style": 20.0, "idio": 70.0},
            {"market": 0.0, "industry": 0.1, "style": 0.2},
            [{"factor_id": "style_beta_score", "sensitivity": 0.3, "factor_vol": 0.05}],
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "_compute_position_risk_mix",
        lambda **kwargs: {"AAPL": {"market": 0.0, "industry": 0.2, "style": 0.3, "idio": 0.5}},
    )
    monkeypatch.setattr(pipeline, "_load_latest_factor_coverage", lambda _cache_db, **kwargs: (None, {}))
    monkeypatch.setattr(
        pipeline,
        "_compute_exposures_modes",
        lambda *args, **kwargs: {"raw": [], "sensitivity": [], "risk_contribution": []},
    )
    monkeypatch.setattr(
        pipeline,
        "stage_refresh_cache_snapshot",
        lambda **kwargs: captured.update({
            "staged_universe_loadings": kwargs["universe_loadings"],
            "reuse_cached_static_payloads": kwargs["reuse_cached_static_payloads"],
        }) or {
            "snapshot_id": "snap_1",
            "risk_engine_state": {"status": "ok"},
            "sanity": {"status": "ok"},
            "health_refreshed": False,
            "persisted_payloads": {},
        },
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "persist_model_outputs",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not persist model outputs on holdings-only fast path")),
    )
    monkeypatch.setattr(
        pipeline.serving_outputs,
        "persist_current_payloads",
        lambda **kwargs: {"status": "ok", "run_id": kwargs["run_id"]},
    )
    monkeypatch.setattr(pipeline.sqlite, "cache_set", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        pipeline.runtime_state,
        "publish_active_snapshot",
        lambda snapshot_id, fallback_publisher=None: captured.update({"published": snapshot_id}) or {"status": "ok"},
    )

    out = pipeline.run_refresh(
        mode="light",
        refresh_scope="full",
        skip_snapshot_rebuild=True,
        skip_cuse4_foundation=True,
        skip_risk_engine=True,
    )

    assert out["status"] == "ok"
    assert out["universe_loadings_reused"] is True
    assert out["universe_loadings_reuse_reason"] == "source_and_risk_engine_match"
    assert out["model_outputs_write"]["status"] == "skipped"
    assert out["model_outputs_write"]["reason"] == "risk_engine_reused"
    assert captured["reuse_cached_static_payloads"] is True
    assert captured["published"] == "snap_1"
    assert isinstance(captured["staged_universe_loadings"], dict)


def test_light_refresh_can_fail_closed_when_stable_core_package_is_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_source_dates",
        lambda **kwargs: {
            "fundamentals_asof": "2026-02-27",
            "classification_asof": "2026-02-27",
            "prices_asof": "2026-03-07",
            "exposures_asof": "2026-03-07",
        },
    )
    monkeypatch.setattr(pipeline.config, "CUSE4_ENABLE_ESTU_AUDIT", False)
    monkeypatch.setattr(
        pipeline,
        "_resolve_effective_risk_engine_meta",
        lambda **kwargs: ({}, "runtime_state"),
    )
    monkeypatch.setattr(pipeline.sqlite, "cache_get_live_first", lambda key, **kwargs: None)
    monkeypatch.setattr(
        pipeline,
        "compute_daily_factor_returns",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("light refresh should fail before recomputing factor returns")),
    )

    with pytest.raises(RuntimeError, match="stable core package"):
        pipeline.run_refresh(
            mode="light",
            skip_snapshot_rebuild=True,
            skip_cuse4_foundation=True,
            enforce_stable_core_package=True,
        )


def test_pipeline_rebuilds_universe_loadings_when_local_source_archive_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
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
        "cross_section_min_age_days": 7,
        "lookback_days": 504,
        "specific_risk_ticker_count": 1,
        "recompute_interval_days": 7,
        "latest_r2": 0.4,
    }
    cached_universe_loadings = {
        "factors": ["style_beta_score"],
        "factor_vols": {"style_beta_score": 0.05},
        "ticker_count": 1,
        "eligible_ticker_count": 1,
        "source_dates": dict(source_dates),
        "risk_engine": {
            "status": "ok",
            "method_version": pipeline.RISK_ENGINE_METHOD_VERSION,
            "last_recompute_date": "2026-03-16",
            "factor_returns_latest_date": "2026-03-13",
            "cross_section_min_age_days": 7,
            "lookback_days": 504,
            "specific_risk_ticker_count": 1,
        },
        "by_ticker": {
            "AAPL": {
                "ticker": "AAPL",
                "price": 100.0,
                "name": "Apple",
                "exposures": {},
                "specific_var": 0.01,
                "specific_vol": 0.1,
                "model_status": "ineligible",
            }
        },
    }

    monkeypatch.setattr(pipeline.core_reads, "load_source_dates", lambda **kwargs: dict(source_dates))
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_prices",
        lambda **kwargs: pd.DataFrame([{"ticker": "AAPL", "ric": "AAPL.OQ", "close": 100.0}]),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_fundamentals",
        lambda **kwargs: pd.DataFrame([{"ticker": "AAPL", "ric": "AAPL.OQ", "market_cap": 1_000_000.0}]),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_raw_cross_section_latest",
        lambda **kwargs: pd.DataFrame([{"ticker": "AAPL", "ric": "AAPL.OQ", "as_of_date": "2026-03-13", "beta_score": 1.1}]),
    )
    monkeypatch.setattr(pipeline.config, "CUSE4_ENABLE_ESTU_AUDIT", False)

    def _cache_get(key: str):
        payloads = {
            "risk_engine_meta": dict(risk_meta),
            "risk_engine_cov": {"factors": ["style_beta_score"], "matrix": [[1.0]]},
            "risk_engine_specific_risk": {"AAPL.OQ": {"ticker": "AAPL", "specific_var": 0.01, "specific_vol": 0.1}},
            "risk": {"cov_matrix": {"factors": ["style_beta_score"], "correlation": [[1.0]]}},
            "universe_loadings": dict(cached_universe_loadings),
        }
        return payloads.get(key)

    monkeypatch.setattr(pipeline.sqlite, "cache_get_live", lambda key, **kwargs: _cache_get(key))
    monkeypatch.setattr(pipeline.sqlite, "cache_get", lambda key, **kwargs: _cache_get(key))
    monkeypatch.setattr(pipeline.sqlite, "cache_get_live_first", lambda key, **kwargs: _cache_get(key))
    monkeypatch.setattr(pipeline.runtime_state, "load_runtime_state", lambda key, fallback_loader=None: _cache_get(key))
    monkeypatch.setattr(pipeline, "load_projection_only_universe_rows", lambda _conn: [])
    monkeypatch.setattr(
        pipeline,
        "_build_universe_ticker_loadings",
        lambda *args, **kwargs: captured.update({"rebuilt": True}) or {
            "as_of_date": "2026-03-13",
            "latest_available_asof": "2026-03-13",
            "factors": ["style_beta_score"],
            "factor_vols": {"style_beta_score": 0.05},
            "factor_catalog": [],
            "ticker_count": 1,
            "eligible_ticker_count": 1,
            "core_estimated_ticker_count": 1,
            "projected_only_ticker_count": 0,
            "ineligible_ticker_count": 0,
            "by_ticker": {
                "AAPL": {
                    "ticker": "AAPL",
                    "price": 100.0,
                    "name": "Apple",
                    "exposures": {"style_beta_score": 1.1},
                    "specific_var": 0.01,
                    "specific_vol": 0.1,
                    "model_status": "core_estimated",
                }
            },
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_build_positions_from_universe",
        lambda by_ticker: ([{"ticker": "AAPL", "weight": 1.0, "exposures": {"style_beta_score": 1.1}}], 100.0),
    )
    monkeypatch.setattr(
        pipeline,
        "risk_decomposition",
        lambda **kwargs: (
            {"market": 1.0, "industry": 10.0, "style": 20.0, "idio": 69.0},
            {"market": 0.1, "industry": 0.2, "style": 0.7},
            [{"factor_id": "style_beta_score", "sensitivity": 0.3, "factor_vol": 0.05}],
        ),
    )
    monkeypatch.setattr(pipeline, "_compute_position_risk_mix", lambda **kwargs: {"AAPL": {"market": 0.1, "industry": 0.2, "style": 0.3, "idio": 0.4}})
    monkeypatch.setattr(pipeline, "_load_latest_factor_coverage", lambda _cache_db, **kwargs: (None, {}))
    monkeypatch.setattr(pipeline, "_compute_exposures_modes", lambda *args, **kwargs: {"raw": [], "sensitivity": [], "risk_contribution": []})
    monkeypatch.setattr(
        pipeline,
        "stage_refresh_cache_snapshot",
        lambda **kwargs: captured.update({"reuse_cached_static_payloads": kwargs["reuse_cached_static_payloads"]}) or {
            "snapshot_id": "snap_local",
            "risk_engine_state": {"status": "ok"},
            "sanity": {"status": "ok"},
            "health_refreshed": False,
            "persisted_payloads": {},
        },
    )
    monkeypatch.setattr(pipeline.model_outputs, "persist_model_outputs", lambda **kwargs: {"status": "skipped"})
    monkeypatch.setattr(pipeline.serving_outputs, "persist_current_payloads", lambda **kwargs: {"status": "ok", "run_id": kwargs["run_id"]})
    monkeypatch.setattr(pipeline.sqlite, "cache_set", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        pipeline.runtime_state,
        "publish_active_snapshot",
        lambda snapshot_id, fallback_publisher=None: {"status": "ok"},
    )

    out = pipeline.run_refresh(
        mode="light",
        refresh_scope="full",
        skip_snapshot_rebuild=True,
        skip_cuse4_foundation=True,
        skip_risk_engine=True,
        prefer_local_source_archive=True,
    )

    assert out["status"] == "ok"
    assert out["universe_loadings_reused"] is False
    assert out["universe_loadings_reuse_reason"] == "rebuilt"
    assert captured["rebuilt"] is True
    assert captured["reuse_cached_static_payloads"] is False


def test_cached_universe_loadings_reuse_requires_matching_risk_engine_and_source_dates() -> None:
    ok, reason = pipeline._can_reuse_cached_universe_loadings(
        {
            "by_ticker": {"AAPL": {"ticker": "AAPL", "model_status": "core_estimated"}},
            "source_dates": {"prices_asof": "2026-03-07"},
            "risk_engine": {
                "status": "ok",
                "method_version": "v1",
                "last_recompute_date": "2026-03-07",
                "factor_returns_latest_date": "2026-03-07",
                "cross_section_min_age_days": 7,
                "lookback_days": 504,
                "specific_risk_ticker_count": 1,
            },
        },
        source_dates={"prices_asof": "2026-03-07"},
        risk_engine_meta={
            "status": "ok",
            "method_version": "v2",
            "last_recompute_date": "2026-03-07",
            "factor_returns_latest_date": "2026-03-07",
            "cross_section_min_age_days": 7,
            "lookback_days": 504,
            "specific_risk_ticker_count": 1,
        },
    )

    assert ok is False
    assert reason == "risk_engine_state_changed"


def test_cached_universe_loadings_reuse_accepts_legacy_source_dates_without_explicit_aliases() -> None:
    ok, reason = pipeline._can_reuse_cached_universe_loadings(
        {
            "by_ticker": {"AAPL": {"ticker": "AAPL", "model_status": "core_estimated"}},
            "source_dates": {
                "fundamentals_asof": "2026-03-07",
                "classification_asof": "2026-03-07",
                "prices_asof": "2026-03-07",
                "exposures_asof": "2026-03-07",
            },
            "risk_engine": {
                "status": "ok",
                "method_version": "v1",
                "last_recompute_date": "2026-03-07",
                "factor_returns_latest_date": "2026-03-07",
                "cross_section_min_age_days": 7,
                "lookback_days": 504,
                "specific_risk_ticker_count": 1,
            },
        },
        source_dates={
            "fundamentals_asof": "2026-03-07",
            "classification_asof": "2026-03-07",
            "prices_asof": "2026-03-07",
            "exposures_asof": "2026-03-07",
            "exposures_latest_available_asof": "2026-03-07",
        },
        risk_engine_meta={
            "status": "ok",
            "method_version": "v1",
            "last_recompute_date": "2026-03-07",
            "factor_returns_latest_date": "2026-03-07",
            "cross_section_min_age_days": 7,
            "lookback_days": 504,
            "specific_risk_ticker_count": 1,
        },
    )

    assert ok is True
    assert reason == "source_and_risk_engine_match"


def test_cached_universe_loadings_reuse_rejects_payload_with_no_modeled_tickers() -> None:
    ok, reason = pipeline._can_reuse_cached_universe_loadings(
        {
            "by_ticker": {
                "SPY": {
                    "ticker": "SPY",
                    "model_status": "ineligible",
                    "exposure_origin": "projected_returns",
                }
            },
            "source_dates": {"prices_asof": "2026-03-07"},
            "risk_engine": {
                "status": "ok",
                "method_version": "v1",
                "last_recompute_date": "2026-03-07",
                "factor_returns_latest_date": "2026-03-07",
                "cross_section_min_age_days": 7,
                "lookback_days": 504,
                "specific_risk_ticker_count": 1,
            },
        },
        source_dates={"prices_asof": "2026-03-07"},
        risk_engine_meta={
            "status": "ok",
            "method_version": "v1",
            "last_recompute_date": "2026-03-07",
            "factor_returns_latest_date": "2026-03-07",
            "cross_section_min_age_days": 7,
            "lookback_days": 504,
            "specific_risk_ticker_count": 1,
        },
    )

    assert ok is False
    assert reason == "no_modeled_tickers"


def test_pipeline_fallback_light_refresh_skips_model_outputs_when_risk_engine_is_reused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    source_dates = {
        "fundamentals_asof": "2026-02-27",
        "classification_asof": "2026-03-01",
        "prices_asof": "2026-03-07",
        "exposures_asof": "2026-03-07",
    }
    risk_meta = {
        "status": "ok",
        "method_version": pipeline.RISK_ENGINE_METHOD_VERSION,
        "last_recompute_date": "2026-03-07",
        "factor_returns_latest_date": "2026-03-07",
        "cross_section_min_age_days": 7,
        "lookback_days": 504,
        "specific_risk_ticker_count": 1,
        "recompute_interval_days": 7,
        "latest_r2": 0.4,
    }
    cached_universe_loadings = {
        "factors": ["style_beta_score"],
        "factor_vols": {"style_beta_score": 0.05},
        "ticker_count": 1,
        "eligible_ticker_count": 1,
        "source_dates": dict(source_dates),
        "risk_engine": {
            "status": "ok",
            "method_version": "stale_version",
            "last_recompute_date": "2026-03-07",
            "factor_returns_latest_date": "2026-03-07",
            "cross_section_min_age_days": 7,
            "lookback_days": 504,
            "specific_risk_ticker_count": 1,
        },
        "by_ticker": {
            "AAPL": {
                "ticker": "AAPL",
                "price": 100.0,
                "weight": 1.0,
                "name": "Apple",
                "exposures": {"style_beta_score": 1.1},
                "specific_var": 0.01,
                "specific_vol": 0.1,
                "model_status": "core_estimated",
            }
        },
    }

    monkeypatch.setattr(
        pipeline.core_reads,
        "load_source_dates",
        lambda **kwargs: dict(source_dates),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_prices",
        lambda **kwargs: pd.DataFrame([{"ticker": "AAPL", "ric": "AAPL.OQ", "close": 100.0}]),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_fundamentals",
        lambda **kwargs: pd.DataFrame(
            [{"ticker": "AAPL", "ric": "AAPL.OQ", "market_cap": 1000.0, "trbc_business_sector": "Technology"}]
        ),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_raw_cross_section_latest",
        lambda *args, **kwargs: pd.DataFrame([{"ticker": "AAPL", "ric": "AAPL.OQ", "beta_score": 1.1}]),
    )
    monkeypatch.setattr(pipeline.config, "CUSE4_ENABLE_ESTU_AUDIT", False)

    def _cache_get(key: str):
        payloads = {
            "risk_engine_meta": dict(risk_meta),
            "risk_engine_cov": {"factors": ["style_beta_score"], "matrix": [[1.0]]},
            "risk_engine_specific_risk": {
                "AAPL.OQ": {
                    "ticker": "AAPL",
                    "specific_var": 0.01,
                    "specific_vol": 0.1,
                }
            },
            "risk": {
                "cov_matrix": {"factors": ["style_beta_score"], "correlation": [[1.0]]},
            },
            "universe_loadings": dict(cached_universe_loadings),
        }
        return payloads.get(key)

    monkeypatch.setattr(pipeline.sqlite, "cache_get_live", lambda key, **kwargs: _cache_get(key))
    monkeypatch.setattr(pipeline.sqlite, "cache_get", lambda key, **kwargs: _cache_get(key))
    monkeypatch.setattr(pipeline.sqlite, "cache_get_live_first", lambda key, **kwargs: _cache_get(key))
    monkeypatch.setattr(
        pipeline.runtime_state,
        "load_runtime_state",
        lambda key, fallback_loader=None: _cache_get(key),
    )
    monkeypatch.setattr(
        pipeline,
        "_build_universe_ticker_loadings",
        lambda *args, **kwargs: {
            "factors": ["style_beta_score"],
            "factor_vols": {"style_beta_score": 0.05},
            "ticker_count": 1,
            "eligible_ticker_count": 1,
            "by_ticker": {"AAPL": {"ticker": "AAPL", "price": 100.0, "exposures": {"style_beta_score": 1.1}}},
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_build_positions_from_universe",
        lambda by_ticker: ([{"ticker": "AAPL", "weight": 1.0, "exposures": {"style_beta_score": 1.1}}], 100.0),
    )
    monkeypatch.setattr(
        pipeline,
        "risk_decomposition",
        lambda **kwargs: (
            {"market": 0.0, "industry": 10.0, "style": 20.0, "idio": 70.0},
            {"market": 0.0, "industry": 0.1, "style": 0.2},
            [{"factor_id": "style_beta_score", "sensitivity": 0.3, "factor_vol": 0.05}],
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "_compute_position_risk_mix",
        lambda **kwargs: {"AAPL": {"market": 0.0, "industry": 0.2, "style": 0.3, "idio": 0.5}},
    )
    monkeypatch.setattr(pipeline, "_load_latest_factor_coverage", lambda _cache_db, **kwargs: (None, {}))
    monkeypatch.setattr(
        pipeline,
        "_compute_exposures_modes",
        lambda *args, **kwargs: {"raw": [], "sensitivity": [], "risk_contribution": []},
    )
    monkeypatch.setattr(
        pipeline,
        "stage_refresh_cache_snapshot",
        lambda **kwargs: captured.update({"reuse_cached_static_payloads": kwargs["reuse_cached_static_payloads"]}) or {
            "snapshot_id": "snap_2",
            "risk_engine_state": {"status": "ok"},
            "sanity": {"status": "ok"},
            "health_refreshed": False,
            "persisted_payloads": {},
        },
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "persist_model_outputs",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("model outputs should be skipped when risk engine is reused")),
    )
    monkeypatch.setattr(
        pipeline.serving_outputs,
        "persist_current_payloads",
        lambda **kwargs: {"status": "ok", "run_id": kwargs["run_id"]},
    )
    monkeypatch.setattr(pipeline.sqlite, "cache_set", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        pipeline.runtime_state,
        "publish_active_snapshot",
        lambda snapshot_id, fallback_publisher=None: {"status": "ok"},
    )

    out = pipeline.run_refresh(
        mode="light",
        refresh_scope="holdings_only",
        skip_snapshot_rebuild=True,
        skip_cuse4_foundation=True,
        skip_risk_engine=True,
    )

    assert out["status"] == "ok"
    assert out["universe_loadings_reused"] is False
    assert out["universe_loadings_reuse_reason"] == "rebuilt"
    assert captured["reuse_cached_static_payloads"] is False
    assert out["model_outputs_write"]["status"] == "skipped"
    assert out["model_outputs_write"]["reason"] == "risk_engine_reused"


def test_run_refresh_prefers_live_risk_engine_artifacts_over_active_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    stale_meta = {
        "status": "ok",
        "method_version": "stale_snapshot",
        "last_recompute_date": "2026-03-01",
        "factor_returns_latest_date": "2026-03-01",
        "cross_section_min_age_days": 7,
        "lookback_days": 504,
        "recompute_interval_days": 7,
        "specific_risk_ticker_count": 1,
        "latest_r2": 0.11,
    }
    fresh_meta = {
        "status": "ok",
        "method_version": "fresh_live",
        "last_recompute_date": "2026-03-08",
        "factor_returns_latest_date": "2026-03-07",
        "cross_section_min_age_days": 7,
        "lookback_days": 504,
        "recompute_interval_days": 7,
        "specific_risk_ticker_count": 2,
        "latest_r2": 0.33,
    }
    fresh_cov = {"factors": ["style_beta_score"], "matrix": [[1.0]]}
    fresh_specific = {"AAPL.OQ": {"ticker": "AAPL", "specific_var": 0.01}}

    monkeypatch.setattr(
        pipeline.core_reads,
        "load_source_dates",
        lambda **kwargs: {
            "prices_asof": "2026-03-07",
            "fundamentals_asof": "2026-03-07",
            "classification_asof": "2026-03-07",
            "exposures_asof": "2026-03-07",
        },
    )
    monkeypatch.setattr(pipeline.core_reads, "load_latest_prices", lambda **kwargs: pd.DataFrame({"ticker": ["AAPL"]}))
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_fundamentals",
        lambda as_of_date=None, **kwargs: pd.DataFrame({"ticker": ["AAPL"]}),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_raw_cross_section_latest",
        lambda **kwargs: pd.DataFrame({"ticker": ["AAPL"], "beta_score": [1.0]}),
    )
    monkeypatch.setattr(
        pipeline,
        "_build_positions_from_universe",
        lambda universe_by_ticker: ([{"ticker": "AAPL", "weight": 1.0, "market_value": 100.0}], 100.0),
    )
    monkeypatch.setattr(
        pipeline,
        "_compute_position_total_risk_contributions",
        lambda *args, **kwargs: {"AAPL": 100.0},
    )
    monkeypatch.setattr(
        pipeline,
        "risk_decomposition",
        lambda *args, **kwargs: (
            {"market": 0.0, "industry": 20.0, "style": 30.0, "idio": 50.0},
            {"market": 0.0, "industry": 0.4, "style": 0.6},
            [{"factor_id": "style_beta_score", "exposure": 0.1, "sensitivity": 0.01, "factor_vol": 0.05, "pct_of_total": 3.0}],
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "_compute_position_risk_mix",
        lambda *args, **kwargs: {"AAPL": {"market": 0.0, "industry": 0.2, "style": 0.3, "idio": 0.5}},
    )
    monkeypatch.setattr(
        pipeline,
        "_build_universe_ticker_loadings",
        lambda *args, **kwargs: {
            "factors": ["style_beta_score"],
            "factor_vols": {"style_beta_score": 0.05},
            "ticker_count": 1,
            "eligible_ticker_count": 1,
            "by_ticker": {"AAPL": {"ticker": "AAPL", "price": 100.0, "exposures": {"style_beta_score": 1.0}}},
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_load_cached_risk_display_payload",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        pipeline,
        "_compute_exposures_modes",
        lambda *args, **kwargs: {"raw": [], "sensitivity": [], "risk_contribution": []},
    )
    monkeypatch.setattr(pipeline, "_load_latest_factor_coverage", lambda _cache_db, **kwargs: (None, {}))
    monkeypatch.setattr(
        pipeline,
        "stage_refresh_cache_snapshot",
        lambda **kwargs: captured.update(
            {
                "risk_engine_meta": kwargs["risk_engine_meta"],
                "recomputed_this_refresh": kwargs["recomputed_this_refresh"],
                "recompute_reason": kwargs["recompute_reason"],
            }
        )
        or {
            "snapshot_id": "snap_live_first",
            "risk_engine_state": kwargs["risk_engine_meta"],
            "sanity": {"status": "ok"},
            "health_refreshed": True,
            "persisted_payloads": {"risk": {"risk_engine": kwargs["risk_engine_meta"]}},
        },
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "persist_model_outputs",
        lambda **kwargs: {"status": "ok", "run_id": kwargs["run_id"]},
    )
    monkeypatch.setattr(
        pipeline.serving_outputs,
        "persist_current_payloads",
        lambda **kwargs: {"status": "ok", "run_id": kwargs["run_id"]},
    )
    monkeypatch.setattr(pipeline.sqlite, "cache_set", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        pipeline.runtime_state,
        "publish_active_snapshot",
        lambda snapshot_id, fallback_publisher=None: {"status": "ok"},
    )
    monkeypatch.setattr(
        pipeline.sqlite,
        "cache_get",
        lambda key, **kwargs: stale_meta if key == "risk_engine_meta" else None,
    )
    monkeypatch.setattr(
        pipeline.sqlite,
        "cache_get_live_first",
        lambda key, **kwargs: {
            "risk_engine_meta": fresh_meta,
            "risk_engine_cov": fresh_cov,
            "risk_engine_specific_risk": fresh_specific,
        }.get(key),
    )
    monkeypatch.setattr(
        pipeline.runtime_state,
        "load_runtime_state",
        lambda key, fallback_loader=None: fresh_meta if key == "risk_engine_meta" else None,
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "load_latest_rebuild_authority_risk_engine_state",
        lambda: {},
    )

    out = pipeline.run_refresh(
        mode="full",
        refresh_scope="full",
        skip_snapshot_rebuild=True,
        skip_cuse4_foundation=True,
        skip_risk_engine=True,
    )

    assert out["status"] == "ok"
    assert captured["risk_engine_meta"]["method_version"] == "fresh_live"
    assert captured["recomputed_this_refresh"] is False
    assert captured["recompute_reason"] == "orchestrator_precomputed"


def test_run_refresh_marks_upstream_core_recompute_for_publication_when_risk_engine_is_skipped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    source_dates = {
        "fundamentals_asof": "2026-02-27",
        "classification_asof": "2026-03-01",
        "prices_asof": "2026-03-07",
        "exposures_asof": "2026-03-07",
    }
    risk_meta = {
        "status": "ok",
        "method_version": pipeline.RISK_ENGINE_METHOD_VERSION,
        "last_recompute_date": "2026-03-07",
        "factor_returns_latest_date": "2026-03-07",
        "cross_section_min_age_days": 7,
        "lookback_days": 504,
        "specific_risk_ticker_count": 1,
        "recompute_interval_days": 7,
        "latest_r2": 0.4,
    }

    monkeypatch.setattr(pipeline, "bootstrap_cuse4_source_tables", lambda **kwargs: {"status": "ok"})
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_source_dates",
        lambda data_db=None: dict(source_dates),
    )
    monkeypatch.setattr(
        pipeline.refresh_context,
        "resolve_effective_risk_engine_meta",
        lambda **kwargs: (dict(risk_meta), "runtime_state"),
    )
    monkeypatch.setattr(pipeline, "_deserialize_covariance", lambda payload: pd.DataFrame({"market": [1.0]}, index=["market"]))
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_prices",
        lambda data_db=None: pd.DataFrame([{"ticker": "AAPL", "ric": "AAPL.OQ", "close": 100.0}]),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_fundamentals",
        lambda data_db=None, as_of_date=None: pd.DataFrame(
            [
                {
                    "ticker": "AAPL",
                    "ric": "AAPL.OQ",
                    "market_cap": 100.0,
                    "trbc_economic_sector_short": "Technology",
                    "trbc_business_sector": "Technology Equipment",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_raw_cross_section_latest",
        lambda data_db=None: pd.DataFrame([{"ticker": "AAPL", "ric": "AAPL.OQ", "beta_score": 1.0}]),
    )
    monkeypatch.setattr(
        pipeline,
        "_build_universe_ticker_loadings",
        lambda *args, **kwargs: {
            "as_of_date": "2026-03-07",
            "by_ticker": {
                "AAPL": {
                    "ticker": "AAPL",
                    "ric": "AAPL.OQ",
                    "as_of_date": "2026-03-07",
                    "model_status": "core_estimated",
                    "model_status_reason": "",
                    "eligibility_reason": "",
                    "exposure_origin": "native",
                    "exposures": {"market": 1.0},
                }
            },
            "factor_catalog": [],
            "factor_count": 1,
            "ticker_count": 1,
            "eligible_ticker_count": 1,
        },
    )
    monkeypatch.setattr(pipeline, "_build_positions_from_universe", lambda universe_by_ticker: ([{"ticker": "AAPL"}], 100.0))
    monkeypatch.setattr(pipeline, "risk_decomposition", lambda **kwargs: ({"market": 1.0, "industry": 0.0, "style": 0.0, "idio": 0.0}, {"market": 1.0, "industry": 0.0, "style": 0.0}, []))
    monkeypatch.setattr(pipeline, "vol_scaled_decomposition", lambda **kwargs: {"market": 1.0, "industry": 0.0, "style": 0.0, "idio": 0.0})
    monkeypatch.setattr(pipeline, "_compute_position_total_risk_contributions", lambda *args, **kwargs: {"AAPL": 1.0})
    monkeypatch.setattr(pipeline, "_compute_position_risk_mix", lambda *args, **kwargs: {"AAPL": {"market": 1.0, "industry": 0.0, "style": 0.0, "idio": 0.0}})
    monkeypatch.setattr(pipeline, "_load_latest_factor_coverage", lambda _cache_db, **kwargs: (None, {}))
    monkeypatch.setattr(
        pipeline,
        "_compute_exposures_modes",
        lambda *args, **kwargs: {"raw": [], "sensitivity": [], "risk_contribution": []},
    )
    monkeypatch.setattr(
        pipeline,
        "stage_refresh_cache_snapshot",
        lambda **kwargs: captured.update(
            {
                "recomputed_this_refresh": kwargs["recomputed_this_refresh"],
                "recompute_reason": kwargs["recompute_reason"],
            }
        )
        or {
            "snapshot_id": "snap_upstream_core",
            "risk_engine_state": kwargs["risk_engine_meta"],
            "sanity": {"status": "ok"},
            "health_refreshed": False,
            "persisted_payloads": {
                "universe_loadings": kwargs["universe_loadings"],
                "portfolio": {"positions": []},
                "exposures": {"raw": [], "sensitivity": [], "risk_contribution": []},
                "risk": {"risk_engine": kwargs["risk_engine_meta"]},
                "refresh_meta": {"source_dates": dict(source_dates)},
            },
        },
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "persist_model_outputs",
        lambda **kwargs: captured.update({"persist_called": True, "persisted_payloads": kwargs["persisted_payloads"]}) or {"status": "ok", "run_id": kwargs["run_id"]},
    )
    monkeypatch.setattr(
        pipeline.refresh_persistence,
        "_overlay_current_membership_truth",
        lambda **kwargs: kwargs["persisted_payloads"],
    )
    monkeypatch.setattr(
        pipeline.serving_outputs,
        "persist_current_payloads",
        lambda **kwargs: {"status": "ok", "run_id": kwargs["run_id"]},
    )
    monkeypatch.setattr(pipeline.sqlite, "cache_set", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline.runtime_state, "persist_runtime_state", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(pipeline.runtime_state, "publish_active_snapshot", lambda *args, **kwargs: {"status": "ok"})

    out = pipeline.run_refresh(
        mode="light",
        refresh_scope="full",
        skip_snapshot_rebuild=True,
        skip_cuse4_foundation=True,
        skip_risk_engine=True,
        upstream_core_recomputed=True,
    )

    assert out["status"] == "ok"
    assert captured["recomputed_this_refresh"] is True
    assert captured["recompute_reason"] == "orchestrator_precomputed"
    assert captured["persist_called"] is True
    assert "universe_loadings" in captured["persisted_payloads"]


def test_run_refresh_skip_risk_engine_reuses_persisted_risk_artifacts_when_runtime_state_meta_wins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    source_dates = {
        "fundamentals_asof": "2026-02-27",
        "classification_asof": "2026-03-01",
        "prices_asof": "2026-03-07",
        "exposures_asof": "2026-03-07",
    }
    risk_meta = {
        "status": "ok",
        "method_version": pipeline.RISK_ENGINE_METHOD_VERSION,
        "last_recompute_date": "2026-03-07",
        "factor_returns_latest_date": "2026-03-07",
        "cross_section_min_age_days": 7,
        "lookback_days": 504,
        "specific_risk_ticker_count": 1,
        "recompute_interval_days": 7,
        "latest_r2": 0.4,
    }
    cached_universe_loadings = {
        "factors": ["style_beta_score"],
        "factor_vols": {"style_beta_score": 0.05},
        "ticker_count": 1,
        "eligible_ticker_count": 1,
        "source_dates": dict(source_dates),
        "risk_engine": dict(risk_meta),
        "by_ticker": {
            "AAPL": {
                "ticker": "AAPL",
                "price": 100.0,
                "weight": 1.0,
                "name": "Apple",
                "exposures": {"style_beta_score": 1.1},
                "specific_var": 0.01,
                "specific_vol": 0.1,
                "model_status": "core_estimated",
            }
        },
    }

    monkeypatch.setattr(
        pipeline.core_reads,
        "load_source_dates",
        lambda **kwargs: dict(source_dates),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_prices",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not load latest prices")),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_fundamentals",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not load latest fundamentals")),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_raw_cross_section_latest",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not load latest exposures")),
    )
    monkeypatch.setattr(pipeline.config, "CUSE4_ENABLE_ESTU_AUDIT", False)
    monkeypatch.setattr(
        pipeline,
        "_resolve_effective_risk_engine_meta",
        lambda **kwargs: (dict(risk_meta), "runtime_state"),
    )

    def _cache_get(key: str):
        payloads = {
            "risk_engine_meta": dict(risk_meta),
            "risk": {
                "cov_matrix": {"factors": ["style_beta_score"], "correlation": [[1.0]]},
            },
            "universe_loadings": dict(cached_universe_loadings),
        }
        return payloads.get(key)

    monkeypatch.setattr(pipeline.sqlite, "cache_get_live", lambda key, **kwargs: _cache_get(key))
    monkeypatch.setattr(pipeline.sqlite, "cache_get", lambda key, **kwargs: _cache_get(key))
    monkeypatch.setattr(pipeline.sqlite, "cache_get_live_first", lambda key, **kwargs: _cache_get(key))
    monkeypatch.setattr(
        pipeline.runtime_state,
        "load_runtime_state",
        lambda key, fallback_loader=None: _cache_get(key),
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "load_latest_rebuild_authority_covariance_payload",
        lambda: {"factors": ["style_beta_score"], "matrix": [[1.0]]},
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "load_latest_rebuild_authority_risk_engine_state",
        lambda: dict(risk_meta),
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "load_latest_rebuild_authority_specific_risk_payload",
        lambda: {
            "AAPL.OQ": {
                "ticker": "AAPL",
                "specific_var": 0.01,
                "specific_vol": 0.1,
            }
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_build_positions_from_universe",
        lambda by_ticker: ([{"ticker": "AAPL", "weight": 1.0, "exposures": {"style_beta_score": 1.1}}], 100.0),
    )

    def _risk_decomposition(**kwargs):
        assert list(kwargs["cov"].columns) == ["style_beta_score"]
        assert "AAPL" in kwargs["specific_risk_by_ticker"]
        return (
            {"market": 0.0, "industry": 10.0, "style": 20.0, "idio": 70.0},
            {"market": 0.0, "industry": 0.1, "style": 0.2},
            [{"factor_id": "style_beta_score", "sensitivity": 0.3, "factor_vol": 0.05}],
        )

    monkeypatch.setattr(pipeline, "risk_decomposition", _risk_decomposition)
    monkeypatch.setattr(
        pipeline,
        "_compute_position_risk_mix",
        lambda **kwargs: {"AAPL": {"market": 0.0, "industry": 0.2, "style": 0.3, "idio": 0.5}},
    )
    monkeypatch.setattr(pipeline, "_load_latest_factor_coverage", lambda _cache_db, **kwargs: (None, {}))
    monkeypatch.setattr(
        pipeline,
        "_compute_exposures_modes",
        lambda *args, **kwargs: {"raw": [], "sensitivity": [], "risk_contribution": []},
    )
    monkeypatch.setattr(
        pipeline,
        "stage_refresh_cache_snapshot",
        lambda **kwargs: captured.update(
            {
                "staged_universe_loadings": kwargs["universe_loadings"],
                "reuse_cached_static_payloads": kwargs["reuse_cached_static_payloads"],
            }
        )
        or {
            "snapshot_id": "snap_1",
            "risk_engine_state": {"status": "ok"},
            "sanity": {"status": "ok"},
            "health_refreshed": False,
            "persisted_payloads": {},
        },
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "persist_model_outputs",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("should not persist model outputs on holdings-only fast path")),
    )
    monkeypatch.setattr(
        pipeline.serving_outputs,
        "persist_current_payloads",
        lambda **kwargs: {"status": "ok", "run_id": kwargs["run_id"]},
    )
    monkeypatch.setattr(pipeline.sqlite, "cache_set", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        pipeline.runtime_state,
        "publish_active_snapshot",
        lambda snapshot_id, fallback_publisher=None: captured.update({"published": snapshot_id}) or {"status": "ok"},
    )

    out = pipeline.run_refresh(
        mode="light",
        refresh_scope="full",
        skip_snapshot_rebuild=True,
        skip_cuse4_foundation=True,
        skip_risk_engine=True,
    )

    assert out["status"] == "ok"
    assert out["model_outputs_write"]["status"] == "skipped"
    assert out["model_outputs_write"]["reason"] == "risk_engine_reused"
    assert captured["reuse_cached_static_payloads"] is True
    assert captured["published"] == "snap_1"
    assert isinstance(captured["staged_universe_loadings"], dict)


def test_run_refresh_skip_risk_engine_does_not_reuse_incompatible_persisted_risk_artifacts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_dates = {
        "fundamentals_asof": "2026-02-27",
        "classification_asof": "2026-03-01",
        "prices_asof": "2026-03-07",
        "exposures_asof": "2026-03-07",
    }
    runtime_risk_meta = {
        "status": "ok",
        "method_version": pipeline.RISK_ENGINE_METHOD_VERSION,
        "last_recompute_date": "2026-03-07",
        "factor_returns_latest_date": "2026-03-07",
        "cross_section_min_age_days": 7,
        "lookback_days": 504,
        "specific_risk_ticker_count": 1,
        "recompute_interval_days": 7,
    }
    persisted_risk_meta = {
        "status": "ok",
        "method_version": pipeline.RISK_ENGINE_METHOD_VERSION,
        "last_recompute_date": "2026-03-14",
        "factor_returns_latest_date": "2026-03-14",
        "cross_section_min_age_days": 7,
        "lookback_days": 504,
        "specific_risk_ticker_count": 2,
        "recompute_interval_days": 7,
    }
    cached_universe_loadings = {
        "factors": ["style_beta_score"],
        "factor_vols": {"style_beta_score": 0.05},
        "ticker_count": 1,
        "eligible_ticker_count": 1,
        "source_dates": dict(source_dates),
        "risk_engine": dict(runtime_risk_meta),
        "by_ticker": {
            "AAPL": {
                "ticker": "AAPL",
                "price": 100.0,
                "weight": 1.0,
                "name": "Apple",
                "exposures": {"style_beta_score": 1.1},
                "specific_var": 0.01,
                "specific_vol": 0.1,
                "model_status": "core_estimated",
            }
        },
    }

    monkeypatch.setattr(
        pipeline.core_reads,
        "load_source_dates",
        lambda **kwargs: dict(source_dates),
    )
    monkeypatch.setattr(pipeline.config, "CUSE4_ENABLE_ESTU_AUDIT", False)
    monkeypatch.setattr(
        pipeline,
        "_resolve_effective_risk_engine_meta",
        lambda **kwargs: (dict(runtime_risk_meta), "runtime_state"),
    )

    def _cache_get(key: str):
        payloads = {
            "risk_engine_meta": dict(runtime_risk_meta),
            "risk": {
                "cov_matrix": {"factors": ["style_beta_score"], "correlation": [[1.0]]},
            },
            "universe_loadings": dict(cached_universe_loadings),
        }
        return payloads.get(key)

    monkeypatch.setattr(pipeline.sqlite, "cache_get_live", lambda key, **kwargs: _cache_get(key))
    monkeypatch.setattr(pipeline.sqlite, "cache_get", lambda key, **kwargs: _cache_get(key))
    monkeypatch.setattr(pipeline.sqlite, "cache_get_live_first", lambda key, **kwargs: _cache_get(key))
    monkeypatch.setattr(
        pipeline.runtime_state,
        "load_runtime_state",
        lambda key, fallback_loader=None: _cache_get(key),
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "load_latest_rebuild_authority_risk_engine_state",
        lambda: dict(persisted_risk_meta),
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "load_latest_rebuild_authority_covariance_payload",
        lambda: {"factors": ["style_beta_score"], "matrix": [[1.0]]},
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "load_latest_rebuild_authority_specific_risk_payload",
        lambda: {
            "AAPL.OQ": {
                "ticker": "AAPL",
                "specific_var": 0.01,
                "specific_vol": 0.1,
            }
        },
    )

    with pytest.raises(RuntimeError, match="skip_risk_engine requested but risk-engine cache is missing"):
        pipeline.run_refresh(
            mode="light",
            refresh_scope="full",
            skip_snapshot_rebuild=True,
            skip_cuse4_foundation=True,
            skip_risk_engine=True,
        )


def test_resolve_effective_risk_engine_meta_prefers_persisted_model_run_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_meta = {
        "status": "ok",
        "method_version": "fresh_live",
        "last_recompute_date": "2026-03-08",
        "factor_returns_latest_date": "2026-03-07",
        "specific_risk_ticker_count": 2,
    }
    persisted_meta = {
        "status": "ok",
        "method_version": pipeline.RISK_ENGINE_METHOD_VERSION,
        "last_recompute_date": "2026-03-16",
        "factor_returns_latest_date": "2026-03-13",
        "specific_risk_ticker_count": 3736,
    }

    monkeypatch.setattr(
        pipeline.runtime_state,
        "load_runtime_state",
        lambda key, fallback_loader=None: runtime_meta if key == "risk_engine_meta" else None,
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "load_latest_rebuild_authority_risk_engine_state",
        lambda: persisted_meta,
    )
    monkeypatch.setattr(
        pipeline,
        "_can_reuse_cached_universe_loadings",
        lambda cached_payload, *, source_dates, risk_engine_meta: (
            risk_engine_meta.get("method_version") == persisted_meta["method_version"]
            and risk_engine_meta.get("factor_returns_latest_date") == persisted_meta["factor_returns_latest_date"]
            and bool(cached_payload),
            "source_and_risk_engine_match",
        ),
    )

    out, source = pipeline._resolve_effective_risk_engine_meta(
        fallback_loader=lambda key: None,
    )

    assert out["method_version"] == persisted_meta["method_version"]
    assert out["factor_returns_latest_date"] == persisted_meta["factor_returns_latest_date"]
    assert out["last_recompute_date"] == persisted_meta["last_recompute_date"]
    assert out["specific_risk_ticker_count"] == persisted_meta["specific_risk_ticker_count"]
    assert out["estimation_exposure_anchor_date"] == "2026-03-06"
    assert source == "model_run_metadata"


def test_resolve_effective_risk_engine_meta_enriches_runtime_with_persisted_latest_r2(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_meta = {
        "status": "ok",
        "method_version": pipeline.RISK_ENGINE_METHOD_VERSION,
        "last_recompute_date": "2026-03-16",
        "factor_returns_latest_date": "2026-03-13",
        "specific_risk_ticker_count": 3736,
    }
    persisted_meta = {
        "status": "ok",
        "method_version": pipeline.RISK_ENGINE_METHOD_VERSION,
        "last_recompute_date": "2026-03-16",
        "factor_returns_latest_date": "2026-03-13",
        "specific_risk_ticker_count": 3736,
        "latest_r2": 0.42,
    }

    monkeypatch.setattr(
        pipeline.runtime_state,
        "load_runtime_state",
        lambda key, fallback_loader=None: runtime_meta if key == "risk_engine_meta" else None,
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "load_latest_rebuild_authority_risk_engine_state",
        lambda: persisted_meta,
    )

    out, source = pipeline._resolve_effective_risk_engine_meta(
        fallback_loader=lambda key: None,
    )

    assert out["latest_r2"] == pytest.approx(0.42)
    assert source == "runtime_state_enriched_from_model_run_metadata"


def test_run_refresh_light_mode_prefers_persisted_model_run_state_over_stale_runtime_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    stale_runtime_meta = {
        "status": "ok",
        "method_version": "fresh_live",
        "last_recompute_date": "2026-03-08",
        "factor_returns_latest_date": "2026-03-07",
        "lookback_days": 504,
        "cross_section_min_age_days": 7,
        "recompute_interval_days": 7,
        "specific_risk_ticker_count": 2,
    }
    persisted_meta = {
        "status": "ok",
        "method_version": pipeline.RISK_ENGINE_METHOD_VERSION,
        "last_recompute_date": "2026-03-16",
        "factor_returns_latest_date": "2026-03-13",
        "lookback_days": 504,
        "cross_section_min_age_days": 7,
        "recompute_interval_days": 7,
        "specific_risk_ticker_count": 3736,
    }

    monkeypatch.setattr(
        pipeline.core_reads,
        "load_source_dates",
        lambda **kwargs: {
            "fundamentals_asof": "2026-02-27",
            "classification_asof": "2026-02-27",
            "prices_asof": "2026-03-13",
            "exposures_asof": "2026-03-13",
            "exposures_latest_available_asof": "2026-03-13",
        },
    )
    monkeypatch.setattr(pipeline, "_load_cached_risk_display_payload", lambda **kwargs: {"factors": [], "correlation": []})
    monkeypatch.setattr(pipeline.core_reads, "load_latest_prices", lambda **kwargs: pytest.fail("should not rebuild prices"))
    monkeypatch.setattr(pipeline.core_reads, "load_latest_fundamentals", lambda as_of_date=None, **kwargs: pytest.fail("should not rebuild fundamentals"))
    monkeypatch.setattr(pipeline.core_reads, "load_raw_cross_section_latest", lambda **kwargs: pytest.fail("should not rebuild exposures"))
    monkeypatch.setattr(pipeline, "_build_positions_from_universe", lambda by_ticker: ([{"ticker": "AAPL"}], 1.0))
    monkeypatch.setattr(
        pipeline,
        "risk_decomposition",
        lambda **kwargs: (
            {"market": 1.0, "industry": 0.0, "style": 0.0, "idio": 0.0},
            {"market": 1.0, "industry": 0.0, "style": 0.0},
            [],
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "_compute_position_risk_mix",
        lambda **kwargs: {"AAPL": {"market": 1.0, "industry": 0.0, "style": 0.0, "idio": 0.0}},
    )
    monkeypatch.setattr(
        pipeline,
        "_compute_position_total_risk_contributions",
        lambda **kwargs: {"AAPL": 1.0},
    )
    monkeypatch.setattr(pipeline, "_load_latest_factor_coverage", lambda cache_db, **kwargs: ("2026-03-13", {}))
    monkeypatch.setattr(pipeline, "_compute_exposures_modes", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        pipeline.runtime_state,
        "load_runtime_state",
        lambda key, fallback_loader=None: stale_runtime_meta if key == "risk_engine_meta" else None,
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "load_latest_rebuild_authority_risk_engine_state",
        lambda: persisted_meta,
    )
    monkeypatch.setattr(
        pipeline,
        "_can_reuse_cached_universe_loadings",
        lambda cached_payload, *, source_dates, risk_engine_meta: (
            risk_engine_meta.get("method_version") == persisted_meta["method_version"]
            and risk_engine_meta.get("factor_returns_latest_date") == persisted_meta["factor_returns_latest_date"]
            and bool(cached_payload),
            "source_and_risk_engine_match",
        ),
    )
    monkeypatch.setattr(
        pipeline.sqlite,
        "cache_get_live_first",
        lambda key, **kwargs: {
            "risk_engine_cov": {"factors": ["style_beta_score"], "matrix": [[1.0]]},
            "risk_engine_specific_risk": {"AAPL": {"ticker": "AAPL", "specific_var": 0.01, "specific_vol": 0.1}},
        }.get(key),
    )
    monkeypatch.setattr(
        pipeline.sqlite,
        "cache_get",
        lambda key, **kwargs: (
            {
                "by_ticker": {"AAPL": {"ticker": "AAPL", "exposures": {}, "model_status": "core_estimated"}},
                "ticker_count": 1,
                "eligible_ticker_count": 1,
                "factor_count": 1,
                "source_dates": {
                    "fundamentals_asof": "2026-02-27",
                    "classification_asof": "2026-02-27",
                    "prices_asof": "2026-03-13",
                    "exposures_asof": "2026-03-13",
                    "exposures_latest_available_asof": "2026-03-13",
                },
                "risk_engine": persisted_meta,
                "factor_catalog": [],
                "factors": [],
                "factor_vols": {},
                "index": [],
            }
            if key == "universe_loadings"
            else None
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "stage_refresh_cache_snapshot",
        lambda **kwargs: captured.update(
            {
                "risk_engine_meta": kwargs["risk_engine_meta"],
                "recomputed_this_refresh": kwargs["recomputed_this_refresh"],
            }
        )
        or {
            "snapshot_id": "snap",
            "risk_engine_state": kwargs["risk_engine_meta"],
            "sanity": {"status": "ok"},
            "health_refreshed": True,
            "persisted_payloads": {"risk": {"risk_engine": kwargs["risk_engine_meta"]}},
        },
    )
    monkeypatch.setattr(pipeline.model_outputs, "persist_model_outputs", lambda **kwargs: {"status": "skipped"})
    monkeypatch.setattr(pipeline.serving_outputs, "persist_current_payloads", lambda **kwargs: {"status": "ok"})
    monkeypatch.setattr(pipeline.sqlite, "cache_set", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline.runtime_state, "persist_runtime_state", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(pipeline.runtime_state, "publish_active_snapshot", lambda *args, **kwargs: {"status": "ok"})

    out = pipeline.run_refresh(
        mode="light",
        refresh_scope="full",
        skip_snapshot_rebuild=True,
        skip_cuse4_foundation=True,
    )

    assert out["status"] == "ok"
    assert out["universe_loadings_reused"] is True
    assert captured["recomputed_this_refresh"] is False
    assert captured["risk_engine_meta"]["factor_returns_latest_date"] == "2026-03-13"
    assert captured["risk_engine_meta"]["method_version"] == pipeline.RISK_ENGINE_METHOD_VERSION


def test_run_refresh_uses_persisted_risk_artifacts_when_runtime_cache_is_degraded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    persisted_meta = {
        "status": "ok",
        "method_version": pipeline.RISK_ENGINE_METHOD_VERSION,
        "last_recompute_date": "2026-03-16",
        "factor_returns_latest_date": "2026-03-13",
        "lookback_days": 504,
        "cross_section_min_age_days": 7,
        "recompute_interval_days": 7,
        "specific_risk_ticker_count": 3736,
    }
    persisted_cov = {
        "factors": ["market", "industry_technology_equipment", "style_beta_score"],
        "matrix": [
            [0.04, 0.01, 0.002],
            [0.01, 0.09, 0.003],
            [0.002, 0.003, 0.16],
        ],
    }
    persisted_specific = {
        "AAPL.OQ": {"ric": "AAPL.OQ", "ticker": "AAPL", "specific_var": 0.01, "specific_vol": 0.1},
        "MSFT.OQ": {"ric": "MSFT.OQ", "ticker": "MSFT", "specific_var": 0.02, "specific_vol": 0.1414},
    }

    monkeypatch.setattr(
        pipeline.core_reads,
        "load_source_dates",
        lambda **kwargs: {
            "fundamentals_asof": "2026-03-13",
            "classification_asof": "2026-03-13",
            "prices_asof": "2026-03-13",
            "exposures_asof": "2026-03-13",
            "exposures_latest_available_asof": "2026-03-13",
        },
    )
    monkeypatch.setattr(pipeline, "_load_cached_risk_display_payload", lambda **kwargs: None)
    monkeypatch.setattr(pipeline.core_reads, "load_latest_prices", lambda **kwargs: pd.DataFrame({"ticker": ["AAPL"]}))
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_fundamentals",
        lambda as_of_date=None, **kwargs: pd.DataFrame({"ticker": ["AAPL"]}),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_raw_cross_section_latest",
        lambda **kwargs: pd.DataFrame({"ticker": ["AAPL"], "market": [1.0], "industry_technology_equipment": [0.4], "beta_score": [1.0]}),
    )
    monkeypatch.setattr(
        pipeline,
        "_build_universe_ticker_loadings",
        lambda *args, **kwargs: {
            "factors": ["market", "industry_technology_equipment", "style_beta_score"],
            "factor_vols": {"market": 0.2, "industry_technology_equipment": 0.3, "style_beta_score": 0.4},
            "ticker_count": 1,
            "eligible_ticker_count": 1,
            "by_ticker": {
                "AAPL": {
                    "ticker": "AAPL",
                    "price": 100.0,
                    "exposures": {
                        "market": 1.0,
                        "industry_technology_equipment": 0.4,
                        "style_beta_score": 1.0,
                    },
                }
            },
            "factor_catalog": [],
            "index": [],
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_build_positions_from_universe",
        lambda universe_by_ticker: (
            [
                {
                    "ticker": "AAPL",
                    "weight": 1.0,
                    "market_value": 100.0,
                    "exposures": {
                        "market": 1.0,
                        "industry_technology_equipment": 0.4,
                        "style_beta_score": 1.0,
                    },
                }
            ],
            100.0,
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "risk_decomposition",
        lambda **kwargs: (
            {"market": 30.0, "industry": 20.0, "style": 10.0, "idio": 40.0},
            {"market": 0.5, "industry": 0.33, "style": 0.17},
            [
                {"factor_id": "market", "factor_vol": 0.2, "sensitivity": 0.2, "pct_of_total": 30.0},
                {"factor_id": "industry_technology_equipment", "factor_vol": 0.3, "sensitivity": 0.12, "pct_of_total": 20.0},
                {"factor_id": "style_beta_score", "factor_vol": 0.4, "sensitivity": 0.4, "pct_of_total": 10.0},
            ],
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "_compute_position_risk_mix",
        lambda **kwargs: {"AAPL": {"market": 0.3, "industry": 0.2, "style": 0.1, "idio": 0.4}},
    )
    monkeypatch.setattr(
        pipeline,
        "_compute_position_total_risk_contributions",
        lambda **kwargs: {"AAPL": 100.0},
    )
    monkeypatch.setattr(pipeline, "_load_latest_factor_coverage", lambda cache_db, **kwargs: ("2026-03-13", {}))
    monkeypatch.setattr(
        pipeline,
        "_compute_exposures_modes",
        lambda positions, cov, factor_details, **kwargs: captured.update({"cov_columns": list(cov.columns)}) or {
            "raw": [],
            "sensitivity": [],
            "risk_contribution": [],
        },
    )
    monkeypatch.setattr(
        pipeline.runtime_state,
        "load_runtime_state",
        lambda key, fallback_loader=None: {"status": "ok", "method_version": "fresh_live", "factor_returns_latest_date": "2026-03-07", "specific_risk_ticker_count": 2} if key == "risk_engine_meta" else None,
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "load_latest_rebuild_authority_risk_engine_state",
        lambda: persisted_meta,
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "load_latest_rebuild_authority_covariance_payload",
        lambda: persisted_cov,
    )
    monkeypatch.setattr(
        pipeline.model_outputs,
        "load_latest_rebuild_authority_specific_risk_payload",
        lambda: persisted_specific,
    )
    monkeypatch.setattr(
        pipeline.sqlite,
        "cache_get_live_first",
        lambda key, **kwargs: {
            "risk_engine_cov": {"factors": ["style_beta_score"], "matrix": [[1.0]]},
            "risk_engine_specific_risk": {"AAPL.OQ": {"ric": "AAPL.OQ", "ticker": "AAPL", "specific_var": 0.01, "specific_vol": 0.1}},
        }.get(key),
    )
    monkeypatch.setattr(pipeline.sqlite, "cache_get", lambda key, **kwargs: None)
    monkeypatch.setattr(pipeline, "load_projection_only_universe_rows", lambda _conn: [])
    monkeypatch.setattr(
        pipeline,
        "stage_refresh_cache_snapshot",
        lambda **kwargs: captured.update(
            {
                "cov_payload": kwargs["cov_payload"],
                "specific_risk_by_security": kwargs["specific_risk_by_security"],
            }
        )
        or {
            "snapshot_id": "snap",
            "risk_engine_state": kwargs["risk_engine_meta"],
            "sanity": {"status": "ok"},
            "health_refreshed": False,
            "persisted_payloads": {"risk": {"risk_engine": kwargs["risk_engine_meta"]}},
        },
    )
    monkeypatch.setattr(pipeline.model_outputs, "persist_model_outputs", lambda **kwargs: {"status": "skipped"})
    monkeypatch.setattr(pipeline.serving_outputs, "persist_current_payloads", lambda **kwargs: {"status": "ok"})
    monkeypatch.setattr(pipeline.sqlite, "cache_set", lambda *args, **kwargs: None)
    monkeypatch.setattr(pipeline.runtime_state, "persist_runtime_state", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(pipeline.runtime_state, "publish_active_snapshot", lambda *args, **kwargs: {"status": "ok"})

    out = pipeline.run_refresh(
        mode="light",
        refresh_scope="full",
        skip_snapshot_rebuild=True,
        skip_cuse4_foundation=True,
    )

    assert out["status"] == "ok"
    assert captured["cov_payload"]["factors"] == ["market", "industry_technology_equipment", "style_beta_score"]
    assert set(captured["specific_risk_by_security"].keys()) == {"AAPL.OQ", "MSFT.OQ"}
    assert captured["cov_columns"] == ["market", "industry_technology_equipment", "style_beta_score"]


def test_run_refresh_publish_only_republishes_cached_payloads_without_recompute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    payloads = {
        "portfolio": {
            "position_count": 2,
            "total_value": 123.45,
            "source_dates": {"prices_asof": "2026-03-14"},
            "run_id": "old_run",
            "snapshot_id": "old_snapshot",
        },
        "risk": {"risk_engine": {"method_version": "v8"}, "run_id": "old_run", "snapshot_id": "old_snapshot"},
        "refresh_meta": {
            "cross_section_snapshot": {"status": "reused"},
            "risk_engine": {"method_version": "v8"},
            "cuse4_foundation": {"status": "ok"},
            "run_id": "old_run",
            "snapshot_id": "old_snapshot",
        },
        "model_sanity": {"status": "ok", "run_id": "old_run", "snapshot_id": "old_snapshot"},
        "eligibility": {},
        "exposures": {"run_id": "old_run", "snapshot_id": "old_snapshot"},
        "health_diagnostics": {"status": "ok", "run_id": "old_run", "snapshot_id": "old_snapshot"},
        "universe_factors": {"run_id": "old_run", "snapshot_id": "old_snapshot"},
        "universe_loadings": {
            "run_id": "old_run",
            "snapshot_id": "old_snapshot",
            "by_ticker": {
                "AAPL": {
                    "ticker": "AAPL",
                    "model_status": "core_estimated",
                    "exposure_origin": "native",
                }
            },
        },
    }

    monkeypatch.setattr(pipeline.publish_payloads, "load_publishable_payloads", lambda **kwargs: (dict(payloads), []))
    monkeypatch.setattr(
        pipeline.serving_outputs,
        "persist_current_payloads",
        lambda **kwargs: captured.update({"payloads": kwargs["payloads"], "refresh_mode": kwargs["refresh_mode"]}) or {
            "status": "ok",
            "snapshot_id": kwargs["snapshot_id"],
        },
    )
    monkeypatch.setattr(pipeline.sqlite, "cache_set", lambda key, value, **kwargs: captured.setdefault("cache_set", []).append((key, value)))
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_source_dates",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("publish-only should not load source dates")),
    )

    out = pipeline.run_refresh(mode="publish")

    assert out["status"] == "ok"
    assert out["mode"] == "publish"
    assert out["universe_loadings_reuse_reason"] == "publish_only_cached_payloads"
    assert out["health_refreshed"] is False
    assert captured["refresh_mode"] == "publish"
    stamped_payloads = dict(captured["payloads"])
    assert stamped_payloads["portfolio"]["run_id"] == out["run_id"]
    assert stamped_payloads["portfolio"]["snapshot_id"] == out["snapshot_id"]
    assert stamped_payloads["risk"]["run_id"] == out["run_id"]
    assert stamped_payloads["exposures"]["snapshot_id"] == out["snapshot_id"]
    assert stamped_payloads["refresh_meta"]["run_id"] == out["run_id"]


def test_run_refresh_publish_only_rejects_broken_universe_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payloads = {
        "eligibility": {},
        "exposures": {},
        "health_diagnostics": {},
        "model_sanity": {},
        "portfolio": {"positions": [], "position_count": 0, "total_value": 0.0},
        "refresh_meta": {"risk_engine": {"core_state_through_date": "2026-03-20"}},
        "risk": {"risk_engine": {"core_state_through_date": "2026-03-20"}},
        "risk_engine_cov": {},
        "risk_engine_specific_risk": {},
        "universe_factors": {},
        "universe_loadings": {
            "by_ticker": {
                "SPY": {
                    "ticker": "SPY",
                    "model_status": "ineligible",
                    "exposure_origin": "projected_returns",
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
        pipeline.serving_outputs,
        "persist_current_payloads",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("publish should fail before persistence")),
    )

    with pytest.raises(RuntimeError, match="Publish-only universe payload integrity failed: no_modeled_tickers"):
        pipeline.run_refresh(mode="publish")


def test_run_refresh_publish_only_rejects_modeled_universe_regression_vs_live(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payloads = {
        "eligibility": {},
        "exposures": {},
        "health_diagnostics": {},
        "model_sanity": {},
        "portfolio": {"positions": [], "position_count": 0, "total_value": 0.0},
        "refresh_meta": {"risk_engine": {"core_state_through_date": "2026-03-20"}},
        "risk": {"risk_engine": {"core_state_through_date": "2026-03-20"}},
        "risk_engine_cov": {},
        "risk_engine_specific_risk": {},
        "universe_factors": {},
        "universe_loadings": {
            "by_ticker": {
                "SPY": {
                    "ticker": "SPY",
                    "model_status": "core_estimated",
                    "exposure_origin": "native",
                }
            }
        },
    }
    live_universe = {
        "by_ticker": {
            "AAPL": {"ticker": "AAPL", "model_status": "core_estimated", "exposure_origin": "native"},
            "ASML": {"ticker": "ASML", "model_status": "projected_only", "exposure_origin": "projected_fundamental"},
        }
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
        pipeline.serving_outputs,
        "load_current_payload",
        lambda payload_name: dict(live_universe) if payload_name == "universe_loadings" else None,
    )
    monkeypatch.setattr(
        pipeline.serving_outputs,
        "persist_current_payloads",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("publish should fail before persistence")),
    )

    with pytest.raises(RuntimeError, match="Publish-only universe payload regressed versus the current live modeled snapshot"):
        pipeline.run_refresh(mode="publish")


def test_run_refresh_publishes_before_deep_health_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    progress_messages: list[str] = []
    progress_events: list[dict[str, object]] = []
    source_dates = {
        "fundamentals_asof": "2026-03-13",
        "classification_asof": "2026-03-13",
        "prices_asof": "2026-03-13",
        "exposures_asof": "2026-03-13",
        "exposures_latest_available_asof": "2026-03-13",
        "exposures_served_asof": "2026-03-13",
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
        lambda **kwargs: pd.DataFrame([{"ticker": "AAPL", "ric": "AAPL.OQ", "close": 100.0}]),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_fundamentals",
        lambda **kwargs: pd.DataFrame([{"ticker": "AAPL", "ric": "AAPL.OQ", "market_cap": 1_000_000.0}]),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_raw_cross_section_latest",
        lambda **kwargs: pd.DataFrame([{"ticker": "AAPL", "ric": "AAPL.OQ", "as_of_date": "2026-03-13"}]),
    )
    monkeypatch.setattr(pipeline, "load_projection_only_universe_rows", lambda _conn: [])
    monkeypatch.setattr(
        pipeline,
        "_build_universe_ticker_loadings",
        lambda *args, **kwargs: {
            "factors": ["market"],
            "factor_vols": {"market": 0.05},
            "factor_catalog": [],
            "ticker_count": 1,
            "eligible_ticker_count": 1,
            "core_estimated_ticker_count": 1,
            "projected_only_ticker_count": 0,
            "ineligible_ticker_count": 0,
            "by_ticker": {"AAPL": {"ticker": "AAPL", "price": 100.0, "exposures": {"market": 1.0}}},
        },
    )
    monkeypatch.setattr(
        pipeline,
        "_build_positions_from_universe",
        lambda by_ticker: (
            [{"ticker": "AAPL", "weight": 1.0, "market_value": 100.0, "exposures": {"market": 1.0}}],
            100.0,
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "risk_decomposition",
        lambda **kwargs: (
            {"market": 100.0, "industry": 0.0, "style": 0.0, "idio": 0.0},
            {"market": 1.0, "industry": 0.0, "style": 0.0},
            [{"factor_id": "market", "sensitivity": 1.0, "factor_vol": 0.05}],
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "_compute_position_risk_mix",
        lambda **kwargs: {"AAPL": {"market": 1.0, "industry": 0.0, "style": 0.0, "idio": 0.0}},
    )
    monkeypatch.setattr(
        pipeline,
        "_compute_position_total_risk_contributions",
        lambda **kwargs: {"AAPL": 100.0},
    )
    monkeypatch.setattr(pipeline, "_load_latest_factor_coverage", lambda _cache_db, **kwargs: (None, {}))
    monkeypatch.setattr(
        pipeline,
        "_compute_exposures_modes",
        lambda *args, **kwargs: {"raw": [], "sensitivity": [], "risk_contribution": []},
    )

    captured: dict[str, object] = {}
    monkeypatch.setattr(
        pipeline,
        "stage_refresh_cache_snapshot",
        lambda **kwargs: captured.update({"staged_recompute_health": kwargs["recompute_health_diagnostics"]})
        or {
            "snapshot_id": "snap_post_publish_health",
            "risk_engine_state": dict(risk_meta),
            "sanity": {"status": "ok"},
            "health_refreshed": False,
            "health_refresh_state": "deferred",
            "persisted_payloads": {
                "portfolio": {"positions": [], "position_count": 1, "total_value": 100.0},
                "risk": {"risk_engine": dict(risk_meta), "risk_shares": {"market": 100.0}},
                "risk_engine_cov": {"factors": ["market"], "matrix": [[1.0]]},
                "universe_loadings": {"ticker_count": 1, "by_ticker": {"AAPL": {"ticker": "AAPL"}}},
                "refresh_meta": {"source_dates": dict(source_dates), "health_refreshed": False, "health_refresh_state": "deferred"},
                "health_diagnostics": {"status": "deferred", "diagnostics_refresh_state": "deferred"},
            },
        },
    )
    monkeypatch.setattr(
        pipeline.refresh_persistence,
        "persist_refresh_outputs",
        lambda **kwargs: events.append("persist_refresh_outputs") or ({"status": "ok"}, {"status": "ok"}),
    )
    monkeypatch.setattr(
        pipeline,
        "compute_health_diagnostics",
        lambda *args, **kwargs: events.append("compute_health_diagnostics")
        or kwargs["progress_callback"]({"message": "section marker", "diagnostics_section": "section1", "progress_kind": "analytics"})
        or {"status": "ok", "notes": []},
    )
    monkeypatch.setattr(
        pipeline.serving_outputs,
        "persist_current_payloads",
        lambda **kwargs: events.append(f"persist_current_payloads:{','.join(sorted(kwargs['payloads'].keys()))}")
        or {"status": "ok", "payload_names": sorted(kwargs["payloads"].keys())},
    )
    monkeypatch.setattr(pipeline.sqlite, "cache_set", lambda *args, **kwargs: None)

    out = pipeline.run_refresh(
        mode="light",
        refresh_scope="full",
        skip_snapshot_rebuild=True,
        skip_cuse4_foundation=True,
        skip_risk_engine=True,
        refresh_deep_health_diagnostics=True,
        progress_callback=lambda event: progress_events.append(dict(event)) or progress_messages.append(str(event.get("message") or "")),
    )

    assert out["status"] == "ok"
    assert captured["staged_recompute_health"] is False
    assert events == [
        "persist_refresh_outputs",
        "compute_health_diagnostics",
        "persist_current_payloads:health_diagnostics,refresh_meta",
    ]
    assert out["health_refreshed"] is True
    assert out["health_refresh_state"] == "recomputed"
    assert "Persisting model and serving outputs" in progress_messages
    assert "Serving payload publish complete" in progress_messages
    assert "Computing deep health diagnostics" in progress_messages
    assert "section marker" in progress_messages
    publish_event = next(event for event in progress_events if event.get("refresh_substage") == "serving_publish_complete")
    section_event = next(event for event in progress_events if event.get("diagnostics_section") == "section1")
    assert publish_event["publish_complete"] is True
    assert publish_event["published_snapshot_id"] == "snap_post_publish_health"
    assert progress_events.index(publish_event) < progress_events.index(section_event)
    assert out["publish_milestone"]["snapshot_id"] == "snap_post_publish_health"
    assert "persist_outputs" in out["substage_timings"]


def test_load_publishable_payloads_prefers_durable_serving_payloads(monkeypatch: pytest.MonkeyPatch) -> None:
    durable_payload = {"status": "durable"}
    cache_payload = {"status": "cache"}

    monkeypatch.setattr(
        pipeline.serving_outputs,
        "load_runtime_payload",
        lambda name, *, fallback_loader=None: durable_payload if name == "portfolio" else {"status": name},
    )
    monkeypatch.setattr(
        pipeline.sqlite,
        "cache_get",
        lambda name: cache_payload if name == "portfolio" else None,
    )

    payloads, missing = pipeline.publish_payloads.load_publishable_payloads()

    assert missing == []
    assert payloads["portfolio"] == durable_payload


def test_refresh_manager_marks_holdings_failure_on_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        refresh_execution_module,
        "run_model_pipeline",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(refresh_manager, "_set_state", lambda **kwargs: kwargs)
    monkeypatch.setattr(refresh_execution_module, "mark_refresh_finished", lambda **kwargs: captured.update(kwargs))

    class _FakeLock:
        def release(self) -> None:
            return None

    monkeypatch.setattr(refresh_manager, "_RUN_LOCK", _FakeLock())

    refresh_manager._run_in_background(
        job_id="abc123",
        pipeline_run_id="api_abc123",
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
    assert captured["run_id"] == "api_abc123"
    assert captured["clear_pending"] is False


def test_refresh_manager_tracks_current_stage_from_pipeline_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    updates: list[dict[str, object]] = []

    def _run_model_pipeline(**kwargs):
        callback = kwargs.get("stage_callback")
        assert callable(callback)
        callback(
            {
                "stage": "serving_refresh",
                "stage_index": 1,
                "stage_count": 1,
                "started_at": "2026-03-09T00:00:00Z",
                "message": "Publishing serving payloads",
                "refresh_substage": "persist_outputs",
                "diagnostics_section": "section1",
                "progress_pct": 25.0,
                "items_processed": 1,
                "items_total": 4,
                "unit": "steps",
            }
        )
        return {"status": "ok", "run_id": "run_123"}

    monkeypatch.setattr(refresh_execution_module, "run_model_pipeline", _run_model_pipeline)
    monkeypatch.setattr(refresh_manager, "_set_state", lambda **kwargs: updates.append(kwargs) or kwargs)

    class _FakeLock:
        def release(self) -> None:
            return None

    monkeypatch.setattr(refresh_manager, "_RUN_LOCK", _FakeLock())

    refresh_manager._run_in_background(
        job_id="abc123",
        pipeline_run_id="api_abc123",
        profile="serve-refresh",
        mode="light",
        as_of_date=None,
        resume_run_id=None,
        from_stage=None,
        to_stage=None,
        force_core=False,
        refresh_scope="holdings_only",
    )

    assert any(update.get("current_stage") == "serving_refresh" for update in updates)
    assert any(update.get("current_stage_message") == "Publishing serving payloads" for update in updates)
    assert any(update.get("current_stage_substage") == "persist_outputs" for update in updates)
    assert any(update.get("current_stage_diagnostics_section") == "section1" for update in updates)
    assert any(update.get("current_stage_progress_pct") == 25.0 for update in updates)
    assert any(update.get("status") == "ok" for update in updates)


def test_start_refresh_reports_light_mode_for_source_daily_and_uses_pipeline_run_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started_calls: list[dict[str, object]] = []

    class _FakeLock:
        def acquire(self, blocking: bool = False) -> bool:
            return True

        def release(self) -> None:
            return None

    class _FakeThread:
        def __init__(self, *args, **kwargs) -> None:
            self.kwargs = kwargs

        def start(self) -> None:
            return None

    monkeypatch.setattr(refresh_manager, "_RUN_LOCK", _FakeLock())
    monkeypatch.setattr(refresh_manager.threading, "Thread", _FakeThread)
    monkeypatch.setattr(refresh_manager, "mark_refresh_started", lambda **kwargs: started_calls.append(kwargs))

    started, state = refresh_manager.start_refresh(
        profile="source-daily",
        force_risk_recompute=False,
    )

    assert started is True
    assert state["mode"] == "light"
    assert state["pipeline_run_id"].startswith("api_")
    assert started_calls[0]["run_id"] == state["pipeline_run_id"]


def test_start_refresh_marks_holdings_failure_if_worker_start_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}
    state_store: dict[str, object] = {}

    monkeypatch.setattr(refresh_manager, "_set_state", lambda **kwargs: state_store.update(kwargs) or dict(state_store))
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
        profile="serve-refresh",
        force_risk_recompute=False,
    )

    assert started is False
    assert state["status"] == "failed"
    assert captured["status"] == "failed"
    assert captured["profile"] == "serve-refresh"
    assert captured["run_id"] == state["pipeline_run_id"]
    assert captured["clear_pending"] is False
