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


def test_positions_store_raises_when_neon_expected_but_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(positions_store.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(positions_store.config, "NEON_DATABASE_URL", "postgres://example")
    monkeypatch.setattr(
        positions_store,
        "_load_positions_from_neon",
        lambda: (_ for _ in ()).throw(RuntimeError("dsn failed")),
    )

    with pytest.raises(positions_store.HoldingsUnavailableError):
        positions_store._load_positions()


def test_build_positions_from_universe_loads_holdings_snapshot_once(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def _load_once():
        calls["count"] += 1
        return (
            {"AAPL": 10.0, "MSFT": -5.0},
            {
                "AAPL": {"account": "MAIN", "sleeve": "NEON HOLDINGS", "source": "NEON"},
                "MSFT": {"account": "MAIN", "sleeve": "NEON HOLDINGS", "source": "NEON"},
            },
        )

    from backend.analytics.services import risk_views

    monkeypatch.setattr(risk_views, "load_positions_snapshot", _load_once)

    positions, total_value = risk_views.build_positions_from_universe(
        {
            "AAPL": {"price": 100.0, "name": "Apple"},
            "MSFT": {"price": 50.0, "name": "Microsoft"},
        }
    )

    assert calls["count"] == 1
    assert len(positions) == 2
    assert total_value == 750.0


def test_build_positions_from_universe_uses_signed_gross_weights_for_long_short_books(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _load_snapshot():
        return (
            {"VRT": -3.0, "ORCL": 2.0, "WMT": -1.0},
            {
                "VRT": {"account": "MAIN", "sleeve": "NEON HOLDINGS", "source": "NEON"},
                "ORCL": {"account": "MAIN", "sleeve": "NEON HOLDINGS", "source": "NEON"},
                "WMT": {"account": "MAIN", "sleeve": "NEON HOLDINGS", "source": "NEON"},
            },
        )

    from backend.analytics.services import risk_views

    monkeypatch.setattr(risk_views, "load_positions_snapshot", _load_snapshot)

    positions, total_value = risk_views.build_positions_from_universe(
        {
            "VRT": {"price": 100.0, "name": "Vertiv"},
            "ORCL": {"price": 50.0, "name": "Oracle"},
            "WMT": {"price": 25.0, "name": "Walmart"},
        }
    )

    assert total_value == -225.0
    by_ticker = {row["ticker"]: row for row in positions}
    assert by_ticker["VRT"]["market_value"] == -300.0
    assert by_ticker["ORCL"]["market_value"] == 100.0
    assert by_ticker["WMT"]["market_value"] == -25.0
    assert by_ticker["VRT"]["weight"] == pytest.approx(-300.0 / 425.0, abs=1e-6)
    assert by_ticker["ORCL"]["weight"] == pytest.approx(100.0 / 425.0, abs=1e-6)
    assert by_ticker["WMT"]["weight"] == pytest.approx(-25.0 / 425.0, abs=1e-6)


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


def test_compute_position_total_risk_contributions_sums_to_total_variance_pct() -> None:
    import pandas as pd
    from backend.analytics.services import risk_views

    positions = [
        {
            "ticker": "LONG1",
            "weight": 0.6,
            "exposures": {"Beta": 1.0, "Book-to-Price": 0.5},
        },
        {
            "ticker": "SHORT1",
            "weight": -0.4,
            "exposures": {"Beta": 0.8, "Book-to-Price": -0.2},
        },
    ]
    cov = pd.DataFrame(
        [[0.04, 0.01], [0.01, 0.09]],
        index=["Beta", "Book-to-Price"],
        columns=["Beta", "Book-to-Price"],
    )
    specific = {
        "LONG1": {"specific_var": 0.02},
        "SHORT1": {"specific_var": 0.03},
    }

    contrib = risk_views.compute_position_total_risk_contributions(
        positions,
        cov,
        specific_risk_by_ticker=specific,
    )

    assert set(contrib) == {"LONG1", "SHORT1"}
    assert sum(contrib.values()) == pytest.approx(100.0, abs=0.05)


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

    monkeypatch.setattr(pipeline.sqlite, "cache_get_live_first", lambda key: _cache_get(key))
    monkeypatch.setattr(
        pipeline.runtime_state,
        "load_runtime_state",
        lambda key, fallback_loader=None: _cache_get(key),
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
        lambda: dict(source_dates),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_prices",
        lambda: (_ for _ in ()).throw(AssertionError("should not load latest prices")),
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

    monkeypatch.setattr(pipeline.sqlite, "cache_get_live", lambda key: _cache_get(key))
    monkeypatch.setattr(pipeline.sqlite, "cache_get", lambda key: _cache_get(key))
    monkeypatch.setattr(pipeline.sqlite, "cache_get_live_first", lambda key: _cache_get(key))
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
    monkeypatch.setattr(pipeline, "_load_latest_factor_coverage", lambda _cache_db: (None, {}))
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


def test_cached_universe_loadings_reuse_requires_matching_risk_engine_and_source_dates() -> None:
    ok, reason = pipeline._can_reuse_cached_universe_loadings(
        {
            "by_ticker": {"AAPL": {"ticker": "AAPL"}},
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
            "by_ticker": {"AAPL": {"ticker": "AAPL"}},
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
        lambda: dict(source_dates),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_prices",
        lambda: pd.DataFrame([{"ticker": "AAPL", "ric": "AAPL.OQ", "close": 100.0}]),
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

    monkeypatch.setattr(pipeline.sqlite, "cache_get_live", lambda key: _cache_get(key))
    monkeypatch.setattr(pipeline.sqlite, "cache_get", lambda key: _cache_get(key))
    monkeypatch.setattr(pipeline.sqlite, "cache_get_live_first", lambda key: _cache_get(key))
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
    monkeypatch.setattr(pipeline, "_load_latest_factor_coverage", lambda _cache_db: (None, {}))
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
        lambda: {
            "prices_asof": "2026-03-07",
            "fundamentals_asof": "2026-03-07",
            "classification_asof": "2026-03-07",
            "exposures_asof": "2026-03-07",
        },
    )
    monkeypatch.setattr(pipeline.core_reads, "load_latest_prices", lambda: pd.DataFrame({"ticker": ["AAPL"]}))
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_latest_fundamentals",
        lambda as_of_date=None: pd.DataFrame({"ticker": ["AAPL"]}),
    )
    monkeypatch.setattr(
        pipeline.core_reads,
        "load_raw_cross_section_latest",
        lambda: pd.DataFrame({"ticker": ["AAPL"], "beta_score": [1.0]}),
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
        lambda: None,
    )
    monkeypatch.setattr(
        pipeline,
        "_compute_exposures_modes",
        lambda *args, **kwargs: {"raw": [], "sensitivity": [], "risk_contribution": []},
    )
    monkeypatch.setattr(pipeline, "_load_latest_factor_coverage", lambda _cache_db: (None, {}))
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
        lambda key: stale_meta if key == "risk_engine_meta" else None,
    )
    monkeypatch.setattr(
        pipeline.sqlite,
        "cache_get_live_first",
        lambda key: {
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
        "universe_loadings": {"run_id": "old_run", "snapshot_id": "old_snapshot"},
    }

    monkeypatch.setattr(pipeline, "_load_publishable_payloads", lambda: (dict(payloads), []))
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
        lambda: (_ for _ in ()).throw(AssertionError("publish-only should not load source dates")),
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

    payloads, missing = pipeline._load_publishable_payloads()

    assert missing == []
    assert payloads["portfolio"] == durable_payload


def test_run_model_pipeline_clears_pending_after_serving_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(run_model_pipeline_module.job_runs, "ensure_schema", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "fail_stale_running_stages", lambda *args, **kwargs: 0)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "completed_stages", lambda *args, **kwargs: set())
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "begin_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "finish_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "run_rows", lambda *args, **kwargs: [])
    monkeypatch.setattr(run_model_pipeline_module.sqlite, "cache_get", lambda key: {})
    monkeypatch.setattr(run_model_pipeline_module.sqlite, "cache_get_live_first", lambda key: {})
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


def test_run_model_pipeline_serve_refresh_does_not_require_source_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "ensure_schema", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "fail_stale_running_stages", lambda *args, **kwargs: 0)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "completed_stages", lambda *args, **kwargs: set())
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "begin_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "finish_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "run_rows", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        run_model_pipeline_module.core_reads,
        "load_source_dates",
        lambda: (_ for _ in ()).throw(AssertionError("serve-refresh should not read source dates")),
    )
    monkeypatch.setattr(run_model_pipeline_module.sqlite, "cache_get", lambda key: {})
    monkeypatch.setattr(run_model_pipeline_module.sqlite, "cache_get_live_first", lambda key: {})
    monkeypatch.setattr(
        run_model_pipeline_module,
        "_run_stage",
        lambda **kwargs: {"status": "ok", "stage": kwargs.get("stage"), "as_of_date": kwargs.get("as_of_date")},
    )
    monkeypatch.setattr(
        run_model_pipeline_module,
        "mark_refresh_finished",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(run_model_pipeline_module.config, "NEON_AUTO_SYNC_ENABLED", False)

    out = run_model_pipeline_module.run_model_pipeline(profile="serve-refresh")

    assert out["status"] == "ok"
    assert out["stage_results"][0]["details"]["as_of_date"] is not None


def test_resolved_as_of_date_uses_local_source_archive_when_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _load_source_dates():
        captured["backend"] = run_model_pipeline_module.core_reads.core_read_backend_name()
        return {"fundamentals_asof": "2026-03-14"}

    monkeypatch.setattr(run_model_pipeline_module.core_reads, "load_source_dates", _load_source_dates)
    monkeypatch.setattr(
        run_model_pipeline_module.core_reads.config,
        "neon_surface_enabled",
        lambda surface: surface == "core_reads",
    )

    out = run_model_pipeline_module._resolved_as_of_date(
        None,
        prefer_local_source_archive=True,
    )

    assert out == "2026-03-13"
    assert captured["backend"] == "local"


def test_source_daily_defaults_ingest_to_current_session_not_stored_source_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(run_model_pipeline_module.job_runs, "ensure_schema", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "fail_stale_running_stages", lambda *args, **kwargs: 0)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "completed_stages", lambda *args, **kwargs: set())
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "begin_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "finish_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "run_rows", lambda *args, **kwargs: [])
    monkeypatch.setattr(run_model_pipeline_module.sqlite, "cache_get", lambda key: {})
    monkeypatch.setattr(run_model_pipeline_module.sqlite, "cache_get_live_first", lambda key: {})
    monkeypatch.setattr(
        run_model_pipeline_module.core_reads,
        "load_source_dates",
        lambda: {"fundamentals_asof": "2026-03-04", "exposures_asof": "2026-03-04"},
    )
    monkeypatch.setattr(run_model_pipeline_module, "_current_xnys_session", lambda: "2026-03-14")
    monkeypatch.setattr(run_model_pipeline_module, "_risk_recompute_due", lambda *_args, **_kwargs: (False, "within_interval"))
    monkeypatch.setattr(run_model_pipeline_module, "mark_refresh_finished", lambda **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.config, "NEON_DATABASE_URL", "")
    monkeypatch.setattr(run_model_pipeline_module.config, "DATA_BACKEND", "sqlite")
    monkeypatch.setattr(run_model_pipeline_module.config, "NEON_AUTO_SYNC_ENABLED", False)
    monkeypatch.setattr(run_model_pipeline_module.config, "NEON_AUTO_PARITY_ENABLED", False)
    monkeypatch.setattr(run_model_pipeline_module.config, "NEON_AUTO_PRUNE_ENABLED", False)

    def _run_stage(**kwargs):
        captured.setdefault("as_of_dates", []).append(kwargs["as_of_date"])
        return {"status": "ok"}

    monkeypatch.setattr(run_model_pipeline_module, "_run_stage", _run_stage)

    out = run_model_pipeline_module.run_model_pipeline(profile="source-daily")

    assert out["status"] == "ok"
    assert captured["as_of_dates"]
    assert all(value == "2026-03-14" for value in captured["as_of_dates"])


def test_run_stage_serving_refresh_uses_local_source_archive_for_local_publish_profiles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(run_model_pipeline_module, "_serving_refresh_skip_risk_engine", lambda **kwargs: (True, "risk_cache_current"))
    monkeypatch.setattr(
        run_model_pipeline_module.core_reads.config,
        "neon_surface_enabled",
        lambda surface: surface == "core_reads",
    )

    def _run_refresh(**kwargs):
        captured["backend"] = run_model_pipeline_module.core_reads.core_read_backend_name()
        return {"status": "ok"}

    monkeypatch.setattr(run_model_pipeline_module, "run_refresh", _run_refresh)

    out = run_model_pipeline_module._run_stage(
        profile="source-daily",
        stage="serving_refresh",
        as_of_date="2026-03-14",
        should_run_core=False,
        serving_mode="light",
        force_core=False,
        core_reason="within_interval",
        data_db=run_model_pipeline_module.DATA_DB,
        cache_db=run_model_pipeline_module.CACHE_DB,
        prefer_local_source_archive=True,
        refresh_scope=None,
    )

    assert out["status"] == "ok"
    assert captured["backend"] == "local"


def test_run_stage_serving_refresh_keeps_neon_backend_for_canonical_serve_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(run_model_pipeline_module, "_serving_refresh_skip_risk_engine", lambda **kwargs: (True, "risk_cache_current"))
    monkeypatch.setattr(
        run_model_pipeline_module.core_reads.config,
        "neon_surface_enabled",
        lambda surface: surface == "core_reads",
    )

    def _run_refresh(**kwargs):
        captured["backend"] = run_model_pipeline_module.core_reads.core_read_backend_name()
        return {"status": "ok"}

    monkeypatch.setattr(run_model_pipeline_module, "run_refresh", _run_refresh)

    out = run_model_pipeline_module._run_stage(
        profile="serve-refresh",
        stage="serving_refresh",
        as_of_date="2026-03-14",
        should_run_core=False,
        serving_mode="light",
        force_core=False,
        core_reason="within_interval",
        data_db=run_model_pipeline_module.DATA_DB,
        cache_db=run_model_pipeline_module.CACHE_DB,
        prefer_local_source_archive=False,
        refresh_scope=None,
    )

    assert out["status"] == "ok"
    assert captured["backend"] == "neon"


def test_run_model_pipeline_reports_stage_runtime_details(monkeypatch: pytest.MonkeyPatch) -> None:
    finished: list[dict[str, object]] = []

    monkeypatch.setattr(run_model_pipeline_module.job_runs, "ensure_schema", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "fail_stale_running_stages", lambda *args, **kwargs: 0)
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "completed_stages", lambda *args, **kwargs: set())
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "begin_stage", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        run_model_pipeline_module.job_runs,
        "finish_stage",
        lambda **kwargs: finished.append(kwargs),
    )
    monkeypatch.setattr(run_model_pipeline_module.job_runs, "run_rows", lambda *args, **kwargs: [])
    monkeypatch.setattr(run_model_pipeline_module.sqlite, "cache_get", lambda key: {})
    monkeypatch.setattr(run_model_pipeline_module.sqlite, "cache_get_live_first", lambda key: {})
    monkeypatch.setattr(
        run_model_pipeline_module,
        "_run_stage",
        lambda **kwargs: {"status": "ok", "stage": kwargs.get("stage")},
    )
    monkeypatch.setattr(
        run_model_pipeline_module,
        "mark_refresh_finished",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(run_model_pipeline_module.config, "NEON_AUTO_SYNC_ENABLED", False)

    out = run_model_pipeline_module.run_model_pipeline(profile="serve-refresh")

    assert out["status"] == "ok"
    assert finished
    details = finished[0]["details"]
    assert isinstance(details, dict)
    assert "duration_seconds" in details
    assert details["stage_index"] == 1


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
                "progress_pct": 25.0,
                "items_processed": 1,
                "items_total": 4,
                "unit": "steps",
            }
        )
        return {"status": "ok", "run_id": "run_123"}

    monkeypatch.setattr(refresh_manager, "run_model_pipeline", _run_model_pipeline)
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
