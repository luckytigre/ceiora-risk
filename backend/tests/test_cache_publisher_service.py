from __future__ import annotations

from pathlib import Path

from backend import config
from backend.analytics import refresh_metadata
from backend.analytics.services import cache_publisher
from backend.data import sqlite as cache_sqlite


def _health_payload() -> dict:
    return {
        "status": "ok",
        "as_of": "2026-03-03",
        "notes": [],
        "section5": {
            "fundamentals": {"fields": []},
            "trbc_history": {"fields": []},
        }
    }


def test_stage_refresh_cache_snapshot_is_not_live_until_publish(monkeypatch, tmp_path: Path) -> None:
    cache_db = tmp_path / "cache.db"
    data_db = tmp_path / "data.db"
    data_db.touch()

    monkeypatch.setattr(config, "SQLITE_PATH", str(cache_db))
    monkeypatch.setattr(cache_sqlite, "_SCHEMA_READY", False)
    monkeypatch.setattr(cache_sqlite, "_SCHEMA_READY_PATH", None)
    monkeypatch.setattr(
        cache_publisher,
        "compute_health_diagnostics",
        lambda *args, **kwargs: {"status": "ok", "section5": {"fundamentals": {"fields": []}, "trbc_history": {"fields": []}}},
    )
    cache_sqlite.cache_set("health_diagnostics", _health_payload())
    cache_sqlite.cache_set("portfolio", {"version": "old"})

    staged = cache_publisher.stage_refresh_cache_snapshot(
        run_id="run_stage_1",
        refresh_mode="light",
        refresh_started_at="2026-03-05T00:00:00Z",
        source_dates={
            "fundamentals_asof": "2026-03-04",
            "exposures_asof": "2026-03-04",
        },
        snapshot_build={"status": "skipped"},
        risk_engine_meta={
            "status": "ok",
            "method_version": "v2",
            "last_recompute_date": "2026-03-05",
            "factor_returns_latest_date": "2026-03-03",
            "cross_section_min_age_days": 7,
            "recompute_interval_days": 7,
            "lookback_days": 504,
            "specific_risk_ticker_count": 2,
        },
        recomputed_this_refresh=False,
        recompute_reason="light_mode_skip",
        cov_payload={"factors": ["style_beta_score"], "matrix": [[1.0]]},
        specific_risk_by_security={"AAPL.OQ": {"ticker": "AAPL", "specific_var": 0.01}},
        positions=[{"ticker": "AAPL", "weight": 1.0, "exposures": {"style_beta_score": 1.1}}],
        total_value=100.0,
        risk_shares={"market": 0.0, "industry": 20.0, "style": 30.0, "idio": 50.0},
        component_shares={"market": 0.0, "industry": 0.4, "style": 0.6},
        factor_details=[
            {"factor_id": "style_beta_score", "exposure": 0.1, "sensitivity": 0.01, "factor_vol": 0.05, "pct_of_total": 3.0}
        ],
        cov_matrix={"factors": ["style_beta_score"], "correlation": [[1.0]]},
        latest_r2=0.35,
        universe_loadings={
            "as_of_date": "2026-03-03",
            "latest_available_asof": "2026-03-04",
            "factors": ["style_beta_score"],
            "factor_vols": {"style_beta_score": 0.05},
            "factor_catalog": [],
            "ticker_count": 1,
            "eligible_ticker_count": 1,
            "core_estimated_ticker_count": 1,
            "projected_only_ticker_count": 0,
            "ineligible_ticker_count": 0,
            "by_ticker": {"AAPL": {"ticker": "AAPL", "price": 100.0, "exposures": {"style_beta_score": 1.1}}},
        },
        exposure_modes={"raw": [], "sensitivity": [], "risk_contribution": []},
        factor_catalog=[],
        cuse4_foundation={"status": "skipped"},
        data_db=data_db,
        cache_db=cache_db,
    )

    assert staged["snapshot_id"] == "run_stage_1"
    # Staged writes should not be visible until publish.
    assert cache_sqlite.cache_get("portfolio") == {"version": "old"}

    cache_sqlite.cache_publish_snapshot(staged["snapshot_id"])
    portfolio_live = cache_sqlite.cache_get("portfolio")
    assert isinstance(portfolio_live, dict)
    assert int(portfolio_live.get("position_count", 0)) == 1
    assert portfolio_live.get("snapshot_id") == "run_stage_1"
    assert portfolio_live.get("run_id") == "run_stage_1"
    assert portfolio_live.get("source_dates", {}).get("exposures_latest_available_asof") == "2026-03-04"
    assert portfolio_live.get("source_dates", {}).get("exposures_served_asof") == "2026-03-03"
    refresh_meta = cache_sqlite.cache_get("refresh_meta")
    assert isinstance(refresh_meta, dict)
    assert refresh_meta.get("snapshot_id") == "run_stage_1"
    assert refresh_meta.get("health_refresh_state") == "carried_forward"
    exposures_live = cache_sqlite.cache_get("exposures")
    assert isinstance(exposures_live, dict)
    assert exposures_live.get("snapshot_id") == "run_stage_1"
    assert exposures_live.get("source_dates", {}).get("exposures_served_asof") == "2026-03-03"
    universe_factors = cache_sqlite.cache_get("universe_factors")
    assert isinstance(universe_factors, dict)
    assert universe_factors.get("core_estimated_ticker_count") == 1
    assert universe_factors.get("projected_only_ticker_count") == 0


def test_stage_refresh_cache_snapshot_keeps_missing_r_squared_missing(monkeypatch, tmp_path: Path) -> None:
    cache_db = tmp_path / "cache.db"
    data_db = tmp_path / "data.db"
    data_db.touch()

    monkeypatch.setattr(config, "SQLITE_PATH", str(cache_db))
    monkeypatch.setattr(cache_sqlite, "_SCHEMA_READY", False)
    monkeypatch.setattr(cache_sqlite, "_SCHEMA_READY_PATH", None)
    monkeypatch.setattr(cache_publisher, "compute_health_diagnostics", lambda *args, **kwargs: {"status": "ok"})

    staged = cache_publisher.stage_refresh_cache_snapshot(
        run_id="run_stage_missing_r2",
        refresh_mode="light",
        refresh_started_at="2026-03-05T00:00:00Z",
        source_dates={
            "fundamentals_asof": "2026-03-04",
            "exposures_asof": "2026-03-04",
        },
        snapshot_build={"status": "skipped"},
        risk_engine_meta={
            "status": "ok",
            "method_version": "v2",
            "last_recompute_date": "2026-03-05",
            "factor_returns_latest_date": "2026-03-03",
            "cross_section_min_age_days": 7,
            "recompute_interval_days": 7,
            "lookback_days": 504,
            "specific_risk_ticker_count": 2,
        },
        recomputed_this_refresh=False,
        recompute_reason="light_mode_skip",
        cov_payload={"factors": ["style_beta_score"], "matrix": [[1.0]]},
        specific_risk_by_security={"AAPL.OQ": {"ticker": "AAPL", "specific_var": 0.01}},
        positions=[{"ticker": "AAPL", "weight": 1.0, "exposures": {"style_beta_score": 1.1}}],
        total_value=100.0,
        risk_shares={"market": 0.0, "industry": 20.0, "style": 30.0, "idio": 50.0},
        component_shares={"market": 0.0, "industry": 0.4, "style": 0.6},
        factor_details=[
            {"factor_id": "style_beta_score", "exposure": 0.1, "sensitivity": 0.01, "factor_vol": 0.05, "pct_of_total": 3.0}
        ],
        cov_matrix={"factors": ["style_beta_score"], "correlation": [[1.0]]},
        latest_r2=None,
        universe_loadings={
            "as_of_date": "2026-03-03",
            "latest_available_asof": "2026-03-04",
            "factors": ["style_beta_score"],
            "factor_vols": {"style_beta_score": 0.05},
            "factor_catalog": [],
            "ticker_count": 1,
            "eligible_ticker_count": 1,
            "core_estimated_ticker_count": 1,
            "projected_only_ticker_count": 0,
            "ineligible_ticker_count": 0,
            "by_ticker": {"AAPL": {"ticker": "AAPL", "price": 100.0, "exposures": {"style_beta_score": 1.1}}},
        },
        exposure_modes={"raw": [], "sensitivity": [], "risk_contribution": []},
        factor_catalog=[],
        cuse4_foundation={"status": "skipped"},
        data_db=data_db,
        cache_db=cache_db,
    )

    assert staged["persisted_payloads"]["risk"]["r_squared"] is None
    assert staged["persisted_payloads"]["risk"]["risk_engine"]["latest_r2"] is None
    assert staged["persisted_payloads"]["universe_factors"]["r_squared"] is None


def test_stage_refresh_cache_snapshot_reuses_matching_health_payload(monkeypatch, tmp_path: Path) -> None:
    cache_db = tmp_path / "cache.db"
    data_db = tmp_path / "data.db"
    data_db.touch()

    monkeypatch.setattr(config, "SQLITE_PATH", str(cache_db))
    monkeypatch.setattr(cache_sqlite, "_SCHEMA_READY", False)
    monkeypatch.setattr(cache_sqlite, "_SCHEMA_READY_PATH", None)

    staged_first = cache_publisher.stage_refresh_cache_snapshot(
        run_id="run_stage_a",
        refresh_mode="light",
        refresh_started_at="2026-03-05T00:00:00Z",
        source_dates={
            "fundamentals_asof": "2026-03-04",
            "exposures_asof": "2026-03-04",
            "exposures_latest_available_asof": "2026-03-04",
        },
        snapshot_build={"status": "skipped"},
        risk_engine_meta={
            "status": "ok",
            "method_version": "v2",
            "last_recompute_date": "2026-03-05",
            "factor_returns_latest_date": "2026-03-03",
            "cross_section_min_age_days": 7,
            "recompute_interval_days": 7,
            "lookback_days": 504,
            "specific_risk_ticker_count": 2,
        },
        recomputed_this_refresh=False,
        recompute_reason="risk_engine_reused",
        cov_payload={"factors": ["style_beta_score"], "matrix": [[1.0]]},
        specific_risk_by_security={"AAPL.OQ": {"ticker": "AAPL", "specific_var": 0.01}},
        positions=[{"ticker": "AAPL", "weight": 1.0, "market_value": 100.0, "exposures": {"style_beta_score": 1.1}}],
        total_value=100.0,
        risk_shares={"market": 0.0, "industry": 20.0, "style": 30.0, "idio": 50.0},
        component_shares={"market": 0.0, "industry": 0.4, "style": 0.6},
        factor_details=[
            {"factor_id": "style_beta_score", "exposure": 0.1, "sensitivity": 0.01, "factor_vol": 0.05, "pct_of_total": 3.0}
        ],
        cov_matrix={"factors": ["style_beta_score"], "correlation": [[1.0]]},
        latest_r2=0.35,
        universe_loadings={
            "as_of_date": "2026-03-03",
            "latest_available_asof": "2026-03-04",
            "factors": ["style_beta_score"],
            "factor_vols": {"style_beta_score": 0.05},
            "factor_catalog": [],
            "ticker_count": 1,
            "eligible_ticker_count": 1,
            "core_estimated_ticker_count": 1,
            "projected_only_ticker_count": 0,
            "ineligible_ticker_count": 0,
            "by_ticker": {"AAPL": {"ticker": "AAPL", "price": 100.0, "exposures": {"style_beta_score": 1.1}}},
        },
        exposure_modes={"raw": [], "sensitivity": [], "risk_contribution": []},
        factor_catalog=[],
        cuse4_foundation={"status": "skipped"},
        data_db=data_db,
        cache_db=cache_db,
    )
    cache_sqlite.cache_publish_snapshot(staged_first["snapshot_id"])

    monkeypatch.setattr(
        cache_publisher,
        "compute_health_diagnostics",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("health diagnostics should have been reused")),
    )

    staged_second = cache_publisher.stage_refresh_cache_snapshot(
        run_id="run_stage_b",
        refresh_mode="light",
        refresh_started_at="2026-03-05T01:00:00Z",
        source_dates={
            "fundamentals_asof": "2026-03-04",
            "exposures_asof": "2026-03-03",
            "exposures_latest_available_asof": "2026-03-04",
        },
        snapshot_build={"status": "skipped"},
        risk_engine_meta={
            "status": "ok",
            "method_version": "v2",
            "last_recompute_date": "2026-03-05",
            "factor_returns_latest_date": "2026-03-03",
            "cross_section_min_age_days": 7,
            "recompute_interval_days": 7,
            "lookback_days": 504,
            "specific_risk_ticker_count": 2,
        },
        recomputed_this_refresh=False,
        recompute_reason="risk_engine_reused",
        cov_payload={"factors": ["style_beta_score"], "matrix": [[1.0]]},
        specific_risk_by_security={"AAPL.OQ": {"ticker": "AAPL", "specific_var": 0.01}},
        positions=[{"ticker": "AAPL", "weight": 1.0, "market_value": 100.0, "exposures": {"style_beta_score": 1.1}}],
        total_value=100.0,
        risk_shares={"market": 0.0, "industry": 20.0, "style": 30.0, "idio": 50.0},
        component_shares={"market": 0.0, "industry": 0.4, "style": 0.6},
        factor_details=[
            {"factor_id": "style_beta_score", "exposure": 0.1, "sensitivity": 0.01, "factor_vol": 0.05, "pct_of_total": 3.0}
        ],
        cov_matrix={"factors": ["style_beta_score"], "correlation": [[1.0]]},
        latest_r2=0.35,
        universe_loadings={
            "as_of_date": "2026-03-03",
            "latest_available_asof": "2026-03-04",
            "factors": ["style_beta_score"],
            "factor_vols": {"style_beta_score": 0.05},
            "factor_catalog": [],
            "ticker_count": 1,
            "eligible_ticker_count": 1,
            "core_estimated_ticker_count": 1,
            "projected_only_ticker_count": 0,
            "ineligible_ticker_count": 0,
            "by_ticker": {"AAPL": {"ticker": "AAPL", "price": 100.0, "exposures": {"style_beta_score": 1.1}}},
        },
        exposure_modes={"raw": [], "sensitivity": [], "risk_contribution": []},
        factor_catalog=[],
        cuse4_foundation={"status": "skipped"},
        data_db=data_db,
        cache_db=cache_db,
    )

    assert staged_second["health_refreshed"] is False
    assert staged_second["health_refresh_state"] == "carried_forward"


def test_stage_refresh_cache_snapshot_defers_health_diagnostics_when_quick_refresh_has_none(
    monkeypatch,
    tmp_path: Path,
) -> None:
    cache_db = tmp_path / "cache.db"
    data_db = tmp_path / "data.db"
    data_db.touch()

    monkeypatch.setattr(config, "SQLITE_PATH", str(cache_db))
    monkeypatch.setattr(cache_sqlite, "_SCHEMA_READY", False)
    monkeypatch.setattr(cache_sqlite, "_SCHEMA_READY_PATH", None)
    monkeypatch.setattr(
        cache_publisher,
        "compute_health_diagnostics",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("quick refresh should not recompute health diagnostics")),
    )

    staged = cache_publisher.stage_refresh_cache_snapshot(
        run_id="run_stage_deferred_health",
        refresh_mode="light",
        refresh_started_at="2026-03-05T01:00:00Z",
        source_dates={
            "fundamentals_asof": "2026-03-04",
            "exposures_asof": "2026-03-04",
            "exposures_latest_available_asof": "2026-03-04",
        },
        snapshot_build={"status": "skipped"},
        risk_engine_meta={
            "status": "ok",
            "method_version": "v2",
            "last_recompute_date": "2026-03-05",
            "factor_returns_latest_date": "2026-03-03",
            "cross_section_min_age_days": 7,
            "recompute_interval_days": 7,
            "lookback_days": 504,
            "specific_risk_ticker_count": 2,
        },
        recomputed_this_refresh=False,
        recompute_reason="risk_engine_reused",
        cov_payload={"factors": ["style_beta_score"], "matrix": [[1.0]]},
        specific_risk_by_security={"AAPL.OQ": {"ticker": "AAPL", "specific_var": 0.01}},
        positions=[{"ticker": "AAPL", "weight": 1.0, "market_value": 100.0, "exposures": {"style_beta_score": 1.1}}],
        total_value=100.0,
        risk_shares={"market": 0.0, "industry": 20.0, "style": 30.0, "idio": 50.0},
        component_shares={"market": 0.0, "industry": 0.4, "style": 0.6},
        factor_details=[
            {"factor_id": "style_beta_score", "exposure": 0.1, "sensitivity": 0.01, "factor_vol": 0.05, "pct_of_total": 3.0}
        ],
        cov_matrix={"factors": ["style_beta_score"], "correlation": [[1.0]]},
        latest_r2=0.35,
        universe_loadings={
            "as_of_date": "2026-03-03",
            "latest_available_asof": "2026-03-04",
            "factors": ["style_beta_score"],
            "factor_vols": {"style_beta_score": 0.05},
            "factor_catalog": [],
            "ticker_count": 1,
            "eligible_ticker_count": 1,
            "core_estimated_ticker_count": 1,
            "projected_only_ticker_count": 0,
            "ineligible_ticker_count": 0,
            "by_ticker": {"AAPL": {"ticker": "AAPL", "price": 100.0, "exposures": {"style_beta_score": 1.1}}},
        },
        exposure_modes={"raw": [], "sensitivity": [], "risk_contribution": []},
        factor_catalog=[],
        cuse4_foundation={"status": "skipped"},
        data_db=data_db,
        cache_db=cache_db,
    )

    assert staged["health_refreshed"] is False
    assert staged["health_refresh_state"] == "deferred"
    health_payload = staged["persisted_payloads"]["health_diagnostics"]
    assert health_payload["status"] == "deferred"
    assert health_payload["diagnostics_refresh_state"] == "deferred"


def test_stage_refresh_cache_snapshot_upgrades_stale_exposure_source_dates(
    monkeypatch,
    tmp_path: Path,
) -> None:
    cache_db = tmp_path / "cache.db"
    data_db = tmp_path / "data.db"
    data_db.touch()

    monkeypatch.setattr(config, "SQLITE_PATH", str(cache_db))
    monkeypatch.setattr(cache_sqlite, "_SCHEMA_READY", False)
    monkeypatch.setattr(cache_sqlite, "_SCHEMA_READY_PATH", None)
    monkeypatch.setattr(
        cache_publisher,
        "compute_health_diagnostics",
        lambda *args, **kwargs: {"status": "ok"},
    )
    monkeypatch.setattr(
        refresh_metadata,
        "load_latest_eligibility_summary",
        lambda _cache_db: {
            "status": "ok",
            "date": "2026-03-13",
            "exp_date": "2026-03-13",
            "latest_available_date": "2026-03-13",
            "regression_coverage": 0.997,
            "structural_eligible_n": 3651,
            "core_structural_eligible_n": 3455,
            "projectable_n": 3639,
            "projected_only_n": 193,
            "drop_pct_from_prev": 0.0,
            "alert_level": "",
            "selection_mode": "well_covered",
        },
    )

    staged = cache_publisher.stage_refresh_cache_snapshot(
        run_id="run_stage_fresh_dates",
        refresh_mode="full",
        refresh_started_at="2026-03-16T08:45:26Z",
        source_dates={
            "fundamentals_asof": "2026-02-27",
            "classification_asof": "2026-02-27",
            "prices_asof": "2026-03-13",
            "exposures_asof": "2026-03-04",
            "exposures_latest_available_asof": "2026-03-04",
            "exposures_served_asof": "2026-03-03",
        },
        snapshot_build={"status": "skipped"},
        risk_engine_meta={
            "status": "ok",
            "method_version": "v8",
            "last_recompute_date": "2026-03-16",
            "factor_returns_latest_date": "2026-03-13",
            "cross_section_min_age_days": 7,
            "recompute_interval_days": 7,
            "lookback_days": 504,
            "specific_risk_ticker_count": 3736,
        },
        recomputed_this_refresh=True,
        recompute_reason="method_version_change",
        cov_payload={"factors": ["style_beta_score"], "matrix": [[1.0]]},
        specific_risk_by_security={"AAPL.OQ": {"ticker": "AAPL", "specific_var": 0.01}},
        positions=[{"ticker": "AAPL", "weight": 1.0, "market_value": 100.0, "exposures": {"style_beta_score": 1.1}}],
        total_value=100.0,
        risk_shares={"market": 3.0, "industry": 24.0, "style": 11.0, "idio": 62.0},
        component_shares={"market": 0.1, "industry": 0.2, "style": 0.7},
        factor_details=[
            {"factor_id": "style_beta_score", "exposure": 0.1, "sensitivity": 0.01, "factor_vol": 0.05, "pct_of_total": 3.0}
        ],
        cov_matrix={"factors": ["style_beta_score"], "correlation": [[1.0]]},
        latest_r2=0.35,
        universe_loadings={
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
            "by_ticker": {"AAPL": {"ticker": "AAPL", "price": 100.0, "exposures": {"style_beta_score": 1.1}}},
        },
        exposure_modes={"raw": [], "sensitivity": [], "risk_contribution": []},
        factor_catalog=[],
        cuse4_foundation={"status": "skipped"},
        recompute_health_diagnostics=True,
        data_db=data_db,
        cache_db=cache_db,
    )

    cache_sqlite.cache_publish_snapshot(staged["snapshot_id"])

    refresh_meta = cache_sqlite.cache_get("refresh_meta")
    assert isinstance(refresh_meta, dict)
    assert refresh_meta["source_dates"]["fundamentals_asof"] == "2026-02-27"
    assert refresh_meta["source_dates"]["classification_asof"] == "2026-02-27"
    assert refresh_meta["source_dates"]["exposures_served_asof"] == "2026-03-13"
    assert refresh_meta["source_dates"]["exposures_latest_available_asof"] == "2026-03-13"
    assert refresh_meta["health_refresh_state"] == "recomputed"

    risk_payload = staged["persisted_payloads"]["risk"]
    assert risk_payload["source_dates"]["fundamentals_asof"] == "2026-02-27"
    assert risk_payload["source_dates"]["classification_asof"] == "2026-02-27"
    assert risk_payload["risk_engine"]["core_state_through_date"] == "2026-03-13"
    assert risk_payload["risk_engine"]["core_rebuild_date"] == "2026-03-16"
    assert risk_payload["risk_engine"]["estimation_exposure_anchor_date"] == "2026-03-06"


def test_stage_refresh_cache_snapshot_refreshes_stale_eligibility_summary_from_current_snapshot(
    monkeypatch,
    tmp_path: Path,
) -> None:
    cache_db = tmp_path / "cache.db"
    data_db = tmp_path / "data.db"
    data_db.touch()

    monkeypatch.setattr(config, "SQLITE_PATH", str(cache_db))
    monkeypatch.setattr(cache_sqlite, "_SCHEMA_READY", False)
    monkeypatch.setattr(cache_sqlite, "_SCHEMA_READY_PATH", None)
    monkeypatch.setattr(cache_publisher, "compute_health_diagnostics", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(
        refresh_metadata,
        "load_latest_eligibility_summary",
        lambda _cache_db: {
            "status": "ok",
            "date": "2017-01-04",
            "exp_date": "2026-03-13",
            "latest_available_date": "2017-01-04",
            "regression_coverage": 0.994,
            "structural_eligible_n": 2717,
            "core_structural_eligible_n": 2587,
            "projectable_n": 2703,
            "projected_only_n": 130,
            "selection_mode": "well_covered",
        },
    )

    staged = cache_publisher.stage_refresh_cache_snapshot(
        run_id="run_stage_refresh_eligibility",
        refresh_mode="light",
        refresh_started_at="2026-03-17T03:10:00Z",
        source_dates={
            "fundamentals_asof": "2026-03-13",
            "classification_asof": "2026-03-13",
            "prices_asof": "2026-03-13",
            "exposures_asof": "2026-03-13",
            "exposures_latest_available_asof": "2026-03-13",
            "exposures_served_asof": "2026-03-13",
        },
        snapshot_build={"status": "skipped"},
        risk_engine_meta={
            "status": "ok",
            "method_version": "v8",
            "last_recompute_date": "2026-03-16",
            "factor_returns_latest_date": "2026-03-13",
            "cross_section_min_age_days": 7,
            "recompute_interval_days": 7,
            "lookback_days": 504,
            "specific_risk_ticker_count": 3736,
        },
        recomputed_this_refresh=False,
        recompute_reason="risk_engine_reused",
        cov_payload={"factors": ["style_beta_score"], "matrix": [[1.0]]},
        specific_risk_by_security={"AAPL.OQ": {"ticker": "AAPL", "specific_var": 0.01}},
        positions=[{"ticker": "AAPL", "weight": 1.0, "market_value": 100.0, "exposures": {"style_beta_score": 1.1}}],
        total_value=100.0,
        risk_shares={"market": 3.0, "industry": 24.0, "style": 11.0, "idio": 62.0},
        component_shares={"market": 0.1, "industry": 0.2, "style": 0.7},
        factor_details=[
            {"factor_id": "style_beta_score", "exposure": 0.1, "sensitivity": 0.01, "factor_vol": 0.05, "pct_of_total": 3.0}
        ],
        cov_matrix={"factors": ["style_beta_score"], "correlation": [[1.0]]},
        latest_r2=0.35,
        universe_loadings={
            "as_of_date": "2026-03-13",
            "latest_available_asof": "2026-03-13",
            "factors": ["style_beta_score"],
            "factor_vols": {"style_beta_score": 0.05},
            "factor_catalog": [],
            "ticker_count": 3446,
            "eligible_ticker_count": 3390,
            "core_estimated_ticker_count": 3210,
            "projected_only_ticker_count": 180,
            "ineligible_ticker_count": 56,
            "by_ticker": {"AAPL": {"ticker": "AAPL", "price": 100.0, "exposures": {"style_beta_score": 1.1}}},
        },
        exposure_modes={"raw": [], "sensitivity": [], "risk_contribution": []},
        factor_catalog=[],
        cuse4_foundation={"status": "skipped"},
        data_db=data_db,
        cache_db=cache_db,
    )

    eligibility = staged["persisted_payloads"]["eligibility"]
    sanity = staged["persisted_payloads"]["model_sanity"]

    assert eligibility["date"] == "2026-03-13"
    assert eligibility["exp_date"] == "2026-03-13"
    assert eligibility["latest_available_date"] == "2026-03-13"
    assert eligibility["selection_mode"] == "serving_snapshot"
    assert eligibility["structural_eligible_n"] == 3390
    assert eligibility["core_structural_eligible_n"] == 3210
    assert eligibility["regression_member_n"] == 3210
    assert eligibility["projectable_n"] == 3390
    assert eligibility["projected_only_n"] == 180
    assert sanity["served_loadings_asof"] == "2026-03-13"
    assert sanity["latest_loadings_available_asof"] == "2026-03-13"
    assert sanity["coverage_date"] == "2026-03-13"
    assert sanity["latest_available_date"] == "2026-03-13"
    assert staged["persisted_payloads"]["risk"]["risk_engine"]["core_state_through_date"] == "2026-03-13"
    assert staged["persisted_payloads"]["risk"]["risk_engine"]["core_rebuild_date"] == "2026-03-16"
    assert staged["persisted_payloads"]["risk"]["risk_engine"]["estimation_exposure_anchor_date"] == "2026-03-06"
