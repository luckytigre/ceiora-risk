from __future__ import annotations

from pathlib import Path

from backend import config
from backend.analytics.services import cache_publisher
from backend.data import sqlite as cache_sqlite


def _health_payload() -> dict:
    return {
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
    cache_sqlite.cache_set("health_diagnostics", _health_payload())
    cache_sqlite.cache_set("portfolio", {"version": "old"})

    staged = cache_publisher.stage_refresh_cache_snapshot(
        run_id="run_stage_1",
        refresh_mode="light",
        refresh_started_at="2026-03-05T00:00:00Z",
        source_dates={"fundamentals_asof": "2026-03-04", "exposures_asof": "2026-03-03"},
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
        cov_payload={"factors": ["Beta"], "matrix": [[1.0]]},
        specific_risk_by_security={"AAPL.OQ": {"ticker": "AAPL", "specific_var": 0.01}},
        positions=[{"ticker": "AAPL", "weight": 1.0, "exposures": {"Beta": 1.1}}],
        total_value=100.0,
        risk_shares={"country": 0.0, "industry": 20.0, "style": 30.0, "idio": 50.0},
        component_shares={"country": 0.0, "industry": 0.4, "style": 0.6},
        factor_details=[
            {"factor": "Beta", "exposure": 0.1, "sensitivity": 0.01, "factor_vol": 0.05, "pct_of_total": 3.0}
        ],
        cov_matrix={"factors": ["Beta"], "correlation": [[1.0]]},
        latest_r2=0.35,
        condition_number=123.0,
        universe_loadings={
            "factors": ["Beta"],
            "factor_vols": {"Beta": 0.05},
            "ticker_count": 1,
            "eligible_ticker_count": 1,
            "by_ticker": {"AAPL": {"ticker": "AAPL", "price": 100.0, "exposures": {"Beta": 1.1}}},
        },
        exposure_modes={"raw": [], "sensitivity": [], "risk_contribution": []},
        cuse4_foundation={"status": "skipped"},
        light_mode=True,
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
    refresh_meta = cache_sqlite.cache_get("refresh_meta")
    assert isinstance(refresh_meta, dict)
    assert refresh_meta.get("snapshot_id") == "run_stage_1"
