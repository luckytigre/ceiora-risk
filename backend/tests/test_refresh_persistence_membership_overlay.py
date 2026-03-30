from __future__ import annotations

import sqlite3
from pathlib import Path

from backend.analytics import refresh_persistence


def _seed_membership_rows(data_db: Path) -> None:
    conn = sqlite3.connect(str(data_db))
    conn.execute(
        """
        CREATE TABLE cuse_security_membership_daily (
            as_of_date TEXT NOT NULL,
            ric TEXT,
            ticker TEXT NOT NULL,
            policy_path TEXT NOT NULL,
            realized_role TEXT NOT NULL,
            output_status TEXT NOT NULL,
            projection_candidate_status TEXT NOT NULL,
            projection_output_status TEXT NOT NULL,
            reason_code TEXT,
            quality_label TEXT NOT NULL,
            source_snapshot_status TEXT NOT NULL,
            projection_method TEXT,
            projection_basis_status TEXT NOT NULL,
            projection_source_package_date TEXT,
            served_exposure_available INTEGER NOT NULL DEFAULT 0,
            run_id TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (as_of_date, ticker)
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO cuse_security_membership_daily (
            as_of_date, ric, ticker, policy_path, realized_role, output_status,
            projection_candidate_status, projection_output_status, reason_code,
            quality_label, source_snapshot_status, projection_method,
            projection_basis_status, projection_source_package_date,
            served_exposure_available, run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "2026-03-26",
                "SPY.P",
                "SPY",
                "returns_projection_candidate",
                "ineligible",
                "projection_unavailable",
                "candidate",
                "unavailable",
                "projection_unavailable",
                "projection_unavailable",
                "served_snapshot",
                None,
                "unavailable",
                "2026-03-26",
                0,
                "run_1",
                "2026-03-26T00:01:00Z",
            ),
            (
                "2026-03-26",
                "QQQ.OQ",
                "QQQ",
                "returns_projection_candidate",
                "projected_returns",
                "served",
                "candidate",
                "available",
                "projected_returns_regression",
                "projected",
                "served_snapshot",
                "peer_returns_regression",
                "available",
                "2026-03-26",
                1,
                "run_1",
                "2026-03-26T00:01:00Z",
            ),
            (
                "2026-03-26",
                "ASML.OQ",
                "ASML",
                "fundamental_projection_candidate",
                "ineligible",
                "unavailable",
                "candidate",
                "unavailable",
                "unavailable",
                "blocked",
                "served_snapshot",
                None,
                "not_applicable",
                None,
                0,
                "run_1",
                "2026-03-26T00:01:00Z",
            ),
        ],
    )
    conn.commit()
    conn.close()


def test_persist_refresh_outputs_overlays_current_membership_truth_before_serving_publish(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"
    _seed_membership_rows(data_db)

    captured: dict[str, object] = {}

    monkeypatch.setattr(
        refresh_persistence.model_outputs,
        "persist_model_outputs",
        lambda **kwargs: {"status": "ok", "run_id": kwargs["run_id"]},
    )
    monkeypatch.setattr(
        refresh_persistence.serving_outputs,
        "persist_current_payloads",
        lambda **kwargs: captured.update({"payloads": kwargs["payloads"]}) or {"status": "ok"},
    )
    monkeypatch.setattr(refresh_persistence.runtime_state, "persist_runtime_state", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(refresh_persistence.runtime_state, "publish_active_snapshot", lambda *args, **kwargs: {"status": "ok"})

    persisted_payloads = {
        "universe_loadings": {
            "as_of_date": "2026-03-26",
            "index": [
                {"ticker": "SPY", "ric": "SPY.P", "model_status": "ineligible", "model_status_reason": "ineligible"},
                {"ticker": "QQQ", "ric": "QQQ.OQ", "model_status": "projected_only", "model_status_reason": "projected_returns_regression"},
                {"ticker": "ASML", "ric": "ASML.OQ", "model_status": "ineligible", "model_status_reason": "ineligible"},
            ],
            "by_ticker": {
                "SPY": {
                    "ticker": "SPY",
                    "ric": "SPY.P",
                    "as_of_date": "2026-03-26",
                    "model_status": "ineligible",
                    "model_status_reason": "ineligible",
                    "eligibility_reason": "ineligible",
                    "exposure_origin": "native",
                    "model_warning": "Ticker is ineligible for strict equity model; analytics shown as N/A.",
                    "exposures": {},
                },
                "QQQ": {
                    "ticker": "QQQ",
                    "ric": "QQQ.OQ",
                    "as_of_date": "2026-03-26",
                    "model_status": "projected_only",
                    "model_status_reason": "projected_returns_regression",
                    "eligibility_reason": "projected_returns_regression",
                    "exposure_origin": "projected_returns",
                    "model_warning": "",
                    "projection_method": "peer_returns_regression",
                    "projection_asof": "2026-03-26",
                    "exposures": {"Market": 1.0},
                },
                "ASML": {
                    "ticker": "ASML",
                    "ric": "ASML.OQ",
                    "as_of_date": "2026-03-26",
                    "model_status": "ineligible",
                    "model_status_reason": "ineligible",
                    "eligibility_reason": "ineligible",
                    "exposure_origin": "native",
                    "model_warning": "Ticker is ineligible for strict equity model; analytics shown as N/A.",
                    "exposures": {},
                },
            },
            "ticker_count": 2,
            "eligible_ticker_count": 0,
            "core_estimated_ticker_count": 0,
            "projected_only_ticker_count": 0,
            "ineligible_ticker_count": 2,
        },
        "universe_factors": {
            "ticker_count": 2,
            "eligible_ticker_count": 0,
            "core_estimated_ticker_count": 0,
            "projected_only_ticker_count": 0,
            "ineligible_ticker_count": 2,
        },
        "portfolio": {
            "positions": [
                {"ticker": "SPY", "model_status": "ineligible", "exposure_origin": "native", "market_value": 100.0},
                {"ticker": "QQQ", "model_status": "projected_only", "exposure_origin": "projected_returns", "market_value": 100.0},
                {"ticker": "ASML", "model_status": "ineligible", "exposure_origin": "native", "market_value": 100.0},
            ],
            "position_count": 3,
            "total_value": 300.0,
        },
        "exposures": {
            "raw": [
                {
                    "factor_id": "market",
                    "drilldown": [
                        {"ticker": "SPY", "model_status": "ineligible", "exposure_origin": "native"},
                        {"ticker": "QQQ", "model_status": "projected_only", "exposure_origin": "projected_returns"},
                        {"ticker": "ASML", "model_status": "ineligible", "exposure_origin": "native"},
                    ],
                }
            ],
            "sensitivity": [],
            "risk_contribution": [],
        },
    }

    refresh_persistence.persist_refresh_outputs(
        data_db=data_db,
        cache_db=cache_db,
        run_id="run_1",
        snapshot_id="run_1",
        refresh_mode="light",
        refresh_started_at="2026-03-26T00:00:00Z",
        recomputed_this_refresh=True,
        params={},
        source_dates={"exposures_asof": "2026-03-26"},
        risk_engine_state={"core_state_through_date": "2026-03-26"},
        cov=None,
        specific_risk_by_security={},
        persisted_payloads=persisted_payloads,
    )

    published_payloads = dict(captured["payloads"])
    published_universe = published_payloads["universe_loadings"]["by_ticker"]

    assert published_universe["SPY"]["model_status_reason"] == "projection_unavailable"
    assert published_universe["SPY"]["exposure_origin"] == "projected_returns"
    assert "Returns-projection candidate" in published_universe["SPY"]["model_warning"]
    assert published_universe["QQQ"]["model_status_reason"] == "projected_returns_regression"
    assert published_universe["QQQ"]["exposure_origin"] == "projected_returns"
    assert published_universe["QQQ"]["model_warning"] == ""
    assert published_universe["ASML"]["model_status_reason"] == "unavailable"
    assert published_universe["ASML"]["exposure_origin"] == "projected_fundamental"
    assert "Fundamental-projection candidate" in published_universe["ASML"]["model_warning"]

    portfolio_by_ticker = {
        row["ticker"]: row
        for row in published_payloads["portfolio"]["positions"]
    }
    assert portfolio_by_ticker["SPY"]["exposure_origin"] == "projected_returns"
    assert portfolio_by_ticker["QQQ"]["exposure_origin"] == "projected_returns"
    assert portfolio_by_ticker["ASML"]["exposure_origin"] == "projected_fundamental"

    drilldown = published_payloads["exposures"]["raw"][0]["drilldown"]
    drilldown_by_ticker = {row["ticker"]: row for row in drilldown}
    assert drilldown_by_ticker["SPY"]["exposure_origin"] == "projected_returns"
    assert drilldown_by_ticker["QQQ"]["exposure_origin"] == "projected_returns"
    assert drilldown_by_ticker["ASML"]["exposure_origin"] == "projected_fundamental"
