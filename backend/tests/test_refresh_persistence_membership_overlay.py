from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

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


def _membership_payload_row(
    *,
    as_of_date: str,
    ric: str,
    ticker: str,
    policy_path: str,
    realized_role: str,
    output_status: str,
    projection_candidate_status: str,
    projection_output_status: str,
    reason_code: str | None,
    quality_label: str,
    source_snapshot_status: str,
    projection_method: str | None,
    projection_basis_status: str,
    projection_source_package_date: str | None,
    served_exposure_available: int,
    run_id: str = "run_1",
    updated_at: str = "2026-03-26T00:01:00Z",
) -> tuple[object, ...]:
    return (
        as_of_date,
        ric,
        ticker,
        policy_path,
        realized_role,
        output_status,
        projection_candidate_status,
        projection_output_status,
        reason_code,
        quality_label,
        source_snapshot_status,
        projection_method,
        projection_basis_status,
        projection_source_package_date,
        served_exposure_available,
        run_id,
        updated_at,
    )


def test_persist_refresh_outputs_overlays_current_run_membership_truth_before_serving_publish(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"

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
    monkeypatch.setattr(
        refresh_persistence,
        "build_cuse_membership_payloads",
        lambda **kwargs: (
            [
                _membership_payload_row(
                    as_of_date="2026-03-26",
                    ric="SPY.P",
                    ticker="SPY",
                    policy_path="returns_projection_candidate",
                    realized_role="ineligible",
                    output_status="projection_unavailable",
                    projection_candidate_status="candidate",
                    projection_output_status="unavailable",
                    reason_code="projection_unavailable",
                    quality_label="projection_unavailable",
                    source_snapshot_status="served_snapshot",
                    projection_method=None,
                    projection_basis_status="unavailable",
                    projection_source_package_date="2026-03-26",
                    served_exposure_available=0,
                ),
                _membership_payload_row(
                    as_of_date="2026-03-26",
                    ric="QQQ.OQ",
                    ticker="QQQ",
                    policy_path="returns_projection_candidate",
                    realized_role="projected_returns",
                    output_status="served",
                    projection_candidate_status="candidate",
                    projection_output_status="available",
                    reason_code="projected_returns_regression",
                    quality_label="projected",
                    source_snapshot_status="served_snapshot",
                    projection_method="peer_returns_regression",
                    projection_basis_status="available",
                    projection_source_package_date="2026-03-26",
                    served_exposure_available=1,
                ),
                _membership_payload_row(
                    as_of_date="2026-03-26",
                    ric="ASML.OQ",
                    ticker="ASML",
                    policy_path="fundamental_projection_candidate",
                    realized_role="ineligible",
                    output_status="unavailable",
                    projection_candidate_status="candidate",
                    projection_output_status="unavailable",
                    reason_code="unavailable",
                    quality_label="blocked",
                    source_snapshot_status="served_snapshot",
                    projection_method=None,
                    projection_basis_status="not_applicable",
                    projection_source_package_date=None,
                    served_exposure_available=0,
                ),
            ],
            [],
        ),
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

def test_persist_refresh_outputs_applies_current_run_membership_truth_even_when_universe_asof_is_older(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"

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
    monkeypatch.setattr(
        refresh_persistence,
        "build_cuse_membership_payloads",
        lambda **kwargs: (
            [
                _membership_payload_row(
                    as_of_date="2026-03-26",
                    ric="QQQ.OQ",
                    ticker="QQQ",
                    policy_path="returns_projection_candidate",
                    realized_role="projected_returns",
                    output_status="served",
                    projection_candidate_status="candidate",
                    projection_output_status="available",
                    reason_code="projected_returns_regression",
                    quality_label="projected",
                    source_snapshot_status="served_snapshot",
                    projection_method="peer_returns_regression",
                    projection_basis_status="available",
                    projection_source_package_date="2026-03-26",
                    served_exposure_available=1,
                ),
            ],
            [],
        ),
    )
    monkeypatch.setattr(refresh_persistence.runtime_state, "persist_runtime_state", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(refresh_persistence.runtime_state, "publish_active_snapshot", lambda *args, **kwargs: {"status": "ok"})

    persisted_payloads = {
        "universe_loadings": {
            "as_of_date": "2026-03-13",
            "index": [{"ticker": "QQQ", "ric": "QQQ.OQ"}],
            "by_ticker": {
                "QQQ": {
                    "ticker": "QQQ",
                    "ric": "QQQ.OQ",
                    "as_of_date": "2026-03-13",
                    "model_status": "ineligible",
                    "model_status_reason": "stale_snapshot",
                    "eligibility_reason": "stale_snapshot",
                    "exposure_origin": "native",
                    "model_warning": "",
                    "exposures": {"Market": 1.0},
                }
            },
        },
        "portfolio": {"positions": [], "position_count": 0, "total_value": 0.0},
        "exposures": {"raw": [], "sensitivity": [], "risk_contribution": []},
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
        source_dates={"exposures_asof": "2026-03-13"},
        risk_engine_state={"core_state_through_date": "2026-03-26"},
        cov=None,
        specific_risk_by_security={},
        persisted_payloads=persisted_payloads,
    )

    published_universe = dict(captured["payloads"]["universe_loadings"]["by_ticker"])
    assert published_universe["QQQ"]["model_status"] == "projected_only"
    assert published_universe["QQQ"]["model_status_reason"] == "projected_returns_regression"
    assert published_universe["QQQ"]["projection_method"] == "peer_returns_regression"


def test_persist_refresh_outputs_uses_current_run_mixed_date_membership_instead_of_latest_history_date(
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
    monkeypatch.setattr(
        refresh_persistence,
        "build_cuse_membership_payloads",
        lambda **kwargs: (
            [
                _membership_payload_row(
                    as_of_date="2026-03-31",
                    ric="AAL.OQ",
                    ticker="AAL",
                    policy_path="native_core_candidate",
                    realized_role="core_estimated",
                    output_status="served",
                    projection_candidate_status="not_applicable",
                    projection_output_status="not_applicable",
                    reason_code=None,
                    quality_label="native_core",
                    source_snapshot_status="observed_snapshot",
                    projection_method=None,
                    projection_basis_status="not_applicable",
                    projection_source_package_date=None,
                    served_exposure_available=1,
                ),
                _membership_payload_row(
                    as_of_date="2026-04-13",
                    ric="SPY.P",
                    ticker="SPY",
                    policy_path="returns_projection_candidate",
                    realized_role="projected_returns",
                    output_status="served",
                    projection_candidate_status="candidate",
                    projection_output_status="available",
                    reason_code="projected_returns_regression",
                    quality_label="returns_projection",
                    source_snapshot_status="served_snapshot",
                    projection_method="ols_returns_regression",
                    projection_basis_status="available",
                    projection_source_package_date="2026-04-13",
                    served_exposure_available=1,
                ),
            ],
            [],
        ),
    )
    monkeypatch.setattr(refresh_persistence.runtime_state, "persist_runtime_state", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(refresh_persistence.runtime_state, "publish_active_snapshot", lambda *args, **kwargs: {"status": "ok"})

    persisted_payloads = {
        "universe_loadings": {
            "as_of_date": "2026-03-31",
            "index": [{"ticker": "AAL", "ric": "AAL.OQ"}, {"ticker": "SPY", "ric": "SPY.P"}],
            "by_ticker": {
                "AAL": {
                    "ticker": "AAL",
                    "ric": "AAL.OQ",
                    "as_of_date": "2026-03-31",
                    "model_status": "core_estimated",
                    "model_status_reason": "",
                    "eligibility_reason": "",
                    "exposure_origin": "native",
                    "model_warning": "",
                    "exposures": {"Market": 1.0},
                },
                "SPY": {
                    "ticker": "SPY",
                    "ric": "SPY.P",
                    "as_of_date": "2026-04-13",
                    "model_status": "projected_only",
                    "model_status_reason": "returns_projection",
                    "eligibility_reason": "returns_projection",
                    "exposure_origin": "projected",
                    "model_warning": "",
                    "exposures": {"Market": 1.0},
                },
            },
        },
        "portfolio": {"positions": [], "position_count": 0, "total_value": 0.0},
        "exposures": {"raw": [], "sensitivity": [], "risk_contribution": []},
    }

    refresh_persistence.persist_refresh_outputs(
        data_db=data_db,
        cache_db=cache_db,
        run_id="run_2",
        snapshot_id="run_2",
        refresh_mode="light",
        refresh_started_at="2026-04-15T00:00:00Z",
        recomputed_this_refresh=False,
        params={},
        source_dates={"exposures_asof": "2026-03-31"},
        risk_engine_state={"core_state_through_date": "2026-04-13"},
        cov=None,
        specific_risk_by_security={},
        persisted_payloads=persisted_payloads,
    )

    published_universe = dict(captured["payloads"]["universe_loadings"]["by_ticker"])
    assert set(published_universe) == {"AAL", "SPY"}
    assert published_universe["AAL"]["model_status"] == "core_estimated"
    assert published_universe["SPY"]["model_status"] == "projected_only"
    assert published_universe["SPY"]["projection_asof"] == "2026-04-13"


def test_persist_refresh_outputs_fails_closed_when_membership_truth_is_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"

    monkeypatch.setattr(
        refresh_persistence.model_outputs,
        "persist_model_outputs",
        lambda **kwargs: {"status": "ok", "run_id": kwargs["run_id"]},
    )
    monkeypatch.setattr(
        refresh_persistence.serving_outputs,
        "persist_current_payloads",
        lambda **kwargs: {"status": "ok"},
    )
    monkeypatch.setattr(
        refresh_persistence,
        "build_cuse_membership_payloads",
        lambda **kwargs: ([], []),
    )
    monkeypatch.setattr(refresh_persistence.runtime_state, "persist_runtime_state", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(refresh_persistence.runtime_state, "publish_active_snapshot", lambda *args, **kwargs: {"status": "ok"})

    persisted_payloads = {
        "universe_loadings": {
            "as_of_date": "2026-03-26",
            "by_ticker": {
                "SPY": {
                    "ticker": "SPY",
                    "ric": "SPY.P",
                    "as_of_date": "2026-03-26",
                    "model_status": "projected_only",
                    "model_status_reason": "returns_projection",
                    "eligibility_reason": "returns_projection",
                    "exposure_origin": "native",
                    "model_warning": "",
                    "exposures": {"market": 1.0},
                },
            },
        },
        "portfolio": {
            "positions": [
                {"ticker": "SPY", "model_status": "projected_only", "exposure_origin": "native", "market_value": 100.0},
            ],
            "position_count": 1,
            "total_value": 100.0,
        },
        "exposures": {"raw": [], "sensitivity": [], "risk_contribution": []},
    }

    with pytest.raises(RuntimeError, match="Current cUSE membership truth is incomplete"):
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


def test_persist_refresh_outputs_drops_tickers_not_in_membership_when_membership_is_present(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Tickers with no membership row are silently dropped when the membership table
    is non-empty — e.g. ETF price tickers admitted by Neon prices but never modelled.
    Contrast with the fully-absent case above, which should fail hard."""
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"

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
    monkeypatch.setattr(
        refresh_persistence,
        "build_cuse_membership_payloads",
        lambda **kwargs: (
            [
                _membership_payload_row(
                    as_of_date="2026-03-26",
                    ric="SPY.P",
                    ticker="SPY",
                    policy_path="core",
                    realized_role="core_estimated",
                    output_status="served",
                    projection_candidate_status="not_applicable",
                    projection_output_status="not_applicable",
                    reason_code=None,
                    quality_label="core",
                    source_snapshot_status="served_snapshot",
                    projection_method=None,
                    projection_basis_status="not_applicable",
                    projection_source_package_date=None,
                    served_exposure_available=1,
                ),
                _membership_payload_row(
                    as_of_date="2026-03-26",
                    ric="QQQ.OQ",
                    ticker="QQQ",
                    policy_path="returns_projection_candidate",
                    realized_role="projected_returns",
                    output_status="served",
                    projection_candidate_status="candidate",
                    projection_output_status="available",
                    reason_code="projected_returns_regression",
                    quality_label="projected",
                    source_snapshot_status="served_snapshot",
                    projection_method="peer_returns_regression",
                    projection_basis_status="available",
                    projection_source_package_date="2026-03-26",
                    served_exposure_available=1,
                ),
            ],
            [],
        ),
    )
    monkeypatch.setattr(refresh_persistence.runtime_state, "persist_runtime_state", lambda *a, **kw: {"status": "ok"})
    monkeypatch.setattr(refresh_persistence.runtime_state, "publish_active_snapshot", lambda *a, **kw: {"status": "ok"})

    persisted_payloads = {
        "universe_loadings": {
            "as_of_date": "2026-03-26",
            "index": [
                {"ticker": "SPY", "ric": "SPY.P"},
                {"ticker": "QQQ", "ric": "QQQ.OQ"},
                {"ticker": "AAXJ", "ric": "AAXJ.O"},  # ETF — no membership row
            ],
            "by_ticker": {
                "SPY": {"ticker": "SPY", "ric": "SPY.P", "as_of_date": "2026-03-26",
                        "model_status": "core_estimated", "exposure_origin": "native",
                        "model_warning": "", "exposures": {"market": 0.9}},
                "QQQ": {"ticker": "QQQ", "ric": "QQQ.OQ", "as_of_date": "2026-03-26",
                        "model_status": "projected_only", "exposure_origin": "projected_returns",
                        "model_warning": "", "exposures": {"market": 1.0}},
                "AAXJ": {"ticker": "AAXJ", "ric": "AAXJ.O", "as_of_date": "2026-03-26",
                         "model_status": "ineligible", "exposure_origin": "native",
                         "model_warning": "", "exposures": {}},
            },
        },
        "portfolio": {"positions": [], "position_count": 0, "total_value": 0.0},
        "exposures": {"raw": [], "sensitivity": [], "risk_contribution": []},
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

    published_universe = captured["payloads"]["universe_loadings"]["by_ticker"]
    assert "SPY" in published_universe
    assert "QQQ" in published_universe
    assert "AAXJ" not in published_universe, "ETF ticker with no membership row must be dropped"

    published_index_tickers = {
        row["ticker"] for row in captured["payloads"]["universe_loadings"].get("index", [])
    }
    assert "AAXJ" not in published_index_tickers, "ETF ticker must also be dropped from index"


def test_persist_refresh_outputs_fails_closed_when_membership_only_covers_small_subset(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"

    monkeypatch.setattr(
        refresh_persistence.model_outputs,
        "persist_model_outputs",
        lambda **kwargs: {"status": "ok", "run_id": kwargs["run_id"]},
    )
    monkeypatch.setattr(
        refresh_persistence.serving_outputs,
        "persist_current_payloads",
        lambda **kwargs: {"status": "ok"},
    )
    monkeypatch.setattr(
        refresh_persistence,
        "build_cuse_membership_payloads",
        lambda **kwargs: (
            [
                _membership_payload_row(
                    as_of_date="2026-03-26",
                    ric="SPY.P",
                    ticker="SPY",
                    policy_path="core",
                    realized_role="core_estimated",
                    output_status="served",
                    projection_candidate_status="not_applicable",
                    projection_output_status="not_applicable",
                    reason_code=None,
                    quality_label="core",
                    source_snapshot_status="served_snapshot",
                    projection_method=None,
                    projection_basis_status="not_applicable",
                    projection_source_package_date=None,
                    served_exposure_available=1,
                ),
            ],
            [],
        ),
    )
    monkeypatch.setattr(refresh_persistence.runtime_state, "persist_runtime_state", lambda *a, **kw: {"status": "ok"})
    monkeypatch.setattr(refresh_persistence.runtime_state, "publish_active_snapshot", lambda *a, **kw: {"status": "ok"})

    by_ticker = {
        "SPY": {
            "ticker": "SPY",
            "ric": "SPY.P",
            "as_of_date": "2026-03-26",
            "model_status": "core_estimated",
            "exposure_origin": "native",
            "model_warning": "",
            "exposures": {"market": 1.0},
        }
    }
    index = [{"ticker": "SPY", "ric": "SPY.P"}]
    for idx in range(30):
        ticker = f"MISS{idx:02d}"
        ric = f"{ticker}.OQ"
        by_ticker[ticker] = {
            "ticker": ticker,
            "ric": ric,
            "as_of_date": "2026-03-26",
            "model_status": "core_estimated",
            "exposure_origin": "native",
            "model_warning": "",
            "exposures": {"market": 1.0},
        }
        index.append({"ticker": ticker, "ric": ric})

    persisted_payloads = {
        "universe_loadings": {
            "as_of_date": "2026-03-26",
            "index": index,
            "by_ticker": by_ticker,
        },
        "portfolio": {"positions": [], "position_count": 0, "total_value": 0.0},
        "exposures": {"raw": [], "sensitivity": [], "risk_contribution": []},
    }

    with pytest.raises(RuntimeError, match="membership coverage is too incomplete"):
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


def test_persist_refresh_outputs_blocks_universe_regression_vs_live_modeled_snapshot(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"
    monkeypatch.setattr(
        refresh_persistence.model_outputs,
        "persist_model_outputs",
        lambda **kwargs: {"status": "ok", "run_id": kwargs["run_id"]},
    )
    monkeypatch.setattr(
        refresh_persistence,
        "_overlay_current_membership_truth",
        lambda **kwargs: kwargs["persisted_payloads"],
    )
    monkeypatch.setattr(
        refresh_persistence,
        "build_cuse_membership_payloads",
        lambda **kwargs: ([], []),
    )
    monkeypatch.setattr(
        refresh_persistence.serving_outputs,
        "load_current_payload",
        lambda payload_name: {
            "by_ticker": {
                "AAPL": {"ticker": "AAPL", "model_status": "core_estimated", "exposure_origin": "native"},
                "ASML": {"ticker": "ASML", "model_status": "projected_only", "exposure_origin": "projected_fundamental"},
            }
        } if payload_name == "universe_loadings" else None,
    )
    monkeypatch.setattr(
        refresh_persistence.serving_outputs,
        "persist_current_payloads",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("publish should fail before persistence")),
    )
    monkeypatch.setattr(refresh_persistence.runtime_state, "persist_runtime_state", lambda *a, **kw: {"status": "ok"})
    monkeypatch.setattr(refresh_persistence.runtime_state, "publish_active_snapshot", lambda *a, **kw: {"status": "ok"})

    persisted_payloads = {
        "universe_loadings": {
            "as_of_date": "2026-03-26",
            "index": [{"ticker": "SPY", "ric": "SPY.P"}],
            "by_ticker": {
                "SPY": {
                    "ticker": "SPY",
                    "ric": "SPY.P",
                    "as_of_date": "2026-03-26",
                    "model_status": "core_estimated",
                    "exposure_origin": "native",
                    "model_warning": "",
                    "exposures": {"market": 1.0},
                },
            },
        },
        "portfolio": {"positions": [], "position_count": 0, "total_value": 0.0},
        "exposures": {"raw": [], "sensitivity": [], "risk_contribution": []},
    }

    with pytest.raises(RuntimeError, match="candidate universe regressed versus the current live modeled snapshot"):
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
