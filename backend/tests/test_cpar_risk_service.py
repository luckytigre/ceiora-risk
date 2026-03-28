from __future__ import annotations

import pytest

from backend.services import cpar_aggregate_risk_service
from backend.services import cpar_meta_service
from backend.services import cpar_risk_service


def test_load_cpar_risk_payload_uses_aggregate_snapshot_owner(monkeypatch) -> None:
    calls: dict[str, object] = {}

    monkeypatch.setattr(
        cpar_aggregate_risk_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_aggregate_context",
        lambda **kwargs: (
            {"package_run_id": "run_curr", "package_date": "2026-03-14"},
            [{"account_id": "acct_a", "account_name": "Account A"}],
            [{"ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 6.0}],
        ),
    )

    def fake_support_rows(**kwargs):
        calls["support_rows"] = kwargs
        return (
            {"AAPL.OQ": {"ric": "AAPL.OQ", "fit_status": "ok", "thresholded_loadings": {"SPY": 1.1}}},
            {"AAPL.OQ": {"ric": "AAPL.OQ", "adj_close": 201.0, "date": "2026-03-14"}},
            {"AAPL.OQ": {"ric": "AAPL.OQ", "trbc_industry_group": "Technology Hardware"}},
            [{"factor_id": "SPY", "factor_id_2": "SPY", "covariance": 0.04, "correlation": 1.0}],
        )

    monkeypatch.setattr(
        cpar_aggregate_risk_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_support_rows",
        fake_support_rows,
    )
    monkeypatch.setattr(
        cpar_aggregate_risk_service.cpar_display_covariance,
        "load_package_display_covariance_rows",
        lambda **kwargs: [{"factor_id": "SPY", "factor_id_2": "SPY", "covariance": 0.03, "correlation": 1.0}],
    )

    def fake_build_snapshot(**kwargs):
        calls["build_snapshot"] = kwargs
        return {"scope": "all_accounts", "positions_count": 1}

    monkeypatch.setattr(
        cpar_aggregate_risk_service,
        "build_cpar_risk_snapshot",
        fake_build_snapshot,
    )

    payload = cpar_aggregate_risk_service.load_cpar_risk_payload()

    assert payload["scope"] == "all_accounts"
    assert calls["support_rows"] == {
        "rics": ["AAPL.OQ"],
        "package_run_id": "run_curr",
        "package_date": "2026-03-14",
        "data_db": None,
    }
    assert calls["build_snapshot"]["accounts"] == [{"account_id": "acct_a", "account_name": "Account A"}]
    assert calls["build_snapshot"]["classification_by_ric"] == {
        "AAPL.OQ": {"ric": "AAPL.OQ", "trbc_industry_group": "Technology Hardware"}
    }
    assert calls["build_snapshot"]["display_covariance_rows"] == [
        {"factor_id": "SPY", "factor_id_2": "SPY", "covariance": 0.03, "correlation": 1.0}
    ]


def test_load_cpar_risk_payload_maps_display_covariance_not_ready(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_aggregate_risk_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_aggregate_context",
        lambda **kwargs: (
            {"package_run_id": "run_curr", "package_date": "2026-03-14"},
            [],
            [],
        ),
    )
    monkeypatch.setattr(
        cpar_aggregate_risk_service.cpar_display_covariance,
        "load_package_display_covariance_rows",
        lambda **kwargs: (_ for _ in ()).throw(cpar_aggregate_risk_service.cpar_outputs.CparPackageNotReady("display not ready")),
    )
    monkeypatch.setattr(
        cpar_aggregate_risk_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_support_rows",
        lambda **kwargs: ({}, {}, {}, []),
    )

    with pytest.raises(cpar_meta_service.CparReadNotReady, match="display not ready"):
        cpar_aggregate_risk_service.load_cpar_risk_payload()


def test_load_cpar_risk_payload_maps_display_covariance_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_aggregate_risk_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_aggregate_context",
        lambda **kwargs: (
            {"package_run_id": "run_curr", "package_date": "2026-03-14"},
            [],
            [],
        ),
    )
    monkeypatch.setattr(
        cpar_aggregate_risk_service.cpar_display_covariance,
        "load_package_display_covariance_rows",
        lambda **kwargs: (_ for _ in ()).throw(cpar_aggregate_risk_service.cpar_outputs.CparAuthorityReadError("display unavailable")),
    )
    monkeypatch.setattr(
        cpar_aggregate_risk_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_support_rows",
        lambda **kwargs: ({}, {}, {}, []),
    )

    with pytest.raises(cpar_meta_service.CparReadUnavailable, match="display unavailable"):
        cpar_aggregate_risk_service.load_cpar_risk_payload()


def test_load_cpar_risk_payload_propagates_support_row_fail_closed_state(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_aggregate_risk_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_aggregate_context",
        lambda **kwargs: (
            {"package_run_id": "run_curr", "package_date": "2026-03-14"},
            [],
            [{"ric": "AAPL.OQ"}],
        ),
    )
    monkeypatch.setattr(
        cpar_aggregate_risk_service.cpar_display_covariance,
        "load_package_display_covariance_rows",
        lambda **kwargs: [{"factor_id": "SPY", "factor_id_2": "SPY", "covariance": 1.0, "correlation": 1.0}],
    )
    monkeypatch.setattr(
        cpar_aggregate_risk_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_support_rows",
        lambda **kwargs: (_ for _ in ()).throw(cpar_meta_service.CparReadUnavailable("support unavailable")),
    )

    with pytest.raises(cpar_meta_service.CparReadUnavailable, match="support unavailable"):
        cpar_aggregate_risk_service.load_cpar_risk_payload()


def test_cpar_risk_service_delegates_to_aggregate_owner(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_risk_service.cpar_aggregate_risk_service,
        "load_cpar_risk_payload",
        lambda **kwargs: {"scope": "all_accounts", "positions_count": 1},
    )

    payload = cpar_risk_service.load_cpar_risk_payload()

    assert payload == {"scope": "all_accounts", "positions_count": 1}
