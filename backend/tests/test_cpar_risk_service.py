from __future__ import annotations

from backend.services import cpar_risk_service


def test_load_cpar_risk_payload_uses_aggregate_snapshot_owner(monkeypatch) -> None:
    calls: dict[str, object] = {}

    monkeypatch.setattr(
        cpar_risk_service.cpar_portfolio_snapshot_service,
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
        cpar_risk_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_support_rows",
        fake_support_rows,
    )

    def fake_build_snapshot(**kwargs):
        calls["build_snapshot"] = kwargs
        return {"scope": "all_accounts", "positions_count": 1}

    monkeypatch.setattr(
        cpar_risk_service.cpar_portfolio_snapshot_service,
        "build_cpar_risk_snapshot",
        fake_build_snapshot,
    )

    payload = cpar_risk_service.load_cpar_risk_payload()

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
