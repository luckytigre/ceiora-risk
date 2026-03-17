from __future__ import annotations

import pytest

from backend.services.dashboard_payload_service import (
    DashboardPayloadNotReady,
    load_exposures_response,
    load_portfolio_response,
    load_risk_response,
)


def _payload_loader_factory(mapping: dict[str, object]):
    def _loader(name: str, *, fallback_loader=None):
        return mapping.get(name)

    return _loader


def test_load_exposures_response_normalizes_factor_fields() -> None:
    payload = {
        "exposures": {
            "raw": [{"factor": "Momentum", "value": 1.0}],
            "sensitivity": [],
            "risk_contribution": [],
            "snapshot_id": "snap_1",
        }
    }
    response = load_exposures_response(
        mode="raw",
        payload_loader=_payload_loader_factory(payload),
        fallback_loader=lambda _key: None,
    )
    assert response["factors"][0]["factor_id"] == "Momentum"
    assert response["snapshot_id"] == "snap_1"


def test_load_risk_response_normalizes_country_fields() -> None:
    payload = {
        "risk": {
            "risk_shares": {"country": 1.0, "style": 99.0},
            "component_shares": {"country": 0.2, "style": 0.8},
            "factor_details": [{"factor": "Country: US", "category": "country"}],
            "cov_matrix": {"factors": ["market"], "correlation": [[1.0]]},
            "risk_engine": {"specific_risk_ticker_count": 1},
        },
        "model_sanity": {"status": "ok", "warnings": [], "checks": {}},
    }
    response = load_risk_response(
        payload_loader=_payload_loader_factory(payload),
        fallback_loader=lambda _key: None,
    )
    assert response["risk_shares"]["market"] == 1.0
    assert "country" not in response["risk_shares"]
    assert response["component_shares"]["market"] == 0.2
    assert response["factor_details"][0]["factor_id"] == "Country: US"
    assert response["factor_details"][0]["category"] == "market"


def test_load_portfolio_response_normalizes_positions() -> None:
    payload = {
        "portfolio": {
            "positions": [{"ticker": "AAPL"}],
            "position_count": 1,
            "total_value": 100.0,
        }
    }
    response = load_portfolio_response(
        payload_loader=_payload_loader_factory(payload),
        fallback_loader=lambda _key: None,
        position_normalizer=lambda row: {**row, "normalized": True},
    )
    assert response["positions"][0]["normalized"] is True


def test_load_risk_response_raises_when_payload_incomplete() -> None:
    payload = {
        "risk": {
            "cov_matrix": {"factors": ["market"]},
            "risk_engine": {"specific_risk_ticker_count": 0},
        }
    }
    with pytest.raises(DashboardPayloadNotReady) as exc_info:
        load_risk_response(
            payload_loader=_payload_loader_factory(payload),
            fallback_loader=lambda _key: None,
        )
    assert exc_info.value.cache_key == "risk"
    assert exc_info.value.refresh_profile == "cold-core"
