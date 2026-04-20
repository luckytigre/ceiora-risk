from __future__ import annotations

import pytest

from backend.services.dashboard_payload_service import (
    DashboardPayloadNotReady,
    load_exposures_response,
    load_portfolio_response,
    load_risk_covariance_response,
    load_risk_response,
    load_risk_summary_response,
)


def _payload_loader_factory(mapping: dict[str, object]):
    def _loader(name: str, *, fallback_loader=None):
        return mapping.get(name)

    return _loader


def test_load_exposures_response_normalizes_factor_fields() -> None:
    payload = {
        "exposures": {
            "raw": [{"factor": "Momentum", "value": 1.0, "coverage_date": "2026-03-13"}],
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
    assert response["factors"][0]["factor_coverage_asof"] == "2026-03-13"
    assert response["factors"][0]["coverage_date"] == "2026-03-13"
    assert response["snapshot_id"] == "snap_1"


def test_load_risk_response_normalizes_country_fields() -> None:
    payload = {
        "risk": {
            "risk_shares": {"country": 1.0, "style": 99.0},
            "vol_scaled_shares": {"country": 2.0, "style": 98.0},
            "component_shares": {"country": 0.2, "style": 0.8},
            "factor_details": [{"factor": "Country: US", "category": "country"}],
            "cov_matrix": {"factors": ["market"], "correlation": [[1.0]]},
            "risk_engine": {
                "specific_risk_ticker_count": 1,
                "factor_returns_latest_date": "2026-03-13",
                "last_recompute_date": "2026-03-16",
            },
        },
        "model_sanity": {
            "status": "ok",
            "warnings": [],
            "checks": {},
            "coverage_date": "2026-03-13",
            "latest_available_date": "2026-03-14",
        },
    }
    response = load_risk_response(
        payload_loader=_payload_loader_factory(payload),
        fallback_loader=lambda _key: None,
    )
    assert response["risk_shares"]["market"] == 1.0
    assert "country" not in response["risk_shares"]
    assert response["vol_scaled_shares"]["market"] == 2.0
    assert "country" not in response["vol_scaled_shares"]
    assert response["component_shares"]["market"] == 0.2
    assert response["factor_details"][0]["factor_id"] == "Country: US"
    assert response["factor_details"][0]["category"] == "market"
    assert response["risk_engine"]["core_state_through_date"] == "2026-03-13"
    assert response["risk_engine"]["core_rebuild_date"] == "2026-03-16"
    assert response["model_sanity"]["served_loadings_asof"] == "2026-03-13"
    assert response["model_sanity"]["latest_loadings_available_asof"] == "2026-03-14"


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


def test_load_risk_summary_response_does_not_require_covariance_completeness() -> None:
    payload = {
        "risk": {
            "risk_shares": {"country": 1.0, "style": 99.0},
            "vol_scaled_shares": {"country": 2.0, "style": 98.0},
            "factor_details": [{"factor": "Country: US", "category": "country"}],
            "factor_catalog": [{"factor_id": "market_beta", "factor_name": "Market Beta"}],
            "source_dates": {"exposures_served_asof": "2026-03-13"},
            "risk_engine": {"specific_risk_ticker_count": 0},
            "run_id": "run_summary",
            "snapshot_id": "snap_summary",
        },
        "model_sanity": {
            "status": "ok",
            "coverage_date": "2026-03-13",
        },
    }

    response = load_risk_summary_response(
        payload_loader=_payload_loader_factory(payload),
        fallback_loader=lambda _key: None,
    )

    assert response["risk_shares"]["market"] == 1.0
    assert response["factor_details"][0]["category"] == "market"
    assert response["factor_catalog"][0]["factor_id"] == "market_beta"
    assert response["snapshot_id"] == "snap_summary"
    assert response["model_sanity"]["served_loadings_asof"] == "2026-03-13"


def test_load_risk_covariance_response_returns_covariance_only() -> None:
    payload = {
        "risk": {
            "cov_matrix": {
                "factors": ["market_beta"],
                "correlation": [[1.0]],
            },
            "run_id": "run_cov",
            "snapshot_id": "snap_cov",
        },
    }

    response = load_risk_covariance_response(
        payload_loader=_payload_loader_factory(payload),
        fallback_loader=lambda _key: None,
    )

    assert response["cov_matrix"]["factors"] == ["market_beta"]
    assert response["snapshot_id"] == "snap_cov"


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
