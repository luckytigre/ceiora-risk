from __future__ import annotations

import pandas as pd

from backend.risk_model.factor_catalog import MARKET_FACTOR
from backend.risk_model.risk_attribution import risk_decomposition


def test_risk_decomposition_emits_market_bucket() -> None:
    cov = pd.DataFrame(
        [
            [0.0400, 0.0000, 0.0000],
            [0.0000, 0.0900, 0.0000],
            [0.0000, 0.0000, 0.0100],
        ],
        index=[MARKET_FACTOR, "Software & Services", "Beta"],
        columns=[MARKET_FACTOR, "Software & Services", "Beta"],
    )
    positions = [
        {
            "ticker": "SHOP",
            "weight": 0.7,
            "exposures": {
                MARKET_FACTOR: 1.0,
                "Software & Services": 1.0,
                "Beta": 0.4,
            },
        },
        {
            "ticker": "AAPL",
            "weight": 0.3,
            "exposures": {
                MARKET_FACTOR: 1.0,
                "Software & Services": 1.0,
                "Beta": 0.6,
            },
        },
    ]

    risk_shares, component_shares, factor_details = risk_decomposition(
        cov=cov,
        positions=positions,
        specific_risk_by_ticker={},
    )

    assert risk_shares["market"] > 0.0
    assert component_shares["market"] > 0.0
    by_factor = {row["factor_id"]: row for row in factor_details}
    assert by_factor[MARKET_FACTOR]["category"] == "market"
    assert by_factor["Software & Services"]["category"] == "industry"
    assert by_factor["Beta"]["category"] == "style"


def test_risk_decomposition_emits_market_bucket_for_factor_ids() -> None:
    cov = pd.DataFrame(
        [
            [0.0400, 0.0000, 0.0000],
            [0.0000, 0.0900, 0.0000],
            [0.0000, 0.0000, 0.0100],
        ],
        index=["market", "industry_software_services", "style_beta_score"],
        columns=["market", "industry_software_services", "style_beta_score"],
    )
    positions = [
        {
            "ticker": "SHOP",
            "weight": 0.7,
            "exposures": {
                "market": 1.0,
                "industry_software_services": 1.0,
                "style_beta_score": 0.4,
            },
        },
        {
            "ticker": "AAPL",
            "weight": 0.3,
            "exposures": {
                "market": 1.0,
                "industry_software_services": 1.0,
                "style_beta_score": 0.6,
            },
        },
    ]

    risk_shares, component_shares, factor_details = risk_decomposition(
        cov=cov,
        positions=positions,
        specific_risk_by_ticker={},
    )

    assert risk_shares["market"] > 0.0
    assert component_shares["market"] > 0.0
    by_factor = {row["factor_id"]: row for row in factor_details}
    assert by_factor["market"]["category"] == "market"
    assert by_factor["industry_software_services"]["category"] == "industry"
    assert by_factor["style_beta_score"]["category"] == "style"
