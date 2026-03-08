from __future__ import annotations

import pandas as pd

from backend.risk_model.risk_attribution import COUNTRY_FACTOR, risk_decomposition


def test_risk_decomposition_emits_country_bucket() -> None:
    cov = pd.DataFrame(
        [
            [0.0400, 0.0000, 0.0000],
            [0.0000, 0.0900, 0.0000],
            [0.0000, 0.0000, 0.0100],
        ],
        index=[COUNTRY_FACTOR, "Software & Services", "Beta"],
        columns=[COUNTRY_FACTOR, "Software & Services", "Beta"],
    )
    positions = [
        {
            "ticker": "SHOP",
            "weight": 0.7,
            "exposures": {
                COUNTRY_FACTOR: -1.0,
                "Software & Services": 1.0,
                "Beta": 0.4,
            },
        },
        {
            "ticker": "AAPL",
            "weight": 0.3,
            "exposures": {
                COUNTRY_FACTOR: 1.0,
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

    assert risk_shares["country"] > 0.0
    assert component_shares["country"] > 0.0
    by_factor = {row["factor"]: row for row in factor_details}
    assert by_factor[COUNTRY_FACTOR]["category"] == "country"
    assert by_factor["Software & Services"]["category"] == "industry"
    assert by_factor["Beta"]["category"] == "style"
