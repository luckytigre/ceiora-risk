from __future__ import annotations

import pandas as pd

from backend.analytics import health


def test_build_factor_exposure_matrix_uses_us_core_population() -> None:
    snapshot_df = pd.DataFrame(
        [
            {"ric": "AAPL.OQ", "beta_score": 0.4, "size_score": 0.3},
            {"ric": "MSFT.OQ", "beta_score": 0.2, "size_score": 0.5},
            {"ric": "BABA.N", "beta_score": -0.1, "size_score": -0.2},
        ]
    )
    eligibility = pd.DataFrame(
        [
            {
                "ric": "AAPL.OQ",
                "is_structural_eligible": True,
                "market_cap": 1000.0,
                "trbc_business_sector": "Technology Equipment",
                "hq_country_code": "US",
            },
            {
                "ric": "MSFT.OQ",
                "is_structural_eligible": True,
                "market_cap": 900.0,
                "trbc_business_sector": "Technology Equipment",
                "hq_country_code": "US",
            },
            {
                "ric": "BABA.N",
                "is_structural_eligible": True,
                "market_cap": 700.0,
                "trbc_business_sector": "Retailers",
                "hq_country_code": "CN",
            },
        ]
    ).set_index("ric")

    matrix = health._build_factor_exposure_matrix(
        snapshot_df,
        eligibility=eligibility,
        core_country_codes={"US"},
    )

    assert list(matrix.index) == ["AAPL.OQ", "MSFT.OQ"]
    assert "Technology Equipment" in matrix.columns
    assert "Retailers" not in matrix.columns
    assert "Beta" in matrix.columns
    assert "Size" in matrix.columns
