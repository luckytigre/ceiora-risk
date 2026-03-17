import numpy as np

from backend.risk_model.wls_regression import (
    estimate_factor_returns_one_stage,
    fitted_returns_one_stage,
)


def test_one_stage_wls_enforces_cap_weighted_industry_constraint() -> None:
    returns = np.array([0.02, 0.01, 0.03, 0.04], dtype=float)
    market_caps = np.array([100.0, 200.0, 150.0, 250.0], dtype=float)
    market_exposures = np.ones((4, 1), dtype=float)
    industry_exposures = np.array(
        [
            [1.0, 0.0],
            [1.0, 0.0],
            [0.0, 1.0],
            [0.0, 1.0],
        ],
        dtype=float,
    )
    style_exposures = np.array([[0.1], [-0.2], [0.3], [-0.1]], dtype=float)

    result = estimate_factor_returns_one_stage(
        returns=returns,
        raw_returns=returns,
        market_caps=market_caps,
        market_exposures=market_exposures,
        industry_exposures=industry_exposures,
        style_exposures=style_exposures,
        market_name="Market",
        industry_names=["Software & Services", "Retailers"],
        style_names=["Beta"],
    )

    industry_weights = (industry_exposures.T @ market_caps) / market_caps.sum()
    industry_returns = np.array(
        [
            result.factor_returns["Software & Services"],
            result.factor_returns["Retailers"],
        ],
        dtype=float,
    )

    assert "Market" in result.factor_returns
    assert abs(float(industry_weights @ industry_returns)) < 1e-10
    assert result.constraint_residual < 1e-10


def test_one_stage_fitted_returns_match_raw_residual_definition() -> None:
    returns = np.array([0.02, 0.01, 0.03, 0.04], dtype=float)
    market_caps = np.array([100.0, 200.0, 150.0, 250.0], dtype=float)
    market_exposures = np.ones((4, 1), dtype=float)
    industry_exposures = np.array(
        [
            [1.0, 0.0],
            [1.0, 0.0],
            [0.0, 1.0],
            [0.0, 1.0],
        ],
        dtype=float,
    )
    style_exposures = np.array([[0.1], [-0.2], [0.3], [-0.1]], dtype=float)

    result = estimate_factor_returns_one_stage(
        returns=returns,
        raw_returns=returns,
        market_caps=market_caps,
        market_exposures=market_exposures,
        industry_exposures=industry_exposures,
        style_exposures=style_exposures,
        market_name="Market",
        industry_names=["Software & Services", "Retailers"],
        style_names=["Beta"],
    )

    fitted = fitted_returns_one_stage(
        result,
        market_exposures=market_exposures,
        industry_exposures=industry_exposures,
        style_exposures=style_exposures,
        market_name="Market",
        industry_names=["Software & Services", "Retailers"],
        style_names=["Beta"],
    )

    assert fitted.shape == (4,)
    assert np.allclose(result.raw_residuals, returns - fitted)
