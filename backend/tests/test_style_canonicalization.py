import numpy as np
import pandas as pd

from backend.risk_model.descriptors import (
    FULL_STYLE_ORTH_RULES,
    apply_style_canonicalization,
    fit_and_apply_style_canonicalization,
)


def test_apply_style_canonicalization_reproduces_core_rows() -> None:
    core_scores = pd.DataFrame(
        {
            "Size": [-1.0, 1.0],
            "Beta": [-0.5, 0.5],
            "Growth": [0.2, -0.1],
        },
        index=["AAPL.OQ", "MSFT.OQ"],
    )
    core_caps = pd.Series([1000.0, 1200.0], index=core_scores.index)
    core_industries = pd.get_dummies(
        pd.Series(["Technology Equipment", "Software"], index=core_scores.index),
        dtype=float,
    )

    core_canonical, model = fit_and_apply_style_canonicalization(
        style_scores=core_scores,
        market_caps=core_caps,
        industry_exposures=core_industries,
    )

    projectable_scores = pd.concat(
        [
            core_scores,
            pd.DataFrame({"Size": [0.2], "Beta": [0.1], "Growth": [0.4]}, index=["BABA.N"]),
        ]
    )
    projectable_industries = pd.get_dummies(
        pd.Series(
            ["Technology Equipment", "Software", "Technology Equipment"],
            index=projectable_scores.index,
        ),
        dtype=float,
    ).reindex(columns=core_industries.columns, fill_value=0.0)

    applied = apply_style_canonicalization(
        style_scores=projectable_scores,
        model=model,
        industry_exposures=projectable_industries,
    )

    assert np.allclose(
        applied.loc[core_scores.index].to_numpy(dtype=float),
        core_canonical.to_numpy(dtype=float),
    )
    assert list(applied.columns) == list(core_canonical.columns)
    assert np.isfinite(applied.loc["BABA.N"].to_numpy(dtype=float)).all()


def _dependency_fixture() -> tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    index = pd.Index(
        [
            "AAA.OQ",
            "BBB.OQ",
            "CCC.OQ",
            "DDD.OQ",
            "EEE.OQ",
            "FFF.OQ",
        ]
    )
    size = np.array([-2.0, -1.0, -0.5, 0.5, 1.0, 2.0], dtype=float)
    beta = np.array([-1.2, -0.4, -0.1, 0.2, 0.7, 1.1], dtype=float)
    industry_a = np.array([1.5, 1.1, 0.9, -0.2, -0.6, -1.0], dtype=float)
    growth = np.array([0.2, -0.3, 0.1, 0.4, -0.2, 0.0], dtype=float)
    scores = pd.DataFrame(
        {
            "Momentum": 1.8 * size + 0.4 * industry_a + np.array([0.1, -0.1, 0.05, -0.05, 0.08, -0.08]),
            "Size": size,
            "Beta": 0.6 * industry_a + beta,
            "Short-Term Reversal": -0.9 * (1.8 * size + 0.4 * industry_a) + np.array([0.2, -0.1, 0.0, 0.1, -0.2, 0.15]),
            "Residual Volatility": 1.4 * size - 0.8 * beta + np.array([0.05, -0.04, 0.03, -0.02, 0.01, -0.03]),
            "Liquidity": 0.9 * size + np.array([0.2, -0.1, 0.05, -0.02, 0.1, -0.15]),
            "Nonlinear Size": size**3 + np.array([0.3, -0.2, 0.1, -0.1, 0.2, -0.3]),
            "Growth": growth,
        },
        index=index,
        dtype=float,
    )
    caps = pd.Series([1200.0, 900.0, 700.0, 750.0, 1100.0, 1500.0], index=index, dtype=float)
    industries = pd.get_dummies(
        pd.Series(
            ["Tech", "Tech", "Health", "Health", "Industrials", "Industrials"],
            index=index,
        ),
        dtype=float,
    )
    return scores, caps, industries


def test_fit_style_canonicalization_is_order_invariant_for_dependent_factors() -> None:
    scores, caps, industries = _dependency_fixture()
    permuted = scores[
        [
            "Momentum",
            "Growth",
            "Residual Volatility",
            "Size",
            "Short-Term Reversal",
            "Beta",
            "Liquidity",
            "Nonlinear Size",
        ]
    ]

    canonical_base, _ = fit_and_apply_style_canonicalization(
        style_scores=scores,
        market_caps=caps,
        orth_rules=FULL_STYLE_ORTH_RULES,
        industry_exposures=industries,
    )
    canonical_permuted, model_permuted = fit_and_apply_style_canonicalization(
        style_scores=permuted,
        market_caps=caps,
        orth_rules=FULL_STYLE_ORTH_RULES,
        industry_exposures=industries,
    )

    assert np.allclose(
        canonical_base.to_numpy(dtype=float),
        canonical_permuted.reindex(columns=scores.columns).to_numpy(dtype=float),
    )

    momentum_controls = set(model_permuted.transforms["Momentum"].residualization.control_names)
    resid_vol_controls = set(model_permuted.transforms["Residual Volatility"].residualization.control_names)
    reversal_controls = set(model_permuted.transforms["Short-Term Reversal"].residualization.control_names)
    liquidity_controls = set(model_permuted.transforms["Liquidity"].residualization.control_names)
    nonlinear_size_controls = set(model_permuted.transforms["Nonlinear Size"].residualization.control_names)

    assert "Size" in momentum_controls
    assert {"Size", "Beta"}.issubset(resid_vol_controls)
    assert "Momentum" in reversal_controls
    assert "Size" in liquidity_controls
    assert "Size" in nonlinear_size_controls


def test_apply_style_canonicalization_is_order_invariant_for_dependent_factors() -> None:
    scores, caps, industries = _dependency_fixture()
    core_scores = scores.iloc[:4].copy()
    core_caps = caps.loc[core_scores.index]
    core_industries = industries.loc[core_scores.index]
    _, model = fit_and_apply_style_canonicalization(
        style_scores=core_scores[["Momentum", "Size", "Beta", "Short-Term Reversal", "Residual Volatility", "Liquidity", "Nonlinear Size", "Growth"]],
        market_caps=core_caps,
        orth_rules=FULL_STYLE_ORTH_RULES,
        industry_exposures=core_industries,
    )

    projectable_industries = industries.reindex(index=scores.index)
    ordered = apply_style_canonicalization(
        style_scores=scores[["Momentum", "Size", "Beta", "Short-Term Reversal", "Residual Volatility", "Liquidity", "Nonlinear Size", "Growth"]],
        model=model,
        industry_exposures=projectable_industries,
    )
    permuted = apply_style_canonicalization(
        style_scores=scores[["Residual Volatility", "Growth", "Momentum", "Liquidity", "Beta", "Size", "Nonlinear Size", "Short-Term Reversal"]],
        model=model,
        industry_exposures=projectable_industries,
    )

    assert np.allclose(
        ordered.to_numpy(dtype=float),
        permuted.reindex(columns=ordered.columns).to_numpy(dtype=float),
    )
