"""Orchestration helpers for running a Barra-style two-phase model."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from barra.wls_regression import (
    WlsTwoPhaseResult,
    estimate_factor_returns_two_phase,
)


@dataclass(frozen=True)
class BarraRunArtifacts:
    factor_returns: dict[str, float]
    position_exposures: pd.DataFrame
    residuals: pd.Series
    r_squared: float
    condition_number: float
    phase_a_condition_number: float
    phase_b_condition_number: float
    residual_vol: float


def run_barra_two_phase(
    *,
    returns: pd.Series,
    market_caps: pd.Series,
    style_exposures: pd.DataFrame,
    industry_exposures: pd.DataFrame | None = None,
) -> BarraRunArtifacts:
    """Run a single-date two-phase cross-sectional Barra regression."""
    aligned = pd.concat(
        [returns.rename("ret"), market_caps.rename("mcap"), style_exposures],
        axis=1,
        join="inner",
    ).dropna(subset=["ret", "mcap"])
    if industry_exposures is not None:
        aligned = aligned.join(industry_exposures, how="left").fillna(0.0)

    y = aligned["ret"].to_numpy(dtype=float)
    caps = aligned["mcap"].to_numpy(dtype=float)
    style_cols = list(style_exposures.columns)
    style_x = aligned[style_cols].to_numpy(dtype=float)

    ind_cols: list[str] = []
    ind_x: np.ndarray | None = None
    if industry_exposures is not None and len(industry_exposures.columns) > 0:
        ind_cols = list(industry_exposures.columns)
        ind_x = aligned[ind_cols].to_numpy(dtype=float)

    result: WlsTwoPhaseResult = estimate_factor_returns_two_phase(
        returns=y,
        market_caps=caps,
        industry_exposures=ind_x,
        style_exposures=style_x,
        industry_names=ind_cols,
        style_names=style_cols,
    )
    return BarraRunArtifacts(
        factor_returns=result.factor_returns,
        position_exposures=aligned[style_cols + ind_cols].copy(),
        residuals=pd.Series(result.residuals, index=aligned.index, dtype=float),
        r_squared=result.r_squared,
        condition_number=result.condition_number,
        phase_a_condition_number=result.phase_a_condition_number,
        phase_b_condition_number=result.phase_b_condition_number,
        residual_vol=result.residual_vol,
    )
