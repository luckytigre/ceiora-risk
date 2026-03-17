"""Regression-frame assembly for daily factor return estimation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import numpy as np
import pandas as pd

from backend.risk_model.descriptors import (
    FULL_STYLE_ORTH_RULES,
    apply_style_canonicalization,
    fit_and_apply_style_canonicalization,
)
from backend.risk_model.eligibility import EligibilityContext, structural_eligibility_for_date
from backend.risk_model.factor_catalog import STYLE_COLUMN_TO_LABEL, STYLE_SCORE_COLS


@dataclass(frozen=True)
class RegressionFrameSummary:
    date: str
    eligibility_date: str
    exposure_date: str | None
    exposure_n: int = 0
    structural_eligible_n: int = 0
    core_structural_eligible_n: int = 0
    regression_member_n: int = 0
    projectable_n: int = 0
    projected_only_n: int = 0
    structural_coverage: float = 0.0
    regression_coverage: float = 0.0
    projectable_coverage: float = 0.0
    missing_style_n: int = 0
    missing_market_cap_n: int = 0
    missing_trbc_economic_sector_short_n: int = 0
    missing_trbc_industry_n: int = 0
    non_equity_n: int = 0
    missing_return_n: int = 0


@dataclass(frozen=True)
class RegressionFrame:
    summary: RegressionFrameSummary
    exposure_snapshot: pd.DataFrame
    eligibility: pd.DataFrame
    regression_index: pd.Index
    projectable_index: pd.Index
    returns_series: pd.Series
    projectable_returns_series: pd.Series
    market_cap_series: pd.Series
    projectable_market_cap_series: pd.Series
    industry_series: pd.Series
    projectable_industry_series: pd.Series
    hq_country_series: pd.Series
    projectable_hq_country_series: pd.Series
    industry_dummies: pd.DataFrame
    projectable_industry_dummies: pd.DataFrame
    style_names: list[str]
    raw_returns: np.ndarray
    projectable_raw_returns: np.ndarray
    returns: np.ndarray
    projectable_returns: np.ndarray
    market_caps: np.ndarray
    style_matrix: np.ndarray | None
    projectable_style_matrix: np.ndarray | None

    @property
    def industry_names(self) -> list[str]:
        return list(self.industry_dummies.columns)


@dataclass(frozen=True)
class RegressionFrameBuildResult:
    frame: RegressionFrame | None
    summary: RegressionFrameSummary
    skip_reason: str | None = None


def _supported_style_score_columns(style_scores: pd.DataFrame) -> list[str]:
    """Keep style columns that have non-zero cross-sectional support on the date."""
    supported: list[str] = []
    for col in style_scores.columns:
        series = pd.to_numeric(style_scores[col], errors="coerce")
        values = series.to_numpy(dtype=float)
        finite = values[np.isfinite(values)]
        if finite.size == 0:
            continue
        if float(np.nanmax(np.abs(finite))) <= 0.0:
            continue
        supported.append(str(col))
    return supported


class RegressionFrameBuilder:
    def __init__(
        self,
        *,
        daily_returns: pd.DataFrame,
        eligibility_ctx: EligibilityContext,
        lag_days: int,
        returns_winsor_pct: float,
        eligibility_resolver: Callable[[EligibilityContext, str], tuple[str | None, pd.DataFrame]] = structural_eligibility_for_date,
        style_fit_canonicalizer: Callable[..., tuple[pd.DataFrame, Any]] = fit_and_apply_style_canonicalization,
        style_apply_canonicalizer: Callable[..., pd.DataFrame] = apply_style_canonicalization,
        orth_rules: dict[str, tuple[str, ...]] | None = None,
        core_country_codes: set[str] | None = None,
    ) -> None:
        self._daily_returns = daily_returns
        self._eligibility_ctx = eligibility_ctx
        self._lag_days = max(0, int(lag_days))
        self._returns_winsor_pct = float(returns_winsor_pct)
        self._eligibility_resolver = eligibility_resolver
        self._style_fit_canonicalizer = style_fit_canonicalizer
        self._style_apply_canonicalizer = style_apply_canonicalizer
        self._orth_rules = FULL_STYLE_ORTH_RULES if orth_rules is None else orth_rules
        self._core_country_codes = (
            {str(code).upper().strip() for code in core_country_codes if str(code).strip()}
            if core_country_codes
            else None
        )

    def _winsorize_cross_section(self, values: np.ndarray) -> tuple[np.ndarray, float | None, float | None]:
        out = np.asarray(values, dtype=float).copy()
        pct = self._returns_winsor_pct
        if not (0.0 < pct < 0.5):
            return out, None, None
        finite = np.isfinite(out)
        if int(finite.sum()) < 10:
            return out, None, None
        lo = float(np.nanpercentile(out[finite], pct * 100.0))
        hi = float(np.nanpercentile(out[finite], (1.0 - pct) * 100.0))
        out[finite] = np.clip(out[finite], lo, hi)
        return out, lo, hi

    @staticmethod
    def _apply_winsor_bounds(values: np.ndarray, lo: float | None, hi: float | None) -> np.ndarray:
        out = np.asarray(values, dtype=float).copy()
        finite = np.isfinite(out)
        if lo is None or hi is None:
            return out
        out[finite] = np.clip(out[finite], float(lo), float(hi))
        return out

    def build(self, *, date: str, eligibility_date: str) -> RegressionFrameBuildResult:
        if str(date) not in self._daily_returns.index:
            return RegressionFrameBuildResult(
                frame=None,
                summary=RegressionFrameSummary(date=str(date), eligibility_date=str(eligibility_date), exposure_date=None),
                skip_reason="missing_return_row",
            )

        ret_row = self._daily_returns.loc[str(date)]
        ret_row = pd.to_numeric(ret_row, errors="coerce")
        ret_row = ret_row[np.isfinite(ret_row.to_numpy(dtype=float))]

        exp_date, eligibility = self._eligibility_resolver(self._eligibility_ctx, str(eligibility_date))
        if exp_date is None or eligibility.empty:
            return RegressionFrameBuildResult(
                frame=None,
                summary=RegressionFrameSummary(date=str(date), eligibility_date=str(eligibility_date), exposure_date=exp_date),
                skip_reason="missing_eligibility",
            )

        exposure_snapshot = self._eligibility_ctx.exposure_snapshots[exp_date]
        structural_mask = eligibility["is_structural_eligible"].astype(bool)
        has_return = eligibility.index.isin(ret_row.index)
        projectable_mask = structural_mask & has_return
        country_all = (
            eligibility["hq_country_code"]
            .fillna("")
            .astype(str)
            .str.upper()
            .str.strip()
        )
        core_eligible_mask = structural_mask
        if self._core_country_codes is not None:
            core_eligible_mask = structural_mask & country_all.isin(self._core_country_codes)
        regression_mask = core_eligible_mask & has_return
        exposure_n = int(len(eligibility))
        structural_n = int(structural_mask.sum())
        core_structural_n = int(core_eligible_mask.sum())
        regression_n = int(regression_mask.sum())
        projectable_n = int(projectable_mask.sum())

        no_struct = eligibility.loc[~structural_mask, "exclusion_reason"].astype(str)
        exploded = no_struct.str.split("|").explode()
        summary = RegressionFrameSummary(
            date=str(date),
            eligibility_date=str(eligibility_date),
            exposure_date=exp_date,
            exposure_n=exposure_n,
            structural_eligible_n=structural_n,
            core_structural_eligible_n=core_structural_n,
            regression_member_n=regression_n,
            projectable_n=projectable_n,
            projected_only_n=max(0, projectable_n - regression_n),
            structural_coverage=float(structural_n / max(1, exposure_n)),
            regression_coverage=float(regression_n / max(1, core_structural_n)),
            projectable_coverage=float(projectable_n / max(1, structural_n)),
            missing_style_n=int((exploded == "missing_style").sum()),
            missing_market_cap_n=int((exploded == "missing_market_cap").sum()),
            missing_trbc_economic_sector_short_n=int((exploded == "missing_trbc_economic_sector_short").sum()),
            missing_trbc_industry_n=int((exploded == "missing_trbc_industry").sum()),
            non_equity_n=int((exploded == "non_equity").sum()),
            missing_return_n=int((structural_mask & ~has_return).sum()),
        )

        projectable_idx = eligibility.index[projectable_mask]
        valid_idx = eligibility.index[regression_mask]
        projectable_returns_series = ret_row.loc[projectable_idx].astype(float)
        returns_series = ret_row.loc[valid_idx].astype(float)
        projectable_market_cap_series = pd.to_numeric(
            eligibility.loc[projectable_idx, "market_cap"], errors="coerce"
        ).astype(float)
        market_cap_series = pd.to_numeric(eligibility.loc[valid_idx, "market_cap"], errors="coerce").astype(float)
        projectable_industry_series = (
            eligibility.loc[projectable_idx, "trbc_business_sector"]
            .fillna("")
            .astype(str)
            .str.strip()
        )
        industry_series = (
            eligibility.loc[valid_idx, "trbc_business_sector"]
            .fillna("")
            .astype(str)
            .str.strip()
        )
        projectable_hq_country_series = (
            eligibility.loc[projectable_idx, "hq_country_code"]
            .fillna("")
            .astype(str)
            .str.upper()
            .str.strip()
        )
        hq_country_series = (
            eligibility.loc[valid_idx, "hq_country_code"]
            .fillna("")
            .astype(str)
            .str.upper()
            .str.strip()
        )

        if projectable_industry_series.eq("").all():
            return RegressionFrameBuildResult(frame=None, summary=summary, skip_reason="missing_l2_sector")
        if projectable_hq_country_series.eq("").all():
            return RegressionFrameBuildResult(frame=None, summary=summary, skip_reason="missing_country")

        raw_returns = returns_series.to_numpy(dtype=float)
        projectable_raw_returns = projectable_returns_series.to_numpy(dtype=float)
        returns, winsor_lo, winsor_hi = self._winsorize_cross_section(raw_returns)
        projectable_returns = self._apply_winsor_bounds(projectable_raw_returns, winsor_lo, winsor_hi)
        market_caps = market_cap_series.to_numpy(dtype=float)

        industry_dummies = pd.get_dummies(industry_series, dtype=float)
        if projectable_n > 0 and industry_dummies.empty and regression_n > 0:
            return RegressionFrameBuildResult(frame=None, summary=summary, skip_reason="empty_dummies")
        if industry_dummies.empty:
            projectable_industry_dummies = pd.DataFrame(index=projectable_idx)
        else:
            projectable_industry_dummies = (
                pd.get_dummies(projectable_industry_series, dtype=float)
                .reindex(columns=industry_dummies.columns, fill_value=0.0)
                .reindex(projectable_idx, fill_value=0.0)
            )

        style_cols_present = [col for col in STYLE_SCORE_COLS if col in exposure_snapshot.columns]
        style_scores_projectable_raw = exposure_snapshot.loc[projectable_idx, style_cols_present].copy()
        style_scores_core_raw = style_scores_projectable_raw.loc[valid_idx].copy()
        style_cols_supported = _supported_style_score_columns(style_scores_core_raw)
        style_names = [STYLE_COLUMN_TO_LABEL[col] for col in style_cols_supported]
        style_matrix: np.ndarray | None = None
        projectable_style_matrix: np.ndarray | None = None
        if style_names:
            style_scores_projectable = style_scores_projectable_raw[style_cols_supported].copy()
            style_scores_projectable.columns = style_names
            if not style_scores_projectable.empty and regression_n > 0:
                core_style_scores = style_scores_projectable.loc[valid_idx].copy()
                style_canonical, style_model = self._style_fit_canonicalizer(
                    style_scores=core_style_scores,
                    market_caps=market_cap_series.loc[valid_idx],
                    orth_rules=self._orth_rules,
                    industry_exposures=industry_dummies,
                )
                style_matrix = style_canonical[style_names].to_numpy(dtype=float)
                projectable_style = self._style_apply_canonicalizer(
                    style_scores=style_scores_projectable.copy(),
                    model=style_model,
                    industry_exposures=projectable_industry_dummies,
                )
                projectable_style_matrix = projectable_style[style_names].to_numpy(dtype=float)

        frame = RegressionFrame(
            summary=summary,
            exposure_snapshot=exposure_snapshot,
            eligibility=eligibility,
            regression_index=valid_idx,
            projectable_index=projectable_idx,
            returns_series=returns_series,
            projectable_returns_series=projectable_returns_series,
            market_cap_series=market_cap_series,
            projectable_market_cap_series=projectable_market_cap_series,
            industry_series=industry_series,
            projectable_industry_series=projectable_industry_series,
            hq_country_series=hq_country_series,
            projectable_hq_country_series=projectable_hq_country_series,
            industry_dummies=industry_dummies,
            projectable_industry_dummies=projectable_industry_dummies,
            style_names=style_names,
            raw_returns=raw_returns,
            projectable_raw_returns=projectable_raw_returns,
            returns=returns,
            projectable_returns=projectable_returns,
            market_caps=market_caps,
            style_matrix=style_matrix,
            projectable_style_matrix=projectable_style_matrix,
        )
        return RegressionFrameBuildResult(frame=frame, summary=summary)
