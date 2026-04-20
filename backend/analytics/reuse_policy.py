"""Cache reuse and risk payload helpers for the analytics pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.analytics.contracts import CovarianceMatrixPayload, RiskEngineMetaPayload, SourceDatesPayload
from backend.analytics.refresh_context import (
    risk_engine_reuse_signature,
    source_dates_reuse_signature,
)
from backend.data import sqlite

_LIVE_REGRESSION_MIN_MODELED_COUNT = 100
_LIVE_REGRESSION_MIN_PROJECTED_COUNT = 25
_LIVE_REGRESSION_COLLAPSE_FRACTION = 0.25


def _universe_loadings_modeled_counts(payload: Any) -> dict[str, int]:
    by_ticker = dict((payload or {}).get("by_ticker") or {})
    counts = {
        "ticker_count": int(len(by_ticker)),
        "modeled_ticker_count": 0,
        "core_estimated_ticker_count": 0,
        "projected_only_ticker_count": 0,
        "ineligible_ticker_count": 0,
    }
    for raw_row in by_ticker.values():
        status = str(dict(raw_row or {}).get("model_status") or "").strip()
        if status == "core_estimated":
            counts["core_estimated_ticker_count"] += 1
            counts["modeled_ticker_count"] += 1
        elif status == "projected_only":
            counts["projected_only_ticker_count"] += 1
            counts["modeled_ticker_count"] += 1
        elif status == "ineligible":
            counts["ineligible_ticker_count"] += 1
    return counts


def _collapsed_vs_live(*, live_count: int, candidate_count: int, minimum_live: int) -> bool:
    if int(live_count) < int(minimum_live):
        return False
    threshold = max(1, int(np.ceil(float(live_count) * _LIVE_REGRESSION_COLLAPSE_FRACTION)))
    return int(candidate_count) < threshold


def universe_loadings_payload_integrity(
    cached_payload: Any,
) -> tuple[bool, str]:
    if not isinstance(cached_payload, dict):
        return False, "missing_cached_payload"
    counts = _universe_loadings_modeled_counts(cached_payload)
    if counts["ticker_count"] <= 0:
        return False, "missing_by_ticker"
    if counts["modeled_ticker_count"] <= 0:
        return False, "no_modeled_tickers"
    return True, "payload_integrity_ok"


def universe_loadings_live_regression_guard(
    candidate_payload: Any,
    *,
    current_live_payload: Any,
) -> tuple[bool, str]:
    candidate_ok, candidate_reason = universe_loadings_payload_integrity(candidate_payload)
    if not candidate_ok:
        return False, candidate_reason
    live_ok, _ = universe_loadings_payload_integrity(current_live_payload)
    if not live_ok:
        return True, "no_live_baseline"

    live_counts = _universe_loadings_modeled_counts(current_live_payload)
    candidate_counts = _universe_loadings_modeled_counts(candidate_payload)

    if live_counts["modeled_ticker_count"] > 0 and candidate_counts["modeled_ticker_count"] <= 0:
        return False, "modeled_tickers_regressed_to_zero_vs_live"
    if live_counts["core_estimated_ticker_count"] > 0 and candidate_counts["core_estimated_ticker_count"] <= 0:
        return False, "core_estimated_regressed_to_zero_vs_live"
    if live_counts["projected_only_ticker_count"] > 0 and candidate_counts["projected_only_ticker_count"] <= 0:
        return False, "projected_only_regressed_to_zero_vs_live"
    if _collapsed_vs_live(
        live_count=live_counts["modeled_ticker_count"],
        candidate_count=candidate_counts["modeled_ticker_count"],
        minimum_live=_LIVE_REGRESSION_MIN_MODELED_COUNT,
    ):
        return False, "modeled_ticker_count_collapsed_vs_live"
    if _collapsed_vs_live(
        live_count=live_counts["core_estimated_ticker_count"],
        candidate_count=candidate_counts["core_estimated_ticker_count"],
        minimum_live=_LIVE_REGRESSION_MIN_MODELED_COUNT,
    ):
        return False, "core_estimated_ticker_count_collapsed_vs_live"
    if _collapsed_vs_live(
        live_count=live_counts["projected_only_ticker_count"],
        candidate_count=candidate_counts["projected_only_ticker_count"],
        minimum_live=_LIVE_REGRESSION_MIN_PROJECTED_COUNT,
    ):
        return False, "projected_only_ticker_count_collapsed_vs_live"
    return True, "live_regression_check_ok"


def can_reuse_cached_universe_loadings(
    cached_payload: Any,
    *,
    source_dates: SourceDatesPayload,
    risk_engine_meta: RiskEngineMetaPayload,
) -> tuple[bool, str]:
    ok, reason = universe_loadings_payload_integrity(cached_payload)
    if not ok:
        return False, reason
    cached_source_dates = cached_payload.get("source_dates")
    if not isinstance(cached_source_dates, dict):
        return False, "missing_cached_source_dates"
    if source_dates_reuse_signature(cached_source_dates) != source_dates_reuse_signature(source_dates):
        return False, "source_dates_changed"
    cached_risk = cached_payload.get("risk_engine")
    if not isinstance(cached_risk, dict):
        return False, "missing_cached_risk_engine"
    if risk_engine_reuse_signature(cached_risk) != risk_engine_reuse_signature(risk_engine_meta):
        return False, "risk_engine_state_changed"
    return True, "source_and_risk_engine_match"


def load_cached_risk_display_payload(*, cache_db: Path | None = None) -> CovarianceMatrixPayload | None:
    cached_risk = sqlite.cache_get("risk", db_path=cache_db)
    if not isinstance(cached_risk, dict):
        return None
    cov_matrix = cached_risk.get("cov_matrix")
    if not isinstance(cov_matrix, dict):
        return None
    return dict(cov_matrix)


def deserialize_covariance(payload: Any) -> pd.DataFrame:
    if not isinstance(payload, dict):
        return pd.DataFrame()
    factors = [str(f) for f in (payload.get("factors") or []) if str(f).strip()]
    matrix = payload.get("matrix") or []
    if not factors or not isinstance(matrix, list):
        return pd.DataFrame()
    try:
        arr = np.asarray(matrix, dtype=float)
    except Exception:
        return pd.DataFrame()
    if arr.ndim != 2 or arr.shape[0] != len(factors) or arr.shape[1] != len(factors):
        return pd.DataFrame()
    return pd.DataFrame(arr, index=factors, columns=factors)
