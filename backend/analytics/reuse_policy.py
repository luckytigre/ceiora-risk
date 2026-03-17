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


def can_reuse_cached_universe_loadings(
    cached_payload: Any,
    *,
    source_dates: SourceDatesPayload,
    risk_engine_meta: RiskEngineMetaPayload,
) -> tuple[bool, str]:
    if not isinstance(cached_payload, dict):
        return False, "missing_cached_payload"
    by_ticker = cached_payload.get("by_ticker")
    if not isinstance(by_ticker, dict) or not by_ticker:
        return False, "missing_by_ticker"
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
