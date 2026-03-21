"""Supplemental cPAR factor-history payload service."""

from __future__ import annotations

import math

from backend.cpar.factor_registry import factor_spec_by_id
from backend.data import cpar_outputs
from backend.services import cpar_meta_service


class CparFactorNotFound(LookupError):
    """Raised when a requested cPAR factor is not part of the cPAR1 registry."""


def load_cpar_factor_history_payload(
    *,
    factor_id: str,
    years: int,
    data_db=None,
) -> dict[str, object]:
    clean_factor_id = str(factor_id or "").strip().upper()
    if not clean_factor_id:
        raise CparFactorNotFound("factor_id is required.")
    try:
        spec = factor_spec_by_id(clean_factor_id)
    except KeyError as exc:
        raise CparFactorNotFound(f"Unknown cPAR factor_id {clean_factor_id!r}.") from exc

    try:
        cpar_meta_service.require_active_package(data_db=data_db)
        latest, rows = cpar_outputs.load_factor_return_history(
            clean_factor_id,
            years=int(years),
            data_db=data_db,
        )
    except cpar_outputs.CparAuthorityReadError as exc:
        raise cpar_meta_service.CparReadUnavailable(str(exc)) from exc

    if latest is None or not rows:
        raise cpar_meta_service.CparReadNotReady(
            "Historical cPAR factor returns are not available yet."
        )

    points = _build_cumulative_points(rows)

    return {
        "factor_id": spec.factor_id,
        "factor_name": spec.label,
        "years": int(years),
        "points": points,
        "_cached": True,
    }


def _build_cumulative_points(rows: list[tuple[str, float]]) -> list[dict[str, object]]:
    cumulative = 1.0
    points: list[dict[str, object]] = []
    for week_end, raw_return in rows:
        current_return = float(raw_return)
        if not math.isfinite(current_return):
            continue
        cumulative *= (1.0 + current_return)
        points.append(
            {
                "date": str(week_end),
                "factor_return": round(current_return, 8),
                "cum_return": round(cumulative - 1.0, 8),
            }
        )
    return points
