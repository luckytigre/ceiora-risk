"""Thin route-facing aggregate cPAR risk payload service."""

from __future__ import annotations

from backend.services import cpar_aggregate_risk_service


def load_cpar_risk_payload(
    *,
    data_db=None,
) -> dict[str, object]:
    return cpar_aggregate_risk_service.load_cpar_risk_payload(data_db=data_db)
