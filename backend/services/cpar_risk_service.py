"""Thin route-facing aggregate cPAR risk payload service."""

from __future__ import annotations

from backend.services import cpar_aggregate_risk_service


def load_cpar_risk_payload(
    *,
    allowed_account_ids: list[str] | tuple[str, ...] | None = None,
    data_db=None,
) -> dict[str, object]:
    return cpar_aggregate_risk_service.load_cpar_risk_payload(
        allowed_account_ids=allowed_account_ids,
        data_db=data_db,
    )
