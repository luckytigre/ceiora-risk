"""Security model-status helpers."""

from __future__ import annotations

from typing import Literal

ModelStatus = Literal["core_estimated", "projected_only", "ineligible"]


def derive_model_status(
    *,
    is_core_regression_member: bool,
    is_projectable: bool,
) -> ModelStatus:
    if is_core_regression_member:
        return "core_estimated"
    if is_projectable:
        return "projected_only"
    return "ineligible"
