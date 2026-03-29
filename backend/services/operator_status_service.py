"""Compatibility shim for cUSE4 operator-status payload semantics.

Prefer importing ``backend.services.cuse4_operator_status_service`` from the
default cUSE4 operator route. This module remains only for older callers that
still import the legacy path directly.
"""

from __future__ import annotations

from backend.services.cuse4_operator_status_service import (
    OperatorStatusDependencies,
    build_operator_status_payload,
    config,
    get_operator_status_dependencies,
)

__all__ = [
    "OperatorStatusDependencies",
    "build_operator_status_payload",
    "config",
    "get_operator_status_dependencies",
]
