"""Compatibility shim for cUSE4 dashboard payload assembly.

Prefer importing ``backend.services.cuse4_dashboard_payload_service`` from the
default cUSE4 route family. This module remains only for older callers and
tests that still import the legacy path directly.
"""

from __future__ import annotations

from backend.services.cuse4_dashboard_payload_service import (
    DashboardPayloadNotReady,
    DashboardPayloadReaders,
    cache_get,
    get_dashboard_payload_readers,
    load_exposures_response,
    load_portfolio_response,
    load_risk_covariance_response,
    load_risk_response,
    load_risk_summary_response,
    load_runtime_payload,
)

__all__ = [
    "DashboardPayloadNotReady",
    "DashboardPayloadReaders",
    "cache_get",
    "get_dashboard_payload_readers",
    "load_exposures_response",
    "load_portfolio_response",
    "load_risk_covariance_response",
    "load_risk_response",
    "load_risk_summary_response",
    "load_runtime_payload",
]
