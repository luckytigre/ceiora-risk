"""Explicit cUSE4 alias for operator-status payload semantics."""

from __future__ import annotations

from typing import Any

from backend.services import operator_status_service as _legacy


config = _legacy.config
core_reads = _legacy.core_reads
get_holdings_sync_state = _legacy.get_holdings_sync_state
get_refresh_status = _legacy.get_refresh_status
job_runs = _legacy.job_runs
_load_authoritative_operator_source_dates = _legacy._load_authoritative_operator_source_dates
_load_local_archive_source_dates = _legacy._load_local_archive_source_dates
_now_iso = _legacy._now_iso
_risk_recompute_due = _legacy._risk_recompute_due
_today_session_date = _legacy._today_session_date
profile_catalog = _legacy.profile_catalog
runtime_state = _legacy.runtime_state
sqlite = _legacy.sqlite


def _sync_legacy_bindings() -> None:
    _legacy.config = config
    _legacy.core_reads = core_reads
    _legacy.get_holdings_sync_state = get_holdings_sync_state
    _legacy.get_refresh_status = get_refresh_status
    _legacy.job_runs = job_runs
    _legacy.profile_catalog = profile_catalog
    _legacy.runtime_state = runtime_state
    _legacy.sqlite = sqlite
    _legacy._load_authoritative_operator_source_dates = _load_authoritative_operator_source_dates
    _legacy._load_local_archive_source_dates = _load_local_archive_source_dates
    _legacy._now_iso = _now_iso
    _legacy._risk_recompute_due = _risk_recompute_due
    _legacy._today_session_date = _today_session_date


def build_operator_status_payload() -> dict[str, Any]:
    _sync_legacy_bindings()
    return _legacy.build_operator_status_payload()

__all__ = [
    "build_operator_status_payload",
    "config",
    "core_reads",
    "get_holdings_sync_state",
    "get_refresh_status",
    "job_runs",
    "_load_authoritative_operator_source_dates",
    "_load_local_archive_source_dates",
    "_now_iso",
    "_risk_recompute_due",
    "_today_session_date",
    "profile_catalog",
    "runtime_state",
    "sqlite",
]
