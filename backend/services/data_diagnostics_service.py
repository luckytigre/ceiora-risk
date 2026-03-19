"""Canonical data-diagnostics payload assembly."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from backend import config
from backend.data import model_outputs
from backend.data.serving_outputs import load_runtime_payload
from backend.data.sqlite import cache_get, cache_get_live_first
from backend.services import data_diagnostics_sections, data_diagnostics_sqlite

DATA_DB = Path(config.DATA_DB_PATH)
CACHE_DB = Path(config.SQLITE_PATH)


def _with_section_source(section: dict[str, Any], source: str) -> dict[str, Any]:
    out = dict(section)
    out["source"] = source
    return out


def _effective_risk_engine_meta() -> dict[str, Any]:
    runtime_meta = cache_get_live_first("risk_engine_meta") or {}
    if isinstance(runtime_meta, dict) and runtime_meta:
        return runtime_meta
    return model_outputs.load_latest_local_diagnostic_risk_engine_state()


def build_data_diagnostics_payload(
    *,
    include_paths: bool = False,
    include_exact_row_counts: bool = False,
    include_expensive_checks: bool = False,
) -> dict[str, Any]:
    data_conn = sqlite3.connect(str(DATA_DB))
    cache_conn = sqlite3.connect(str(CACHE_DB))
    try:
        source_tables = data_diagnostics_sections.load_source_tables(
            data_conn,
            include_exact_row_counts=include_exact_row_counts,
            include_expensive_checks=include_expensive_checks,
        )
        exposure_source = data_diagnostics_sections.resolve_exposure_source(data_conn)
        exposure_source_table = str(exposure_source.get("table") or "")
        dup_stats = data_diagnostics_sections.load_exposure_duplicates(
            data_conn,
            exposure_source_table=exposure_source_table,
            include_expensive_checks=include_expensive_checks,
        )
        runtime_elig_summary = data_diagnostics_sections.eligibility_summary_from_runtime_payload(
            load_runtime_payload("eligibility", fallback_loader=cache_get)
            if DATA_DB.exists()
            else None
        )
        cache_elig_summary = data_diagnostics_sections.load_eligibility_summary(cache_conn)
        if bool(runtime_elig_summary.get("available")):
            elig_summary = _with_section_source(runtime_elig_summary, "durable_serving_payload:eligibility")
        else:
            elig_summary = _with_section_source(cache_elig_summary, "legacy_cache:daily_universe_eligibility_summary")

        model_output_cross_section = data_diagnostics_sections.factor_cross_section_from_model_outputs(data_conn)
        cache_factor_cross_section = data_diagnostics_sections.load_factor_cross_section(cache_conn)
        if bool(model_output_cross_section.get("available")):
            factor_cross_section = _with_section_source(
                model_output_cross_section,
                "durable_model_outputs:model_factor_returns_daily",
            )
        else:
            factor_cross_section = _with_section_source(
                cache_factor_cross_section,
                "legacy_cache:daily_factor_returns",
            )

        payload = {
            "status": "ok",
            "database_path": DATA_DB.name,
            "cache_db_path": CACHE_DB.name,
            "diagnostic_scope": {
                "source": "local_sqlite_and_cache",
                "plain_english": (
                    "Detailed diagnostics reflect this backend instance's local SQLite ingest/archive and cache state. "
                    "Use the Health page for authoritative operator truth, lane status, and Neon health."
                ),
            },
            "truth_surfaces": {
                "dashboard_serving": {
                    "source": "durable_serving_payloads",
                    "plain_english": (
                        "Risk, Explore, Positions, Health, and other user-facing pages should read compact durable serving payloads "
                        "instead of rebuilding directly from raw source tables."
                    ),
                },
                "operator_status": {
                    "source": "runtime_status_and_job_runs",
                    "plain_english": (
                        "Operator status is the live control-room truth for lane status, holdings dirty state, active snapshot, "
                        "authoritative source recency, and Neon mirror/parity health."
                    ),
                },
                "local_diagnostics": {
                    "source": "local_sqlite_and_cache",
                    "plain_english": (
                        "This diagnostics endpoint inspects the current backend instance and its local SQLite/cache files. "
                        "Treat it as a deep local-ingest/archive panel, not the live operator control room."
                    ),
                },
            },
            "exposure_source_table": exposure_source_table,
            "exposure_source": exposure_source,
            "source_tables": source_tables,
            "exposure_duplicates": dup_stats,
            "cross_section_usage": {
                "eligibility_summary": elig_summary,
                "factor_cross_section": factor_cross_section,
            },
            "risk_engine_meta": _effective_risk_engine_meta(),
            "cuse4_foundation": cache_get("cuse4_foundation") or {},
            "cache_outputs": data_diagnostics_sqlite.load_cache_rows(CACHE_DB),
        }
        if include_paths:
            payload["database_path"] = str(DATA_DB)
            payload["cache_db_path"] = str(CACHE_DB)
        return payload
    finally:
        data_conn.close()
        cache_conn.close()
