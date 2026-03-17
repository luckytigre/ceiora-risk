"""Canonical data-diagnostics payload assembly."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from backend import config
from backend.data.sqlite import cache_get, cache_get_live_first
from backend.services import data_diagnostics_sections, data_diagnostics_sqlite

DATA_DB = Path(config.DATA_DB_PATH)
CACHE_DB = Path(config.SQLITE_PATH)


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
        elig_summary = data_diagnostics_sections.load_eligibility_summary(cache_conn)
        factor_cross_section = data_diagnostics_sections.load_factor_cross_section(cache_conn)

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
            "risk_engine_meta": cache_get_live_first("risk_engine_meta") or {},
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
