"""Section builders for the data-diagnostics service."""

from __future__ import annotations

import sqlite3
from typing import Any

from backend.services import data_diagnostics_sqlite as sqlite_diag


def load_source_tables(
    conn: sqlite3.Connection,
    *,
    include_exact_row_counts: bool = False,
    include_expensive_checks: bool = False,
) -> dict[str, dict[str, Any] | None]:
    canonical_tables = {
        "security_registry": "security_registry",
        "security_policy_current": "security_policy_current",
        "security_taxonomy_current": "security_taxonomy_current",
        "security_source_observation_daily": "security_source_observation_daily",
        "security_master_compat_current": "security_master_compat_current",
        "security_fundamentals_pit": "security_fundamentals_pit",
        "security_classification_pit": "security_classification_pit",
        "security_prices_eod": "security_prices_eod",
        "estu_membership_daily": "estu_membership_daily",
        "barra_raw_cross_section_history": "barra_raw_cross_section_history",
        "universe_cross_section_snapshot": "universe_cross_section_snapshot",
    }
    out: dict[str, dict[str, Any] | None] = {}
    for label, table in canonical_tables.items():
        stats = sqlite_diag.table_stats(
            conn,
            table,
            include_exact_row_counts=include_exact_row_counts,
            include_expensive_checks=include_expensive_checks,
        )
        out[label] = stats if bool(stats.get("exists")) else None
    return out


def resolve_exposure_source(conn: sqlite3.Connection) -> dict[str, Any]:
    table = "barra_raw_cross_section_history"
    latest_asof = None
    if sqlite_diag.table_exists(conn, table):
        row = conn.execute(f"SELECT MAX(as_of_date) FROM {table}").fetchone()
        latest_asof = row[0] if row else None
    return {
        "table": table,
        "selection_mode": "canonical_well_covered_raw_snapshot",
        "is_dynamic": False,
        "latest_asof": str(latest_asof) if latest_asof is not None else None,
        "plain_english": (
            "The analytics engine reads barra_raw_cross_section_history as the canonical raw source, "
            "then selects a single well-covered exposure snapshot for estimation and serving."
        ),
    }


def load_exposure_duplicates(
    conn: sqlite3.Connection,
    *,
    exposure_source_table: str,
    include_expensive_checks: bool = False,
) -> dict[str, dict[str, Any]]:
    if include_expensive_checks:
        return {
            "active_exposure_source": {
                **sqlite_diag.exposure_duplicate_stats(conn, exposure_source_table),
                "computed": True,
            },
        }
    return {
        "active_exposure_source": {
            "table": exposure_source_table,
            "exists": sqlite_diag.table_exists(conn, exposure_source_table),
            "duplicate_groups": None,
            "duplicate_extra_rows": None,
            "computed": False,
        },
    }


def load_eligibility_summary(cache_conn: sqlite3.Connection) -> dict[str, Any]:
    summary = {
        "available": False,
        "latest": None,
        "min_structural_eligible_n": None,
        "max_structural_eligible_n": None,
        "min_core_structural_eligible_n": None,
        "max_core_structural_eligible_n": None,
        "min_regression_member_n": None,
        "max_regression_member_n": None,
        "min_projectable_n": None,
        "max_projectable_n": None,
        "min_projected_only_n": None,
        "max_projected_only_n": None,
    }
    if not sqlite_diag.table_exists(cache_conn, "daily_universe_eligibility_summary"):
        return summary

    elig_cols = sqlite_diag.table_columns(cache_conn, "daily_universe_eligibility_summary")
    core_structural_expr = (
        "core_structural_eligible_n" if "core_structural_eligible_n" in elig_cols else "structural_eligible_n"
    )
    projectable_expr = "projectable_n" if "projectable_n" in elig_cols else "regression_member_n"
    projected_only_expr = "projected_only_n" if "projected_only_n" in elig_cols else "0"
    projectable_coverage_expr = (
        "projectable_coverage" if "projectable_coverage" in elig_cols else "regression_coverage"
    )
    latest = cache_conn.execute(
        f"""
        SELECT date, exp_date, exposure_n, structural_eligible_n,
               {core_structural_expr} AS core_structural_eligible_n,
               regression_member_n,
               {projectable_expr} AS projectable_n,
               {projected_only_expr} AS projected_only_n,
               structural_coverage, regression_coverage,
               {projectable_coverage_expr} AS projectable_coverage,
               alert_level
        FROM daily_universe_eligibility_summary
        ORDER BY date DESC
        LIMIT 1
        """
    ).fetchone()
    mins = cache_conn.execute(
        f"""
        SELECT MIN(structural_eligible_n), MAX(structural_eligible_n),
               MIN({core_structural_expr}), MAX({core_structural_expr}),
               MIN(regression_member_n), MAX(regression_member_n),
               MIN({projectable_expr}), MAX({projectable_expr}),
               MIN({projected_only_expr}), MAX({projected_only_expr})
        FROM daily_universe_eligibility_summary
        """
    ).fetchone()

    summary["available"] = True
    if latest:
        summary["latest"] = {
            "date": str(latest[0]),
            "exp_date": str(latest[1]) if latest[1] is not None else None,
            "exposure_n": int(latest[2] or 0),
            "structural_eligible_n": int(latest[3] or 0),
            "core_structural_eligible_n": int(latest[4] or 0),
            "regression_member_n": int(latest[5] or 0),
            "projectable_n": int(latest[6] or 0),
            "projected_only_n": int(latest[7] or 0),
            "structural_coverage_pct": round(100.0 * float(latest[8] or 0.0), 2),
            "regression_coverage_pct": round(100.0 * float(latest[9] or 0.0), 2),
            "projectable_coverage_pct": round(100.0 * float(latest[10] or 0.0), 2),
            "alert_level": str(latest[11] or ""),
        }
    if mins:
        summary["min_structural_eligible_n"] = int(mins[0] or 0)
        summary["max_structural_eligible_n"] = int(mins[1] or 0)
        summary["min_core_structural_eligible_n"] = int(mins[2] or 0)
        summary["max_core_structural_eligible_n"] = int(mins[3] or 0)
        summary["min_regression_member_n"] = int(mins[4] or 0)
        summary["max_regression_member_n"] = int(mins[5] or 0)
        summary["min_projectable_n"] = int(mins[6] or 0)
        summary["max_projectable_n"] = int(mins[7] or 0)
        summary["min_projected_only_n"] = int(mins[8] or 0)
        summary["max_projected_only_n"] = int(mins[9] or 0)
    return summary


def eligibility_summary_from_runtime_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    latest = dict(payload or {})
    if not latest:
        return {
            "available": False,
            "latest": None,
            "min_structural_eligible_n": None,
            "max_structural_eligible_n": None,
            "min_core_structural_eligible_n": None,
            "max_core_structural_eligible_n": None,
            "min_regression_member_n": None,
            "max_regression_member_n": None,
            "min_projectable_n": None,
            "max_projectable_n": None,
            "min_projected_only_n": None,
            "max_projected_only_n": None,
        }
    structural_eligible_n = int(latest.get("structural_eligible_n") or 0)
    core_structural_eligible_n = int(latest.get("core_structural_eligible_n") or structural_eligible_n)
    regression_member_n = int(latest.get("regression_member_n") or 0)
    projectable_n = int(latest.get("projectable_n") or regression_member_n)
    projected_only_n = int(latest.get("projected_only_n") or 0)
    return {
        "available": True,
        "latest": {
            "date": str(latest.get("date") or ""),
            "exp_date": str(latest.get("exp_date") or latest.get("date") or ""),
            "exposure_n": int(latest.get("exposure_n") or 0),
            "structural_eligible_n": structural_eligible_n,
            "core_structural_eligible_n": core_structural_eligible_n,
            "regression_member_n": regression_member_n,
            "projectable_n": projectable_n,
            "projected_only_n": projected_only_n,
            "structural_coverage_pct": round(100.0 * float(latest.get("structural_coverage") or 0.0), 2),
            "regression_coverage_pct": round(100.0 * float(latest.get("regression_coverage") or 0.0), 2),
            "projectable_coverage_pct": round(100.0 * float(latest.get("projectable_coverage") or 0.0), 2),
            "alert_level": str(latest.get("alert_level") or ""),
        },
        "min_structural_eligible_n": structural_eligible_n,
        "max_structural_eligible_n": structural_eligible_n,
        "min_core_structural_eligible_n": core_structural_eligible_n,
        "max_core_structural_eligible_n": core_structural_eligible_n,
        "min_regression_member_n": regression_member_n,
        "max_regression_member_n": regression_member_n,
        "min_projectable_n": projectable_n,
        "max_projectable_n": projectable_n,
        "min_projected_only_n": projected_only_n,
        "max_projected_only_n": projected_only_n,
    }


def load_factor_cross_section(cache_conn: sqlite3.Connection) -> dict[str, Any]:
    summary = {"available": False, "latest": None, "min_cross_section_n": None, "max_cross_section_n": None}
    if not sqlite_diag.table_exists(cache_conn, "daily_factor_returns"):
        return summary

    latest = cache_conn.execute(
        """
        SELECT date, MIN(cross_section_n), MAX(cross_section_n), MIN(eligible_n), MAX(eligible_n)
        FROM daily_factor_returns
        WHERE date = (SELECT MAX(date) FROM daily_factor_returns)
        """
    ).fetchone()
    minmax = cache_conn.execute(
        "SELECT MIN(cross_section_n), MAX(cross_section_n), MIN(eligible_n), MAX(eligible_n) FROM daily_factor_returns"
    ).fetchone()

    summary["available"] = True
    if latest:
        summary["latest"] = {
            "date": str(latest[0]) if latest[0] is not None else None,
            "cross_section_n_min": int(latest[1] or 0),
            "cross_section_n_max": int(latest[2] or 0),
            "eligible_n_min": int(latest[3] or 0),
            "eligible_n_max": int(latest[4] or 0),
        }
    if minmax:
        summary["min_cross_section_n"] = int(minmax[0] or 0)
        summary["max_cross_section_n"] = int(minmax[1] or 0)
        summary["min_eligible_n"] = int(minmax[2] or 0)
        summary["max_eligible_n"] = int(minmax[3] or 0)
    return summary


def factor_cross_section_from_model_outputs(data_conn: sqlite3.Connection) -> dict[str, Any]:
    summary = {
        "available": False,
        "latest": None,
        "min_cross_section_n": None,
        "max_cross_section_n": None,
        "min_eligible_n": None,
        "max_eligible_n": None,
    }
    if not sqlite_diag.table_exists(data_conn, "model_factor_returns_daily"):
        return summary
    latest = data_conn.execute(
        """
        SELECT date, MIN(cross_section_n), MAX(cross_section_n), MIN(eligible_n), MAX(eligible_n)
        FROM model_factor_returns_daily
        WHERE date = (SELECT MAX(date) FROM model_factor_returns_daily)
        """
    ).fetchone()
    minmax = data_conn.execute(
        """
        SELECT MIN(cross_section_n), MAX(cross_section_n), MIN(eligible_n), MAX(eligible_n)
        FROM model_factor_returns_daily
        """
    ).fetchone()
    if latest is None and minmax is None:
        return summary
    summary["available"] = True
    if latest:
        summary["latest"] = {
            "date": str(latest[0]) if latest[0] is not None else None,
            "cross_section_n_min": int(latest[1] or 0),
            "cross_section_n_max": int(latest[2] or 0),
            "eligible_n_min": int(latest[3] or 0),
            "eligible_n_max": int(latest[4] or 0),
        }
    if minmax:
        summary["min_cross_section_n"] = int(minmax[0] or 0)
        summary["max_cross_section_n"] = int(minmax[1] or 0)
        summary["min_eligible_n"] = int(minmax[2] or 0)
        summary["max_eligible_n"] = int(minmax[3] or 0)
    return summary
