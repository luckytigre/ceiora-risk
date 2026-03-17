"""Source-date helpers for canonical source reads."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Callable

from backend import config
from backend.trading_calendar import previous_or_same_xnys_session


def _pit_latest_closed_anchor(*, today: date | None = None) -> str:
    current = today or datetime.utcnow().date()
    frequency = str(config.SOURCE_DAILY_PIT_FREQUENCY or "monthly").strip().lower()
    if frequency == "quarterly":
        quarter_start_month = (((current.month - 1) // 3) * 3) + 1
        period_start = date(current.year, quarter_start_month, 1)
    else:
        period_start = date(current.year, current.month, 1)
    previous_day = (period_start - timedelta(days=1)).isoformat()
    return previous_or_same_xnys_session(previous_day)


def resolve_latest_barra_tuple(
    *,
    fetch_rows_fn: Callable[[str, list[Any] | None], list[dict[str, Any]]],
    exposure_source_table_required_fn: Callable[[], str],
) -> dict[str, str] | None:
    table = exposure_source_table_required_fn()
    rows = fetch_rows_fn(
        f"""
        SELECT as_of_date, barra_model_version, descriptor_schema_version, assumption_set_version
        FROM {table}
        ORDER BY as_of_date DESC, updated_at DESC
        LIMIT 1
        """,
        None,
    )
    if not rows:
        return None
    row = rows[0]
    return {
        "as_of_date": str(row.get("as_of_date") or ""),
        "barra_model_version": str(row.get("barra_model_version") or ""),
        "descriptor_schema_version": str(row.get("descriptor_schema_version") or ""),
        "assumption_set_version": str(row.get("assumption_set_version") or ""),
    }


def load_source_dates(
    *,
    fetch_rows_fn: Callable[[str, list[Any] | None], list[dict[str, Any]]],
    table_exists_fn: Callable[[str], bool],
    exposure_source_table_required_fn: Callable[[], str],
) -> dict[str, str | None]:
    def _max_val(sql: str) -> str | None:
        rows = fetch_rows_fn(sql, None)
        if not rows:
            return None
        val = rows[0].get("latest")
        return str(val) if val else None

    pit_latest_closed_anchor = _pit_latest_closed_anchor()
    fundamentals_asof = None
    if table_exists_fn("security_fundamentals_pit"):
        fundamentals_asof = _max_val(
            f"""
            SELECT MAX(as_of_date) AS latest
            FROM security_fundamentals_pit
            WHERE as_of_date <= '{pit_latest_closed_anchor}'
            """
        )
    classification_asof = None
    if table_exists_fn("security_classification_pit"):
        classification_asof = _max_val(
            f"""
            SELECT MAX(as_of_date) AS latest
            FROM security_classification_pit
            WHERE as_of_date <= '{pit_latest_closed_anchor}'
            """
        )
    prices_asof = None
    if table_exists_fn("security_prices_eod"):
        prices_asof = _max_val(
            "SELECT MAX(date) AS latest FROM security_prices_eod"
        )

    exposures_latest_available_asof = _max_val(
        f"SELECT MAX(as_of_date) AS latest FROM {exposure_source_table_required_fn()}"
    )

    return {
        "fundamentals_asof": fundamentals_asof,
        "classification_asof": classification_asof,
        "prices_asof": prices_asof,
        "exposures_asof": exposures_latest_available_asof,
        "exposures_latest_available_asof": exposures_latest_available_asof,
    }
