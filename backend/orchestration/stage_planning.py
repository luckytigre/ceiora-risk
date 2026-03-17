"""Stage-date and as-of planning helpers for the model pipeline."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Callable
from zoneinfo import ZoneInfo

from backend.data import core_reads
from backend.trading_calendar import is_xnys_session, previous_or_same_xnys_session

logger = logging.getLogger(__name__)


def selected_stages_require_source_as_of(selected: list[str]) -> bool:
    return any(stage in {"ingest", "estu_audit"} for stage in selected)


def selected_stages_include_ingest(selected: list[str]) -> bool:
    return "ingest" in selected


def current_xnys_session(*, datetime_cls=datetime) -> str:
    ny_now = datetime_cls.now(ZoneInfo("America/New_York"))
    ny_date = ny_now.date()
    if is_xnys_session(ny_date.isoformat()) and ny_now.time() < datetime_cls.strptime("18:00", "%H:%M").time():
        return previous_or_same_xnys_session((ny_date - timedelta(days=1)).isoformat())
    return previous_or_same_xnys_session(ny_date.isoformat())


def resolved_as_of_date(
    user_as_of_date: str | None,
    *,
    prefer_local_source_archive: bool = False,
    current_xnys_session_resolver: Callable[[], str] | None = None,
) -> str:
    if user_as_of_date and str(user_as_of_date).strip():
        return previous_or_same_xnys_session(str(user_as_of_date).strip())
    try:
        if prefer_local_source_archive:
            with core_reads.core_read_backend("local"):
                source_dates = core_reads.load_source_dates()
        else:
            source_dates = core_reads.load_source_dates()
    except Exception:  # noqa: BLE001
        logger.warning("Falling back to today's session because source dates could not be loaded.", exc_info=True)
        source_dates = {}
    current_session = (
        current_xnys_session_resolver()
        if current_xnys_session_resolver is not None
        else current_xnys_session()
    )
    return previous_or_same_xnys_session(
        str(
            source_dates.get("fundamentals_asof")
            or source_dates.get("exposures_asof")
            or current_session
        )
    )
