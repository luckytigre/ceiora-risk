"""XNYS trading-calendar utilities used across ingestion and analytics."""

from __future__ import annotations

from functools import lru_cache
from typing import Iterable

import pandas as pd


@lru_cache(maxsize=1)
def _xnys_calendar():
    try:
        import exchange_calendars as xcals
    except ImportError as exc:  # pragma: no cover - runtime dependency guard
        raise RuntimeError(
            "exchange_calendars is required for XNYS business-day policy. "
            "Install it with `pip install exchange-calendars`."
        ) from exc
    return xcals.get_calendar("XNYS")


def _to_norm_ts(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.normalize()


def _sessions_in_range(start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
    cal = _xnys_calendar()
    sessions = cal.sessions_in_range(start, end)
    sessions = pd.DatetimeIndex(pd.to_datetime(sessions))
    if sessions.tz is not None:
        sessions = sessions.tz_convert("UTC").tz_localize(None)
    return sessions.normalize()


def previous_or_same_xnys_session(value: str | pd.Timestamp) -> str:
    """Return ISO date for the nearest XNYS session <= value."""
    target = _to_norm_ts(value)
    # 2 years back is ample for nearest-previous session lookup.
    sessions = _sessions_in_range(target - pd.Timedelta(days=730), target)
    if len(sessions) == 0:
        raise ValueError(f"No XNYS session found on or before {target.date().isoformat()}")
    return str(sessions[-1].date())


def lagged_xnys_session(value: str | pd.Timestamp, days: int) -> str:
    """Return the lagged XNYS session used by the core regression age guard."""
    shift = max(0, int(days))
    target = _to_norm_ts(value)
    shifted = target if shift <= 0 else (target - pd.Timedelta(days=shift))
    return previous_or_same_xnys_session(shifted)


def is_xnys_session(value: str | pd.Timestamp) -> bool:
    target = _to_norm_ts(value)
    sessions = _sessions_in_range(target, target)
    return bool(len(sessions) == 1 and sessions[0] == target)


def non_xnys_dates(values: Iterable[str]) -> list[str]:
    """Return sorted unique ISO dates that are not XNYS sessions."""
    uniq = sorted({str(v) for v in values if str(v).strip()})
    if not uniq:
        return []
    min_d = _to_norm_ts(uniq[0])
    max_d = _to_norm_ts(uniq[-1])
    sessions = _sessions_in_range(min_d, max_d)
    session_set = {str(ts.date()) for ts in sessions}
    return [d for d in uniq if d not in session_set]


def filter_xnys_sessions(values: Iterable[str]) -> list[str]:
    """Return sorted unique ISO dates that are valid XNYS sessions."""
    uniq = sorted({str(v) for v in values if str(v).strip()})
    if not uniq:
        return []
    blocked = set(non_xnys_dates(uniq))
    return [d for d in uniq if d not in blocked]
