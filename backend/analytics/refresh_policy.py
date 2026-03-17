"""Shared refresh gating helpers for analytics and orchestration flows."""

from __future__ import annotations

import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any


def parse_iso_date(value: Any) -> date | None:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(str(value)).date()
    except (TypeError, ValueError):
        return None


def risk_recompute_due(
    meta: dict[str, Any],
    *,
    today_utc: date,
    method_version: str,
    interval_days: int,
) -> tuple[bool, str]:
    if not meta:
        return True, "missing_meta"
    if str(meta.get("method_version") or "") != str(method_version):
        return True, "method_version_change"
    last_recompute = parse_iso_date(meta.get("last_recompute_date"))
    if last_recompute is None:
        return True, "missing_last_recompute_date"
    interval = max(1, int(interval_days))
    if (today_utc - last_recompute).days >= interval:
        return True, f"interval_elapsed_{interval}d"
    return False, "within_interval"


def latest_factor_return_date(cache_db: Path) -> str | None:
    conn = sqlite3.connect(str(cache_db))
    try:
        row = conn.execute("SELECT MAX(date) FROM daily_factor_returns").fetchone()
    except sqlite3.OperationalError:
        row = None
    finally:
        conn.close()
    if not row or not row[0]:
        return None
    return str(row[0])
