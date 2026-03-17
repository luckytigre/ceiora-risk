"""Factor-history route service."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backend import config
from backend.data.history_queries import (
    load_factor_return_history,
    resolve_factor_history_factor,
)
from backend.data.serving_outputs import load_runtime_payload
from backend.data.sqlite import cache_get


@dataclass(frozen=True)
class FactorHistoryNotReady(RuntimeError):
    cache_key: str
    message: str
    refresh_profile: str = "cold-core"


def _resolve_from_payload_catalog(clean: str) -> tuple[str, str]:
    payload_names = ("universe_factors", "risk", "universe_loadings")
    for payload_name in payload_names:
        payload = load_runtime_payload(payload_name, fallback_loader=cache_get)
        catalog = (payload or {}).get("factor_catalog") if isinstance(payload, dict) else None
        if not isinstance(catalog, list):
            continue
        for entry in catalog:
            if not isinstance(entry, dict):
                continue
            entry_id = str(entry.get("factor_id") or "").strip()
            entry_name = str(entry.get("factor_name") or "").strip()
            if clean == entry_id or clean == entry_name:
                return entry_id or clean, entry_name or clean
        return "", ""
    return "", ""


def resolve_factor_identifier(factor_token: str, *, cache_db: Path | None = None) -> tuple[str, str]:
    clean = str(factor_token or "").strip()
    if not clean:
        return "", ""
    payload_factor_id, payload_factor_name = _resolve_from_payload_catalog(clean)
    if payload_factor_id or payload_factor_name:
        return payload_factor_id, payload_factor_name
    return resolve_factor_history_factor(
        Path(cache_db or config.SQLITE_PATH),
        factor_token=clean,
    )


def load_factor_history_response(
    *,
    factor_token: str,
    years: int,
    cache_db: Path | None = None,
) -> dict[str, Any]:
    resolved_factor_id, factor_name = resolve_factor_identifier(
        factor_token,
        cache_db=cache_db,
    )
    latest, rows = load_factor_return_history(
        Path(cache_db or config.SQLITE_PATH),
        factor=str(factor_name),
        years=int(years),
    )
    if latest is None:
        raise FactorHistoryNotReady(
            cache_key="daily_factor_returns",
            message="Historical factor returns are not available yet.",
        )
    if not rows:
        return {
            "factor_id": resolved_factor_id,
            "factor_name": factor_name,
            "years": int(years),
            "points": [],
            "_cached": True,
        }

    points = []
    cumulative = 1.0
    for dt, raw_ret in rows:
        value = float(raw_ret or 0.0)
        if not math.isfinite(value):
            value = 0.0
        cumulative *= (1.0 + value)
        points.append(
            {
                "date": str(dt),
                "factor_return": round(value, 8),
                "cum_return": round(cumulative - 1.0, 8),
            }
        )

    return {
        "factor_id": resolved_factor_id,
        "factor_name": factor_name,
        "years": int(years),
        "points": points,
        "_cached": True,
    }
