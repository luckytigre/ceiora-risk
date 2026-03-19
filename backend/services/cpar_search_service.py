"""Read-only cPAR search payload service."""

from __future__ import annotations

from typing import Any

from backend.data import cpar_outputs
from backend.services import cpar_meta_service


def _search_rank(row: dict[str, Any], needle: str) -> tuple[int, int, str]:
    ticker = str(row.get("ticker") or "").upper()
    name = str(row.get("display_name") or "").upper()
    ric = str(row.get("ric") or "").upper()

    if ticker == needle:
        return (0, 0, ticker)
    if ric == needle:
        return (0, 1, ticker)
    if ticker.startswith(needle):
        return (1, len(ticker), ticker)
    if ric.startswith(needle):
        return (1, len(ric), ticker)
    if needle in ticker:
        return (2, ticker.find(needle), ticker)
    if needle in ric:
        return (2, ric.find(needle), ticker)
    if name.startswith(needle):
        return (3, len(name), ticker)
    return (4, name.find(needle), ticker)


def load_cpar_search_payload(
    *,
    q: str,
    limit: int,
    data_db=None,
) -> dict[str, object]:
    package = cpar_meta_service.require_active_package(data_db=data_db)
    clean_q = str(q or "").strip()
    if not clean_q:
        return {
            **cpar_meta_service.package_meta_payload(package),
            "query": q,
            "limit": int(limit),
            "total": 0,
            "results": [],
        }
    try:
        rows = cpar_outputs.search_package_instrument_fits(
            clean_q,
            package_run_id=str(package["package_run_id"]),
            data_db=data_db,
        )
    except cpar_outputs.CparPackageNotReady as exc:
        raise cpar_meta_service.CparReadNotReady(str(exc)) from exc
    except cpar_outputs.CparAuthorityReadError as exc:
        raise cpar_meta_service.CparReadUnavailable(str(exc)) from exc
    needle = clean_q.upper()
    ranked = sorted(rows, key=lambda row: _search_rank(row, needle))
    hits = ranked[: int(limit)]
    return {
        **cpar_meta_service.package_meta_payload(package),
        "query": q,
        "limit": int(limit),
        "total": len(ranked),
        "results": [
            {
                "ticker": row.get("ticker"),
                "ric": row.get("ric"),
                "display_name": row.get("display_name"),
                "fit_status": row.get("fit_status"),
                "warnings": list(row.get("warnings") or []),
                "hq_country_code": row.get("hq_country_code"),
            }
            for row in hits
        ],
    }
