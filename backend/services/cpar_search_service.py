"""Read-only cPAR search payload service."""

from __future__ import annotations

from typing import Any

from backend.data import cpar_outputs
from backend.data import registry_quote_reads
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


def _bool_flag(row: dict[str, Any], key: str) -> bool:
    return bool(int(row.get(key) or 0) == 1)


def _active_cpar_tier(row: dict[str, Any]) -> tuple[str, str, str]:
    fit_status = str(row.get("fit_status") or "").strip()
    target_scope = str(row.get("target_scope") or "").strip().lower()
    if fit_status == "limited_history":
        return (
            "active_package_limited",
            "Active Package (Limited)",
            "The active cPAR package has a usable fit for this security, but history depth or continuity is weaker than ideal.",
        )
    if fit_status == "insufficient_history":
        return (
            "active_package_insufficient",
            "Active Package (Insufficient)",
            "The active cPAR package tracks this security, but there is not enough history to expose a fit.",
        )
    if "core" in target_scope:
        return (
            "active_package_core",
            "Active Core",
            "This security is covered directly in the active cPAR package core target set.",
        )
    return (
        "active_package_extended",
        "Active Extended",
        "This security is covered in the active cPAR package extended target set.",
    )


def _registry_cpar_tier(row: dict[str, Any]) -> tuple[str, str, str]:
    if _bool_flag(row, "allow_cpar_core_target"):
        return (
            "registry_core_target",
            "Core Target",
            "Registry policy admits this security to the cPAR core target path, but it is not present in the active package.",
        )
    if _bool_flag(row, "allow_cpar_extended_target"):
        return (
            "registry_extended_target",
            "Extended Target",
            "Registry policy admits this security to the cPAR extended target path, but it is not present in the active package.",
        )
    return (
        "limited_info",
        "Limited Info",
        "This security is tracked in the registry, but it is not currently admitted to an active cPAR target path.",
    )


def _decorate_active_package_row(row: dict[str, Any]) -> dict[str, Any]:
    risk_tier, risk_tier_label, risk_tier_detail = _active_cpar_tier(row)
    ticker = row.get("ticker")
    stage_supported = bool(str(ticker or "").strip()) and str(row.get("ticker_detail_use_status") or "available") == "available"
    return {
        "ticker": row.get("ticker"),
        "ric": row.get("ric"),
        "display_name": row.get("display_name"),
        "target_scope": row.get("target_scope"),
        "fit_family": row.get("fit_family"),
        "price_on_package_date_status": row.get("price_on_package_date_status"),
        "fit_row_status": row.get("fit_row_status") or "present",
        "fit_quality_status": row.get("fit_quality_status") or row.get("fit_status"),
        "portfolio_use_status": row.get("portfolio_use_status"),
        "ticker_detail_use_status": row.get("ticker_detail_use_status") or "available",
        "hedge_use_status": row.get("hedge_use_status"),
        "reason_code": row.get("reason_code"),
        "quality_label": row.get("quality_label"),
        "fit_status": row.get("fit_status"),
        "warnings": list(row.get("warnings") or []),
        "hq_country_code": row.get("hq_country_code"),
        "risk_tier": risk_tier,
        "risk_tier_label": risk_tier_label,
        "risk_tier_detail": risk_tier_detail,
        "quote_source": "active_package",
        "quote_source_label": "Active cPAR Package",
        "quote_source_detail": "This quote is backed by the active published cPAR package.",
        "scenario_stage_supported": stage_supported,
        "scenario_stage_detail": (
            None
            if stage_supported
            else "cPAR what-if staging stays limited to active-package rows with a resolved ticker."
        ),
    }


def _decorate_registry_row(row: dict[str, Any]) -> dict[str, Any]:
    risk_tier, risk_tier_label, risk_tier_detail = _registry_cpar_tier(row)
    return {
        "ticker": row.get("ticker"),
        "ric": row.get("ric"),
        "display_name": row.get("common_name") or row.get("ticker") or row.get("ric"),
        "target_scope": None,
        "fit_family": None,
        "price_on_package_date_status": "present" if row.get("price") is not None else "missing",
        "fit_row_status": "missing",
        "fit_quality_status": None,
        "portfolio_use_status": "missing_cpar_fit",
        "ticker_detail_use_status": "registry_only",
        "hedge_use_status": "hedge_unavailable",
        "reason_code": "not_in_active_package",
        "quality_label": "registry_only",
        "fit_status": None,
        "warnings": [],
        "hq_country_code": row.get("hq_country_code"),
        "risk_tier": risk_tier,
        "risk_tier_label": risk_tier_label,
        "risk_tier_detail": risk_tier_detail,
        "quote_source": "registry_runtime",
        "quote_source_label": "Registry Runtime",
        "quote_source_detail": "This quote is backed by registry/runtime authority because the active cPAR package does not contain a fit row for it.",
        "scenario_stage_supported": False,
        "scenario_stage_detail": "cPAR what-if staging stays limited to active-package names.",
    }


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
    combined: list[tuple[tuple[int, int, str], int, dict[str, Any]]] = []
    seen_rics: set[str] = set()
    for row in rows:
        decorated = _decorate_active_package_row(row)
        seen_rics.add(str(decorated.get("ric") or "").upper())
        combined.append((_search_rank(decorated, needle), 0, decorated))
    try:
        registry_rows = registry_quote_reads.search_registry_quote_rows(
            clean_q,
            limit=max(int(limit) * 8, int(limit)),
            as_of_date=str(package["package_date"]),
            data_db=data_db,
        )
    except registry_quote_reads.RegistryQuoteReadError:
        registry_rows = []
    for row in registry_rows:
        clean_ric = str(row.get("ric") or "").upper().strip()
        if not clean_ric or clean_ric in seen_rics:
            continue
        decorated = _decorate_registry_row(row)
        combined.append((_search_rank(decorated, needle), 1, decorated))
        seen_rics.add(clean_ric)
    ranked = sorted(combined, key=lambda row: (row[0], row[1]))
    hits = [row for _, _, row in ranked[: int(limit)]]
    return {
        **cpar_meta_service.package_meta_payload(package),
        "query": q,
        "limit": int(limit),
        "total": len(ranked),
        "results": hits,
    }
