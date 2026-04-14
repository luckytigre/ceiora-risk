"""Persistence coordinator for analytics refresh outputs."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend import config
from backend.data.cuse_membership_reads import load_cuse_membership_rows
from backend.data import model_outputs, runtime_state, serving_outputs, sqlite
from backend.risk_model.cuse_membership import membership_row_to_overlay

logger = logging.getLogger(__name__)

_UNIVERSE_OVERLAY_FIELDS = (
    "model_status",
    "model_status_reason",
    "eligibility_reason",
    "exposure_origin",
    "projection_method",
    "projection_r_squared",
    "projection_obs_count",
    "projection_asof",
    "cuse_realized_role",
    "cuse_output_status",
    "cuse_reason_code",
    "quality_label",
    "projection_basis_status",
    "projection_candidate_status",
    "projection_output_status",
    "served_exposure_available",
)


def _membership_warning(
    *,
    policy_path: str,
    output_status: str,
    projection_source_package_date: str,
    existing_warning: str,
) -> str:
    if output_status == "projection_unavailable":
        if projection_source_package_date:
            return (
                "Returns-projection candidate has no persisted projected loadings for "
                f"active core package {projection_source_package_date}."
            )
        return "Returns-projection candidate has no persisted projected loadings for the active core package."
    if output_status == "unavailable" and policy_path == "fundamental_projection_candidate":
        return "Fundamental-projection candidate is not currently served from the active cUSE snapshot."
    return existing_warning


def _load_current_membership_lookup(
    *,
    data_db: Path,
    universe_payload: dict[str, Any],
) -> dict[tuple[str, str], dict[str, Any]]:
    by_ticker = dict(universe_payload.get("by_ticker") or {})
    relevant_dates = sorted(
        {
            str(row.get("as_of_date") or universe_payload.get("as_of_date") or "").strip()
            for row in by_ticker.values()
            if str(row.get("as_of_date") or universe_payload.get("as_of_date") or "").strip()
        }
    )
    if not relevant_dates:
        return {}
    rows = load_cuse_membership_rows(data_db=data_db, as_of_dates=relevant_dates)
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        as_of_date = str(row.get("as_of_date") or "").strip()
        ticker = str(row.get("ticker") or "").strip().upper()
        ric = str(row.get("ric") or "").strip().upper()
        if as_of_date and ticker:
            lookup[(as_of_date, ticker)] = dict(row)
        if as_of_date and ric:
            lookup[(as_of_date, ric)] = dict(row)
    return lookup


def _assert_current_membership_coverage(
    *,
    universe_payload: dict[str, Any],
    membership_lookup: dict[tuple[str, str], dict[str, Any]],
) -> None:
    by_ticker = dict(universe_payload.get("by_ticker") or {})
    missing: list[str] = []
    for ticker, raw_row in by_ticker.items():
        row = dict(raw_row or {})
        as_of_date = str(row.get("as_of_date") or universe_payload.get("as_of_date") or "").strip()
        clean_ticker = str(row.get("ticker") or ticker).strip().upper()
        clean_ric = str(row.get("ric") or "").strip().upper()
        if not as_of_date or not (clean_ticker or clean_ric):
            continue
        if (as_of_date, clean_ticker) in membership_lookup:
            continue
        if clean_ric and (as_of_date, clean_ric) in membership_lookup:
            continue
        missing.append(f"{clean_ticker or clean_ric}@{as_of_date}")
    if missing:
        sample = ", ".join(sorted(missing)[:20])
        raise RuntimeError(
            "Current cUSE membership truth is incomplete for serving publish: "
            f"{sample}"
        )


def _apply_current_membership_to_universe_payload(
    *,
    data_db: Path,
    universe_payload: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    by_ticker = dict(universe_payload.get("by_ticker") or {})
    if not by_ticker:
        return universe_payload, False
    membership_lookup = _load_current_membership_lookup(
        data_db=data_db,
        universe_payload=universe_payload,
    )
    if not membership_lookup:
        # Membership is completely absent — fail hard so the publish is blocked
        # rather than serving a universe with no membership truth applied.
        _assert_current_membership_coverage(
            universe_payload=universe_payload,
            membership_lookup=membership_lookup,
        )
        return universe_payload, False

    updated = False
    dropped: list[str] = []
    updated_by_ticker: dict[str, dict[str, Any]] = {}
    for ticker, raw_row in by_ticker.items():
        row = dict(raw_row or {})
        as_of_date = str(row.get("as_of_date") or universe_payload.get("as_of_date") or "").strip()
        clean_ticker = str(row.get("ticker") or ticker).strip().upper()
        clean_ric = str(row.get("ric") or "").strip().upper()
        membership_row = None
        if as_of_date and clean_ticker:
            membership_row = membership_lookup.get((as_of_date, clean_ticker))
        if membership_row is None and as_of_date and clean_ric:
            membership_row = membership_lookup.get((as_of_date, clean_ric))
        if membership_row is None:
            # Membership exists for the core set but this ticker has no row —
            # it is outside the modelled universe (e.g. an ETF price ticker that
            # was never admitted by security_registry). Drop it from the serving
            # payload so the universe stays consistent with what was modelled.
            dropped.append(clean_ticker or clean_ric or ticker)
            updated = True
            continue

        overlay = membership_row_to_overlay(
            membership_row,
            payload_exposures=dict(row.get("exposures") or {}),
        )
        row.update(overlay)
        if membership_row.get("projection_method") and not row.get("projection_method"):
            row["projection_method"] = membership_row.get("projection_method")
        if membership_row.get("projection_source_package_date") and not row.get("projection_asof"):
            row["projection_asof"] = membership_row.get("projection_source_package_date")
        row["model_warning"] = _membership_warning(
            policy_path=str(membership_row.get("policy_path") or ""),
            output_status=str(membership_row.get("output_status") or ""),
            projection_source_package_date=str(membership_row.get("projection_source_package_date") or ""),
            existing_warning=str(row.get("model_warning") or ""),
        )
        updated_by_ticker[ticker] = row
        updated = True

    if dropped:
        logger.warning(
            "Dropped %d tickers from serving universe with no cUSE membership coverage: %s%s",
            len(dropped),
            ", ".join(sorted(dropped)[:20]),
            "" if len(dropped) <= 20 else f" ... (+{len(dropped) - 20} more)",
        )

    if not updated:
        return universe_payload, False

    updated_payload = dict(universe_payload)
    updated_payload["by_ticker"] = updated_by_ticker

    dropped_set = set(dropped)
    if isinstance(updated_payload.get("index"), list):
        updated_index: list[dict[str, Any]] = []
        for raw_index_row in updated_payload["index"]:
            index_row = dict(raw_index_row or {})
            clean_ticker = str(index_row.get("ticker") or "").strip().upper()
            if clean_ticker in dropped_set:
                continue
            source_row = updated_by_ticker.get(clean_ticker)
            if source_row is not None:
                for field in _UNIVERSE_OVERLAY_FIELDS:
                    if field in source_row:
                        index_row[field] = source_row.get(field)
            updated_index.append(index_row)
        updated_payload["index"] = updated_index

    model_statuses = [
        str(row.get("model_status") or "").strip()
        for row in updated_by_ticker.values()
    ]
    core_estimated_count = sum(1 for status in model_statuses if status == "core_estimated")
    projected_only_count = sum(1 for status in model_statuses if status == "projected_only")
    ineligible_count = sum(1 for status in model_statuses if status == "ineligible")
    updated_payload["ticker_count"] = len(updated_by_ticker)
    updated_payload["core_estimated_ticker_count"] = core_estimated_count
    updated_payload["projected_only_ticker_count"] = projected_only_count
    updated_payload["ineligible_ticker_count"] = ineligible_count
    updated_payload["eligible_ticker_count"] = core_estimated_count + projected_only_count
    return updated_payload, True


def _apply_universe_status_to_portfolio_payload(
    *,
    portfolio_payload: dict[str, Any],
    universe_payload: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    positions = list(portfolio_payload.get("positions") or [])
    if not positions:
        return portfolio_payload, False
    by_ticker = dict(universe_payload.get("by_ticker") or {})
    updated_positions: list[dict[str, Any]] = []
    updated = False
    for raw_position in positions:
        position = dict(raw_position or {})
        ticker = str(position.get("ticker") or "").strip().upper()
        source_row = by_ticker.get(ticker)
        if source_row is None:
            updated_positions.append(position)
            continue
        for field in _UNIVERSE_OVERLAY_FIELDS:
            if field in source_row:
                position[field] = source_row.get(field)
        updated_positions.append(position)
        updated = True
    if not updated:
        return portfolio_payload, False
    updated_payload = dict(portfolio_payload)
    updated_payload["positions"] = updated_positions
    return updated_payload, True


def _apply_universe_status_to_exposures_payload(
    *,
    exposures_payload: dict[str, Any],
    universe_payload: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    by_ticker = dict(universe_payload.get("by_ticker") or {})
    updated_payload = dict(exposures_payload)
    updated = False
    for mode in ("raw", "sensitivity", "risk_contribution"):
        rows = list(updated_payload.get(mode) or [])
        updated_rows: list[dict[str, Any]] = []
        for raw_row in rows:
            row = dict(raw_row or {})
            drilldown = list(row.get("drilldown") or [])
            updated_drilldown: list[dict[str, Any]] = []
            for raw_item in drilldown:
                item = dict(raw_item or {})
                ticker = str(item.get("ticker") or "").strip().upper()
                source_row = by_ticker.get(ticker)
                if source_row is not None:
                    item["model_status"] = source_row.get("model_status")
                    item["exposure_origin"] = source_row.get("exposure_origin")
                    updated = True
                updated_drilldown.append(item)
            row["drilldown"] = updated_drilldown
            updated_rows.append(row)
        updated_payload[mode] = updated_rows
    return updated_payload, updated


def _overlay_current_membership_truth(
    *,
    data_db: Path,
    cache_db: Path,
    snapshot_id: str,
    persisted_payloads: dict[str, Any],
) -> dict[str, Any]:
    universe_payload = dict(persisted_payloads.get("universe_loadings") or {})
    if not universe_payload:
        return persisted_payloads
    updated_universe_payload, universe_updated = _apply_current_membership_to_universe_payload(
        data_db=data_db,
        universe_payload=universe_payload,
    )
    if not universe_updated:
        return persisted_payloads

    updated_payloads = dict(persisted_payloads)
    updated_payloads["universe_loadings"] = updated_universe_payload

    universe_factors_payload = dict(updated_payloads.get("universe_factors") or {})
    if universe_factors_payload:
        universe_factors_payload["ticker_count"] = updated_universe_payload.get("ticker_count", universe_factors_payload.get("ticker_count"))
        universe_factors_payload["eligible_ticker_count"] = updated_universe_payload.get("eligible_ticker_count", universe_factors_payload.get("eligible_ticker_count"))
        universe_factors_payload["core_estimated_ticker_count"] = updated_universe_payload.get("core_estimated_ticker_count", universe_factors_payload.get("core_estimated_ticker_count"))
        universe_factors_payload["projected_only_ticker_count"] = updated_universe_payload.get("projected_only_ticker_count", universe_factors_payload.get("projected_only_ticker_count"))
        universe_factors_payload["ineligible_ticker_count"] = updated_universe_payload.get("ineligible_ticker_count", universe_factors_payload.get("ineligible_ticker_count"))
        updated_payloads["universe_factors"] = universe_factors_payload

    portfolio_payload = dict(updated_payloads.get("portfolio") or {})
    if portfolio_payload:
        updated_portfolio_payload, portfolio_updated = _apply_universe_status_to_portfolio_payload(
            portfolio_payload=portfolio_payload,
            universe_payload=updated_universe_payload,
        )
        if portfolio_updated:
            updated_payloads["portfolio"] = updated_portfolio_payload

    exposures_payload = dict(updated_payloads.get("exposures") or {})
    if exposures_payload:
        updated_exposures_payload, exposures_updated = _apply_universe_status_to_exposures_payload(
            exposures_payload=exposures_payload,
            universe_payload=updated_universe_payload,
        )
        if exposures_updated:
            updated_payloads["exposures"] = updated_exposures_payload

    for payload_name in ("universe_loadings", "universe_factors", "portfolio", "exposures"):
        if payload_name in updated_payloads:
            sqlite.cache_set(
                payload_name,
                updated_payloads[payload_name],
                snapshot_id=snapshot_id,
                db_path=cache_db,
            )
    return updated_payloads


def persist_refresh_outputs(
    *,
    data_db: Path,
    cache_db: Path,
    run_id: str,
    snapshot_id: str,
    refresh_mode: str,
    refresh_started_at: str,
    recomputed_this_refresh: bool,
    params: dict[str, Any],
    source_dates: dict[str, Any],
    risk_engine_state: dict[str, Any],
    cov,
    specific_risk_by_security: dict[str, Any],
    persisted_payloads: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    model_outputs_write: dict[str, Any] = {"status": "skipped"}
    serving_outputs_write: dict[str, Any] = {"status": "skipped"}
    try:
        if not recomputed_this_refresh:
            model_outputs_write = {
                "status": "skipped",
                "reason": "risk_engine_reused",
                "run_id": run_id,
            }
        else:
            model_outputs_write = model_outputs.persist_model_outputs(
                data_db=data_db,
                cache_db=cache_db,
                run_id=run_id,
                refresh_mode=refresh_mode,
                status="ok",
                started_at=refresh_started_at,
                completed_at=datetime.now(timezone.utc).isoformat(),
                source_dates=source_dates,
                params=params,
                risk_engine_state=risk_engine_state,
                cov=cov,
                specific_risk_by_ticker=specific_risk_by_security,
                persisted_payloads=persisted_payloads,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to persist relational model outputs")
        model_outputs_write = {
            "status": "error",
            "run_id": run_id,
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
        sqlite.cache_set("model_outputs_write", model_outputs_write, db_path=cache_db)
        raise RuntimeError(f"Relational model output persistence failed: {type(exc).__name__}: {exc}") from exc
    sqlite.cache_set("model_outputs_write", model_outputs_write, db_path=cache_db)

    persisted_payloads = _overlay_current_membership_truth(
        data_db=data_db,
        cache_db=cache_db,
        snapshot_id=snapshot_id,
        persisted_payloads=persisted_payloads,
    )

    try:
        serving_outputs_write = serving_outputs.persist_current_payloads(
            data_db=data_db,
            run_id=run_id,
            snapshot_id=snapshot_id,
            refresh_mode=refresh_mode,
            payloads=persisted_payloads,
            replace_all=True,
        )
        neon_write = serving_outputs_write.get("neon_write") if isinstance(serving_outputs_write, dict) else None
        if (
            config.serving_payload_neon_write_required()
            and isinstance(neon_write, dict)
            and str(neon_write.get("status") or "") != "ok"
        ):
            raise RuntimeError(f"Serving payload Neon write failed: {neon_write}")
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to persist serving payloads")
        serving_outputs_write = {
            "status": "error",
            "run_id": run_id,
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
        sqlite.cache_set("serving_outputs_write", serving_outputs_write, db_path=cache_db)
        raise RuntimeError(f"Serving payload persistence failed: {type(exc).__name__}: {exc}") from exc
    sqlite.cache_set("serving_outputs_write", serving_outputs_write, db_path=cache_db)
    runtime_state.persist_runtime_state(
        "risk_engine_meta",
        risk_engine_state,
        fallback_writer=lambda key, value: sqlite.cache_set(key, value, db_path=cache_db),
    )
    runtime_state.publish_active_snapshot(
        snapshot_id,
        fallback_publisher=lambda sid: sqlite.cache_publish_snapshot(sid, db_path=cache_db),
    )
    return model_outputs_write, serving_outputs_write
