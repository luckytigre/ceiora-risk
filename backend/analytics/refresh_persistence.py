"""Persistence coordinator for analytics refresh outputs."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend import config
from backend.analytics import reuse_policy
from backend.data.cuse_membership_reads import load_cuse_membership_rows
from backend.data import model_outputs, runtime_state, serving_outputs, sqlite
from backend.risk_model.cuse_membership import build_cuse_membership_payloads, membership_row_to_overlay

logger = logging.getLogger(__name__)

_MAX_ALLOWED_MEMBERSHIP_MISSES = 25
_MAX_ALLOWED_MEMBERSHIP_MISS_FRACTION = 0.05
_MAX_ALLOWED_MEMBERSHIP_MISS_COUNT_FOR_FRACTION = 5

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

_MEMBERSHIP_PAYLOAD_COLUMNS = (
    "as_of_date",
    "ric",
    "ticker",
    "policy_path",
    "realized_role",
    "output_status",
    "projection_candidate_status",
    "projection_output_status",
    "reason_code",
    "quality_label",
    "source_snapshot_status",
    "projection_method",
    "projection_basis_status",
    "projection_source_package_date",
    "served_exposure_available",
    "run_id",
    "updated_at",
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


def _membership_rows_from_payload(
    membership_payload: list[tuple[Any, ...]] | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for payload_row in membership_payload or []:
        if not payload_row:
            continue
        rows.append(
            {
                _MEMBERSHIP_PAYLOAD_COLUMNS[idx]: payload_row[idx]
                for idx in range(min(len(payload_row), len(_MEMBERSHIP_PAYLOAD_COLUMNS)))
            }
        )
    return rows


def _build_membership_lookup(
    rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for row in rows:
        ticker = str(row.get("ticker") or "").strip().upper()
        ric = str(row.get("ric") or "").strip().upper()
        if ticker:
            lookup[ticker] = dict(row)
        if ric:
            lookup[ric] = dict(row)
    return lookup


def _load_current_membership_lookup(
    *,
    data_db: Path,
    universe_payload: dict[str, Any],
    membership_rows: list[dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    if not dict(universe_payload.get("by_ticker") or {}):
        return {}
    rows = list(membership_rows or [])
    if not rows:
        rows = load_cuse_membership_rows(data_db=data_db, as_of_dates=None)
    return _build_membership_lookup(rows)


def _assert_current_membership_coverage(
    *,
    universe_payload: dict[str, Any],
    membership_lookup: dict[str, dict[str, Any]],
) -> None:
    by_ticker = dict(universe_payload.get("by_ticker") or {})
    missing: list[str] = []
    for ticker, raw_row in by_ticker.items():
        row = dict(raw_row or {})
        clean_ticker = str(row.get("ticker") or ticker).strip().upper()
        clean_ric = str(row.get("ric") or "").strip().upper()
        if not (clean_ticker or clean_ric):
            continue
        if clean_ticker in membership_lookup:
            continue
        if clean_ric and clean_ric in membership_lookup:
            continue
        missing.append(clean_ticker or clean_ric)
    if missing:
        sample = ", ".join(sorted(missing)[:20])
        raise RuntimeError(
            "Current cUSE membership truth is incomplete for serving publish: "
            f"{sample}"
        )


def _assert_membership_coverage_not_partial(
    *,
    universe_payload: dict[str, Any],
    matched: int,
    missing: list[str],
) -> None:
    if not missing:
        return
    total = int(matched + len(missing))
    if total <= 0:
        return
    missing_count = int(len(missing))
    missing_fraction = float(missing_count) / float(total)
    if (
        missing_count <= _MAX_ALLOWED_MEMBERSHIP_MISSES
        and (
            missing_count <= _MAX_ALLOWED_MEMBERSHIP_MISS_COUNT_FOR_FRACTION
            or missing_fraction <= _MAX_ALLOWED_MEMBERSHIP_MISS_FRACTION
        )
    ):
        return
    sample = ", ".join(sorted(missing)[:20])
    raise RuntimeError(
        "Current cUSE membership coverage is too incomplete for serving publish: "
        f"matched={matched} missing={missing_count} total={total} "
        f"missing_pct={round(missing_fraction * 100.0, 2)} sample={sample}"
    )


def _apply_current_membership_to_universe_payload(
    *,
    data_db: Path,
    universe_payload: dict[str, Any],
    membership_rows: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], bool]:
    by_ticker = dict(universe_payload.get("by_ticker") or {})
    if not by_ticker:
        return universe_payload, False
    membership_lookup = _load_current_membership_lookup(
        data_db=data_db,
        universe_payload=universe_payload,
        membership_rows=membership_rows,
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
    matched = 0
    updated_by_ticker: dict[str, dict[str, Any]] = {}
    for ticker, raw_row in by_ticker.items():
        row = dict(raw_row or {})
        clean_ticker = str(row.get("ticker") or ticker).strip().upper()
        clean_ric = str(row.get("ric") or "").strip().upper()
        membership_row = None
        if clean_ticker:
            membership_row = membership_lookup.get(clean_ticker)
        if membership_row is None and clean_ric:
            membership_row = membership_lookup.get(clean_ric)
        if membership_row is None:
            # Membership exists for the core set but this ticker has no row —
            # it is outside the modelled universe (e.g. an ETF price ticker that
            # was never admitted by security_registry). Drop it from the serving
            # payload so the universe stays consistent with what was modelled.
            dropped.append(clean_ticker or clean_ric or ticker)
            updated = True
            continue

        matched += 1
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

    _assert_membership_coverage_not_partial(
        universe_payload=universe_payload,
        matched=matched,
        missing=dropped,
    )

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
    membership_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    universe_payload = dict(persisted_payloads.get("universe_loadings") or {})
    if not universe_payload:
        return persisted_payloads
    updated_universe_payload, universe_updated = _apply_current_membership_to_universe_payload(
        data_db=data_db,
        universe_payload=universe_payload,
        membership_rows=membership_rows,
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
    completed_at = datetime.now(timezone.utc).isoformat()
    cuse_membership_payload, cuse_stage_results_payload = build_cuse_membership_payloads(
        data_db=data_db,
        universe_payload=dict((persisted_payloads or {}).get("universe_loadings") or {}),
        risk_engine_state=risk_engine_state,
        run_id=run_id,
        updated_at=completed_at,
    )
    current_membership_rows = _membership_rows_from_payload(cuse_membership_payload)
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
                completed_at=completed_at,
                source_dates=source_dates,
                params=params,
                risk_engine_state=risk_engine_state,
                cov=cov,
                specific_risk_by_ticker=specific_risk_by_security,
                persisted_payloads=persisted_payloads,
                cuse_membership_payload=cuse_membership_payload,
                cuse_stage_results_payload=cuse_stage_results_payload,
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
        membership_rows=current_membership_rows,
    )
    candidate_universe_payload = persisted_payloads.get("universe_loadings")
    if isinstance(candidate_universe_payload, dict) and candidate_universe_payload:
        live_universe_payload = serving_outputs.load_current_payload("universe_loadings")
        regression_ok, regression_reason = reuse_policy.universe_loadings_live_regression_guard(
            candidate_universe_payload,
            current_live_payload=live_universe_payload,
        )
        if not regression_ok:
            raise RuntimeError(
                "cUSE serving publish blocked because the candidate universe regressed versus the current live modeled snapshot: "
                f"{regression_reason}"
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
