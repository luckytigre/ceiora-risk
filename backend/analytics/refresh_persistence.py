"""Persistence coordinator for analytics refresh outputs."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend import config
from backend.data import model_outputs, runtime_state, serving_outputs, sqlite

logger = logging.getLogger(__name__)


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
