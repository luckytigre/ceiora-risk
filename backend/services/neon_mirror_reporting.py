"""Neon mirror artifact, repair, and health publication helpers."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend import config
from backend.data import runtime_state, sqlite

logger = logging.getLogger(__name__)


def persist_runtime_health_payload(state_key: str, payload: dict[str, Any]) -> dict[str, Any]:
    try:
        return runtime_state.persist_runtime_state(
            state_key,
            payload,
            fallback_writer=lambda key, value: sqlite.cache_set(key, value),
        )
    except Exception:  # noqa: BLE001
        logger.exception(
            "Runtime-state publish failed for %s; writing local fallback health payload instead.",
            state_key,
        )
        sqlite.cache_set(state_key, payload)
        return {
            "status": "warning",
            "source": "sqlite_fallback_only",
        }


def extract_neon_mirror_error(neon_mirror: dict[str, Any] | None) -> dict[str, str] | None:
    if not isinstance(neon_mirror, dict):
        return None
    candidates = [
        neon_mirror.get("error"),
        (neon_mirror.get("sync") or {}).get("error"),
        (neon_mirror.get("factor_returns_sync") or {}).get("error"),
        (neon_mirror.get("prune") or {}).get("error"),
        (neon_mirror.get("parity") or {}).get("error"),
    ]
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        error_type = str(candidate.get("type") or "").strip()
        error_message = str(candidate.get("message") or "").strip()
        if error_type or error_message:
            return {
                "type": error_type or "RuntimeError",
                "message": error_message or "Neon mirror step failed.",
            }
    return None


def write_neon_mirror_artifact(
    *,
    run_id: str,
    profile: str,
    as_of_date: str,
    overall_status: str,
    neon_mirror: dict[str, Any],
) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    reports_dir = Path(config.APP_DATA_DIR) / "audit_reports" / "neon_parity"
    reports_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": str(run_id),
        "profile": str(profile),
        "as_of_date": str(as_of_date),
        "overall_status": str(overall_status),
        "neon_mirror": neon_mirror,
    }
    artifact_path = reports_dir / f"neon_mirror_{stamp}_{run_id}.json"
    artifact_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    latest_path = reports_dir / "latest_neon_mirror_report.json"
    latest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return str(artifact_path)


def publish_neon_sync_health(
    *,
    run_id: str,
    profile: str,
    as_of_date: str,
    neon_mirror: dict[str, Any],
    artifact_path: str | None,
) -> None:
    mirror_status = str(neon_mirror.get("status") or "").strip().lower()
    sync_status = str((neon_mirror.get("sync") or {}).get("status") or "").strip().lower()
    parity = neon_mirror.get("parity") if isinstance(neon_mirror.get("parity"), dict) else {}
    parity_status = str((parity or {}).get("status") or "").strip().lower()
    parity_issues = list((parity or {}).get("issues") or [])
    error_details = extract_neon_mirror_error(neon_mirror)

    has_error = (
        mirror_status in {"failed", "mismatch"}
        or sync_status in {"failed", "mismatch"}
        or parity_status in {"failed", "mismatch"}
    )
    status = "error" if has_error else "ok"
    message = (
        f"Neon mirror={mirror_status or 'unknown'} sync={sync_status or 'n/a'} "
        f"parity={parity_status or 'n/a'}"
    )
    if not has_error and mirror_status in {"", "unknown", "skipped"}:
        status = "warning"

    payload = {
        "status": status,
        "message": message,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": str(run_id),
        "profile": str(profile),
        "as_of_date": str(as_of_date),
        "mirror_status": mirror_status or None,
        "sync_status": sync_status or None,
        "parity_status": parity_status or None,
        "parity_issue_count": int(len(parity_issues)),
        "parity_issue_examples": [str(x) for x in parity_issues[:10]],
        "error_type": (error_details or {}).get("type"),
        "error_message": (error_details or {}).get("message"),
        "artifact_path": str(artifact_path) if artifact_path else None,
    }
    persist_runtime_health_payload("neon_sync_health", payload)

    if status == "error":
        logger.error("Neon sync/parity health ERROR: %s", message)
    elif status == "warning":
        logger.warning("Neon sync/parity health WARNING: %s", message)
    else:
        logger.info("Neon sync/parity health OK: %s", message)
