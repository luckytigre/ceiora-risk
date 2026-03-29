"""Post-run publication and reporting helpers for the model pipeline."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend import config
from backend.services import neon_mirror as neon_mirror_service
from backend.services import neon_mirror_reporting as neon_mirror_reporting_service

logger = logging.getLogger(__name__)


def extract_neon_mirror_error(neon_mirror: dict[str, Any] | None) -> dict[str, str] | None:
    return neon_mirror_reporting_service.extract_neon_mirror_error(neon_mirror)


def write_neon_mirror_artifact(
    *,
    run_id: str,
    profile: str,
    as_of_date: str,
    overall_status: str,
    neon_mirror: dict[str, Any],
) -> str:
    return neon_mirror_reporting_service.write_neon_mirror_artifact(
        run_id=run_id,
        profile=profile,
        as_of_date=as_of_date,
        overall_status=overall_status,
        neon_mirror=neon_mirror,
    )


def latest_neon_mirror_artifact_for_run(*, run_id: str) -> Path | None:
    reports_dir = Path(config.APP_DATA_DIR) / "audit_reports" / "neon_parity"
    if not reports_dir.exists():
        return None
    candidates = sorted(
        reports_dir.glob(f"neon_mirror_*_{run_id}.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def repair_neon_sync_health_from_existing_workspace(
    *,
    run_id: str,
    profile: str,
    as_of_date: str,
    workspace_sqlite_path: Path,
    workspace_cache_path: Path | None = None,
    prior_artifact_path: Path | None = None,
    dsn: str | None = None,
    source_years: int | None = None,
    analytics_years: int | None = None,
) -> dict[str, Any]:
    artifact_path = prior_artifact_path or latest_neon_mirror_artifact_for_run(run_id=run_id)
    if artifact_path is None:
        raise FileNotFoundError(f"no prior Neon mirror artifact found for run_id={run_id}")
    artifact_payload = json.loads(Path(artifact_path).read_text(encoding="utf-8"))
    prior_mirror = artifact_payload.get("neon_mirror")
    if not isinstance(prior_mirror, dict):
        raise RuntimeError(f"artifact missing neon_mirror payload: {artifact_path}")

    parity = neon_mirror_service.run_bounded_parity_audit(
        sqlite_path=Path(workspace_sqlite_path),
        cache_path=(Path(workspace_cache_path) if workspace_cache_path is not None else None),
        dsn=dsn,
        source_years=int(source_years or config.NEON_SOURCE_RETENTION_YEARS),
        analytics_years=int(analytics_years or config.NEON_ANALYTICS_RETENTION_YEARS),
    )

    repaired_mirror = dict(prior_mirror)
    repaired_mirror["parity"] = parity
    sync_status = str((repaired_mirror.get("sync") or {}).get("status") or "").strip().lower()
    prune_status = str((repaired_mirror.get("prune") or {}).get("status") or "").strip().lower()
    parity_status = str((parity or {}).get("status") or "").strip().lower()
    repaired_mirror["status"] = (
        "ok"
        if sync_status == "ok" and parity_status == "ok" and prune_status not in {"failed", "mismatch"}
        else "mismatch"
    )

    new_artifact_path = write_neon_mirror_artifact(
        run_id=run_id,
        profile=profile,
        as_of_date=as_of_date,
        overall_status=("ok" if repaired_mirror["status"] == "ok" else "failed"),
        neon_mirror=repaired_mirror,
    )
    publish_neon_sync_health(
        run_id=run_id,
        profile=profile,
        as_of_date=as_of_date,
        neon_mirror=repaired_mirror,
        artifact_path=new_artifact_path,
    )
    return {
        "status": repaired_mirror["status"],
        "run_id": str(run_id),
        "profile": str(profile),
        "as_of_date": str(as_of_date),
        "prior_artifact_path": str(artifact_path),
        "artifact_path": str(new_artifact_path),
        "parity_status": parity_status or None,
        "parity_issue_count": int(len((parity or {}).get("issues") or [])),
        "parity_issue_examples": list((parity or {}).get("issues") or [])[:10],
    }


def publish_neon_sync_health(
    *,
    run_id: str,
    profile: str,
    as_of_date: str,
    neon_mirror: dict[str, Any],
    artifact_path: str | None,
) -> None:
    neon_mirror_reporting_service.publish_neon_sync_health(
        run_id=run_id,
        profile=profile,
        as_of_date=as_of_date,
        neon_mirror=neon_mirror,
        artifact_path=artifact_path,
    )


def extract_serving_payload_neon_failure(stage_results: list[dict[str, Any]]) -> dict[str, str] | None:
    for item in stage_results:
        if str(item.get("stage") or "") != "serving_refresh":
            continue
        if str(item.get("status") or "") != "failed":
            continue
        error = item.get("error") if isinstance(item.get("error"), dict) else {}
        message = str((error or {}).get("message") or "").strip()
        if "Serving payload" not in message or "Neon" not in message:
            continue
        return {
            "type": str((error or {}).get("type") or "RuntimeError"),
            "message": message,
        }
    return None


def publish_neon_serving_write_health(
    *,
    run_id: str,
    profile: str,
    as_of_date: str,
    error: dict[str, str],
) -> None:
    message = "Required Neon serving-payload publish failed"
    payload = {
        "status": "error",
        "message": message,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": str(run_id),
        "profile": str(profile),
        "as_of_date": str(as_of_date),
        "mirror_status": "skipped",
        "sync_status": "serving_payload_write_failed",
        "parity_status": None,
        "parity_issue_count": 0,
        "parity_issue_examples": [],
        "error_type": str(error.get("type") or "RuntimeError"),
        "error_message": str(error.get("message") or message),
        "artifact_path": None,
        "health_scope": "serving_payload_write",
    }
    neon_mirror_reporting_service.persist_runtime_health_payload("neon_sync_health", payload)
    logger.error("Neon serving payload health ERROR: %s", payload["error_message"])
