"""Control-plane service for dispatching cPAR package builds."""

from __future__ import annotations

import uuid
from typing import Any

from backend import config
from backend.ops import cloud_run_jobs
from backend.orchestration import cpar_profiles


def dispatch_cpar_build(
    *,
    profile: str,
    as_of_date: str | None = None,
) -> tuple[bool, dict[str, Any]]:
    profile_key = cpar_profiles.resolve_profile_name(profile)
    package_date = cpar_profiles.resolve_package_date(
        profile=profile_key,
        as_of_date=(str(as_of_date).strip() if as_of_date else None),
    )

    if not config.cpar_build_cloud_job_configured():
        return False, {
            "status": "unavailable",
            "error": {
                "type": "cloud_run_job_unconfigured",
                "message": (
                    "cPAR build dispatch is unavailable because the Cloud Run Job "
                    "environment contract is incomplete."
                ),
            },
        }

    pipeline_run_id = f"cpar_crj_{uuid.uuid4().hex[:12]}"
    try:
        dispatch = cloud_run_jobs.dispatch_cpar_build(
            pipeline_run_id=pipeline_run_id,
            profile=profile_key,
            as_of_date=package_date,
        )
    except Exception as exc:
        return False, {
            "status": "failed",
            "pipeline_run_id": pipeline_run_id,
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }

    return True, {
        "status": "dispatched",
        "pipeline_run_id": pipeline_run_id,
        "profile": profile_key,
        "package_date": package_date,
        "execution_name": dispatch.get("execution_name"),
        "dispatch_backend": "cloud_run_job",
    }
