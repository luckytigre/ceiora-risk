"""Cloud Run Job entrypoint for cPAR package-build execution."""

from __future__ import annotations

import os
import sys
import uuid

from backend.orchestration.run_cpar_pipeline import run_cpar_pipeline


def main() -> int:
    profile = str(os.getenv("CPAR_PROFILE", "cpar-weekly")).strip() or "cpar-weekly"
    as_of_date = str(os.getenv("CPAR_AS_OF_DATE", "")).strip() or None
    pipeline_run_id = str(os.getenv("CPAR_PIPELINE_RUN_ID", "")).strip() or f"cpar_job_{uuid.uuid4().hex[:12]}"

    result = run_cpar_pipeline(
        profile=profile,
        as_of_date=as_of_date,
        run_id=pipeline_run_id,
    )
    return 0 if result.get("status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
