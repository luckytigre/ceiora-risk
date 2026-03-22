#!/usr/bin/env python3
"""Recompute Neon parity for an existing successful workspace and republish health."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from backend import config
from backend.orchestration import post_run_publish


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True, help="Existing orchestrator run id to repair.")
    parser.add_argument("--profile", default="core-weekly", help="Profile label for the repaired health payload.")
    parser.add_argument("--as-of-date", required=True, help="As-of date for the repaired health payload.")
    parser.add_argument(
        "--workspace-dir",
        type=Path,
        default=None,
        help="Workspace directory containing data.db/cache.db. Defaults to backend/runtime/neon_rebuild_workspace/<run-id>.",
    )
    parser.add_argument(
        "--artifact-path",
        type=Path,
        default=None,
        help="Prior Neon mirror artifact to reuse for sync/prune state. Defaults to latest artifact for the run id.",
    )
    parser.add_argument("--dsn", default=None, help="Neon DSN override. Defaults to NEON_DATABASE_URL.")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON output.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    workspace_dir = (
        Path(args.workspace_dir)
        if args.workspace_dir is not None
        else Path(config.APP_DATA_DIR) / "neon_rebuild_workspace" / str(args.run_id)
    )
    out = post_run_publish.repair_neon_sync_health_from_existing_workspace(
        run_id=str(args.run_id),
        profile=str(args.profile),
        as_of_date=str(args.as_of_date),
        workspace_sqlite_path=workspace_dir / "data.db",
        workspace_cache_path=workspace_dir / "cache.db",
        prior_artifact_path=(Path(args.artifact_path) if args.artifact_path is not None else None),
        dsn=args.dsn,
    )

    if args.json:
        print(json.dumps(out, indent=2))
    else:
        print(f"status: {out['status']}")
        print(f"artifact_path: {out['artifact_path']}")
        print(f"parity_status: {out['parity_status']}")
        print(f"parity_issue_count: {out['parity_issue_count']}")
        for issue in out["parity_issue_examples"]:
            print(f"- {issue}")
    return 0 if str(out.get("status")) == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
