#!/usr/bin/env python3
"""CLI wrapper for profile-driven model pipeline orchestration."""

from __future__ import annotations



from backend.orchestration.run_model_pipeline import _parse_args, run_model_pipeline


if __name__ == "__main__":
    args = _parse_args()
    print(
        run_model_pipeline(
            profile=args.profile,
            as_of_date=args.as_of_date,
            run_id=args.run_id,
            resume_run_id=args.resume_run_id,
            from_stage=args.from_stage,
            to_stage=args.to_stage,
            force_core=bool(args.force_core),
        )
    )
