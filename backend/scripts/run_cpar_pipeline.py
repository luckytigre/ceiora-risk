#!/usr/bin/env python3
"""CLI wrapper for the dedicated cPAR package-build pipeline."""

from __future__ import annotations

from backend.orchestration.run_cpar_pipeline import main


if __name__ == "__main__":
    raise SystemExit(main())
