from __future__ import annotations

from pathlib import Path

import pytest

from backend.scripts import run_refresh_job


def test_validate_runtime_assets_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    missing = Path("/tmp/does-not-exist/NEON_CANONICAL_SCHEMA.sql")
    monkeypatch.setattr(run_refresh_job, "_required_runtime_asset_paths", lambda: [missing])
    with pytest.raises(FileNotFoundError, match="Missing required runtime schema assets"):
        run_refresh_job._validate_runtime_assets()


def test_validate_runtime_assets_passes_when_present(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    paths = []
    for name in (
        "NEON_CANONICAL_SCHEMA.sql",
        "NEON_CPAR_SCHEMA.sql",
        "NEON_HOLDINGS_SCHEMA.sql",
        "NEON_REGISTRY_FIRST_CLEANUP.sql",
    ):
        p = tmp_path / name
        p.write_text("-- ok\n", encoding="utf-8")
        paths.append(p)
    monkeypatch.setattr(run_refresh_job, "_required_runtime_asset_paths", lambda: paths)
    run_refresh_job._validate_runtime_assets()
