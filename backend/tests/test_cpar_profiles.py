from __future__ import annotations

import pytest

from backend.orchestration import cpar_profiles


def test_cpar_weekly_resolves_latest_completed_package_date_from_midweek_asof() -> None:
    out = cpar_profiles.resolve_package_date(profile="cpar-weekly", as_of_date="2026-03-18")

    assert out == "2026-03-13"


def test_cpar_package_date_requires_explicit_anchor_date() -> None:
    with pytest.raises(ValueError, match="explicit XNYS weekly package date"):
        cpar_profiles.resolve_package_date(profile="cpar-package-date", as_of_date="2026-03-18")

    assert cpar_profiles.resolve_package_date(profile="cpar-package-date", as_of_date="2026-03-13") == "2026-03-13"


def test_cpar_stage_window_can_end_early_but_must_start_at_source_read() -> None:
    profile_key, _cfg, selected = cpar_profiles.planned_stages_for_profile(
        profile="cpar-weekly",
        to_stage="package_build",
    )

    assert profile_key == "cpar-weekly"
    assert selected == ["source_read", "package_build"]

    with pytest.raises(ValueError, match="must start at source_read"):
        cpar_profiles.planned_stages_for_profile(
            profile="cpar-weekly",
            from_stage="package_build",
            to_stage="persist_package",
        )
