from __future__ import annotations

import pytest

from backend import config


@pytest.fixture(autouse=True)
def _disable_live_neon_model_output_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default tests to local-only durable model writes.

    Tests that intentionally exercise Neon behavior can still override these
    functions with explicit monkeypatches and mocked connections/writers.
    """

    monkeypatch.setattr(config, "neon_dsn", lambda: "")
    monkeypatch.setattr(config, "neon_primary_model_data_enabled", lambda: False)
