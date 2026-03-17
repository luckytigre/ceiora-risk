from __future__ import annotations

import pytest

from backend.data.neon_primary_write import execute_neon_primary_write


def test_execute_neon_primary_write_raises_before_fallback_when_neon_required() -> None:
    calls: list[str] = []

    with pytest.raises(RuntimeError, match="Neon serving payload persistence failed"):
        execute_neon_primary_write(
            base_result={"status": "ok"},
            neon_enabled=True,
            neon_required=True,
            perform_neon_write=lambda: calls.append("neon") or {
                "status": "error",
                "error": {"type": "RuntimeError", "message": "boom"},
            },
            perform_fallback_write=lambda: calls.append("sqlite") or {"status": "ok"},
            failure_label="serving payload persistence",
            fallback_result_key="sqlite_mirror_write",
        )

    assert calls == ["neon"]


def test_execute_neon_primary_write_uses_fallback_when_neon_optional() -> None:
    calls: list[str] = []

    out = execute_neon_primary_write(
        base_result={"status": "ok"},
        neon_enabled=True,
        neon_required=False,
        perform_neon_write=lambda: calls.append("neon") or {
            "status": "error",
            "error": {"type": "RuntimeError", "message": "boom"},
        },
        perform_fallback_write=lambda: calls.append("sqlite") or {"status": "ok"},
        failure_label="runtime-state persistence",
        fallback_result_key="fallback_write",
    )

    assert calls == ["neon", "sqlite"]
    assert out["authority_store"] == "sqlite"
    assert out["neon_write"]["status"] == "error"
    assert out["fallback_write"]["status"] == "ok"
