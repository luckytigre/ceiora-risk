from __future__ import annotations

import pytest

from backend.data import runtime_state


def test_load_runtime_state_prefers_neon_when_primary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runtime_state.config, "runtime_state_primary_reads_enabled", lambda: True)
    monkeypatch.setattr(runtime_state.config, "neon_surface_enabled", lambda surface: surface == "runtime_state")
    monkeypatch.setattr(
        runtime_state,
        "_read_neon_runtime_state",
        lambda key: {"status": "ok", "source": "neon", "value": {"key": key, "source": "neon"}},
    )

    out = runtime_state.load_runtime_state(
        "risk_engine_meta",
        fallback_loader=lambda key: {"key": key, "source": "sqlite"},
    )

    assert out == {"key": "risk_engine_meta", "source": "neon"}


def test_load_runtime_state_falls_back_when_neon_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runtime_state.config, "runtime_state_primary_reads_enabled", lambda: True)
    monkeypatch.setattr(runtime_state.config, "runtime_state_cache_fallback_enabled", lambda: True)
    monkeypatch.setattr(runtime_state.config, "neon_surface_enabled", lambda surface: surface == "runtime_state")
    monkeypatch.setattr(
        runtime_state,
        "_read_neon_runtime_state",
        lambda _key: {"status": "missing", "source": "neon", "value": None},
    )

    out = runtime_state.load_runtime_state(
        "neon_sync_health",
        fallback_loader=lambda key: {"key": key, "source": "sqlite"},
    )

    assert out == {"key": "neon_sync_health", "source": "sqlite"}


def test_persist_runtime_state_raises_before_fallback_when_neon_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(runtime_state.config, "neon_surface_enabled", lambda surface: surface == "runtime_state")
    monkeypatch.setattr(runtime_state.config, "runtime_state_neon_write_required", lambda: True)
    monkeypatch.setattr(
        runtime_state,
        "_write_neon_runtime_state",
        lambda key, value: calls.append(f"neon:{key}") or {
            "status": "error",
            "error": {"type": "RuntimeError", "message": "boom"},
        },
    )

    with pytest.raises(RuntimeError, match="Neon runtime-state persistence failed"):
        runtime_state.persist_runtime_state(
            "risk_engine_meta",
            {"status": "ok"},
            fallback_writer=lambda key, value: calls.append(f"sqlite:{key}"),
        )

    assert calls == ["neon:risk_engine_meta"]


def test_publish_active_snapshot_writes_neon_and_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(runtime_state.config, "neon_surface_enabled", lambda surface: surface == "runtime_state")
    monkeypatch.setattr(runtime_state.config, "runtime_state_neon_write_required", lambda: False)
    monkeypatch.setattr(
        runtime_state,
        "_write_neon_runtime_state",
        lambda key, value: calls.append(f"neon:{key}:{value['snapshot_id']}") or {"status": "ok"},
    )

    out = runtime_state.publish_active_snapshot(
        "snap_1",
        fallback_publisher=lambda snapshot_id: calls.append(f"sqlite:{snapshot_id}"),
    )

    assert out["status"] == "ok"
    assert out["authority_store"] == "neon"
    assert calls == [
        "neon:__cache_snapshot_active:snap_1",
        "sqlite:snap_1",
    ]


def test_runtime_state_rejects_unknown_keys() -> None:
    with pytest.raises(ValueError, match="unsupported runtime_state key"):
        runtime_state.load_runtime_state("unknown_key")


@pytest.mark.parametrize("state_key", ["refresh_status", "holdings_sync_state"])
def test_runtime_state_accepts_operator_state_keys(
    monkeypatch: pytest.MonkeyPatch,
    state_key: str,
) -> None:
    monkeypatch.setattr(runtime_state.config, "runtime_state_primary_reads_enabled", lambda: False)

    out = runtime_state.load_runtime_state(
        state_key,
        fallback_loader=lambda key: {"key": key, "source": "sqlite"},
    )

    assert out == {"key": state_key, "source": "sqlite"}
