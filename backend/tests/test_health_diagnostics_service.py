from __future__ import annotations

import pytest

from backend.services import cuse4_health_diagnostics_service as service


def test_load_health_diagnostics_payload_returns_payload_for_ok_state() -> None:
    payload = service.load_health_diagnostics_payload(
        payload_state_loader=lambda *_args, **_kwargs: {
            "status": "ok",
            "source": "neon",
            "value": {"status": "ok", "notes": ["fresh"]},
        }
    )

    assert payload["status"] == "ok"
    assert payload["notes"] == ["fresh"]
    assert payload["_cached"] is True


def test_load_health_diagnostics_payload_raises_not_ready_for_missing_state() -> None:
    with pytest.raises(service.HealthDiagnosticsNotReady) as excinfo:
        service.load_health_diagnostics_payload(
            payload_state_loader=lambda *_args, **_kwargs: {
                "status": "missing",
                "source": "neon",
                "value": None,
            }
        )

    assert excinfo.value.cache_key == "health_diagnostics"


def test_load_health_diagnostics_payload_raises_unavailable_for_error_state() -> None:
    with pytest.raises(service.HealthDiagnosticsUnavailable) as excinfo:
        service.load_health_diagnostics_payload(
            payload_state_loader=lambda *_args, **_kwargs: {
                "status": "error",
                "source": "neon",
                "value": None,
                "error": {"type": "OperationalError", "message": "timed out"},
            }
        )

    assert excinfo.value.source == "neon"
    assert "OperationalError" in excinfo.value.message
