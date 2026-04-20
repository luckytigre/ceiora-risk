from __future__ import annotations

from backend.api.routes.readiness import authority_unavailable_payload
from backend.api.routes.readiness import cache_not_ready_payload


def test_cache_not_ready_payload_uses_canonical_profile_endpoint() -> None:
    payload = cache_not_ready_payload(
        cache_key="portfolio",
        message="Portfolio cache is not ready.",
    )

    assert payload["status"] == "not_ready"
    assert payload["action"]["method"] == "POST"
    assert payload["action"]["endpoint"] == "/api/refresh?profile=serve-refresh"


def test_authority_unavailable_payload_surfaces_error_and_source() -> None:
    payload = authority_unavailable_payload(
        error="health_diagnostics_authority_unavailable",
        message="Health diagnostics authority is unavailable from neon.",
        source="neon",
    )

    assert payload["status"] == "unavailable"
    assert payload["error"] == "health_diagnostics_authority_unavailable"
    assert payload["source"] == "neon"
