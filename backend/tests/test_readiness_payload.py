from __future__ import annotations

from backend.api.routes.readiness import cache_not_ready_payload


def test_cache_not_ready_payload_uses_canonical_profile_endpoint() -> None:
    payload = cache_not_ready_payload(
        cache_key="portfolio",
        message="Portfolio cache is not ready.",
    )

    assert payload["status"] == "not_ready"
    assert payload["action"]["method"] == "POST"
    assert payload["action"]["endpoint"] == "/api/refresh?profile=serve-refresh"
