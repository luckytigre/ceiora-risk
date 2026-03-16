"""Small shared helper for Neon-primary writes with optional local mirrors."""

from __future__ import annotations

from typing import Any, Callable


def execute_neon_primary_write(
    *,
    base_result: dict[str, Any],
    neon_enabled: bool,
    neon_required: bool,
    perform_neon_write: Callable[[], dict[str, Any]],
    perform_fallback_write: Callable[[], dict[str, Any]] | None,
    failure_label: str,
    fallback_result_key: str,
    fallback_authority: str = "sqlite",
    fallback_skipped_reason_when_neon: str = "neon_primary",
    fallback_skipped_reason_when_no_neon: str = "no_fallback_writer",
) -> dict[str, Any]:
    result = dict(base_result)
    result["authority_store"] = fallback_authority
    result["neon_write"] = {"status": "skipped", "reason": "neon_not_configured"}
    result[fallback_result_key] = {
        "status": "skipped",
        "reason": fallback_skipped_reason_when_neon if neon_enabled else fallback_skipped_reason_when_no_neon,
    }

    if neon_enabled:
        result["authority_store"] = "neon"
        result["neon_write"] = perform_neon_write()
        if str(result["neon_write"].get("status") or "") != "ok" and neon_required:
            err = result["neon_write"].get("error") if isinstance(result["neon_write"], dict) else None
            raise RuntimeError(
                f"Neon {failure_label} failed: "
                + str((err or {}).get("type") or "unknown")
                + ": "
                + str((err or {}).get("message") or "unknown")
            )
        if perform_fallback_write is not None:
            result[fallback_result_key] = perform_fallback_write()
        if str(result["neon_write"].get("status") or "") != "ok":
            result["authority_store"] = (
                fallback_authority
                if str((result.get(fallback_result_key) or {}).get("status") or "") == "ok"
                else "neon"
            )
        return result

    if perform_fallback_write is not None:
        result[fallback_result_key] = perform_fallback_write()
    return result
