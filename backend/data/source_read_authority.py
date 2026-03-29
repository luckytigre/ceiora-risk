"""Lower authority helpers for registry-first source reads."""

from __future__ import annotations

from typing import Any, Callable

def prefer_runtime_registry(
    *,
    missing_tables_fn: Callable[..., list[str]],
    fetch_rows_fn: Callable[[str, list[Any] | None], list[dict[str, Any]]] | None = None,
    tickers: list[str] | None = None,
    require_taxonomy: bool,
) -> bool:
    required_tables = ["security_registry", "security_policy_current"]
    if require_taxonomy:
        required_tables.append("security_taxonomy_current")
    if missing_tables_fn(*required_tables):
        return False
    if fetch_rows_fn is None:
        return False
    clean = [t.upper() for t in (tickers or []) if t.strip()]
    ticker_filter = ""
    params: list[Any] | None = None
    if clean:
        placeholders = ",".join("?" for _ in clean)
        ticker_filter = f" AND UPPER(TRIM(COALESCE(reg.ticker, ''))) IN ({placeholders})"
        params = list(clean)
    taxonomy_join = ""
    missing_companion_expr = "pol.ric IS NULL"
    if require_taxonomy:
        taxonomy_join = """
        LEFT JOIN security_taxonomy_current tax
          ON UPPER(TRIM(tax.ric)) = UPPER(TRIM(reg.ric))
        """
        missing_companion_expr = "pol.ric IS NULL OR tax.ric IS NULL"
    try:
        rows = fetch_rows_fn(
            f"""
            SELECT
                COUNT(*) AS registry_row_count,
                SUM(
                    CASE
                        WHEN COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') = 'active'
                        THEN 1
                        ELSE 0
                    END
                ) AS active_registry_row_count,
                SUM(
                    CASE
                        WHEN COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') = 'active'
                         AND ({missing_companion_expr})
                        THEN 1
                        ELSE 0
                    END
                ) AS active_missing_companion_count
            FROM security_registry reg
            LEFT JOIN security_policy_current pol
              ON UPPER(TRIM(pol.ric)) = UPPER(TRIM(reg.ric))
            {taxonomy_join}
            WHERE reg.ric IS NOT NULL
              AND TRIM(reg.ric) <> ''
              {ticker_filter}
            """,
            params,
        )
    except Exception:
        return False
    if not rows:
        return False
    row = rows[0]
    registry_row_count = int(row.get("registry_row_count") or 0)
    active_registry_row_count = int(row.get("active_registry_row_count") or 0)
    active_missing_companion_count = int(row.get("active_missing_companion_count") or 0)
    if registry_row_count == 0:
        return not clean
    if active_registry_row_count == 0:
        return not clean
    return active_missing_companion_count == 0
