"""Identifier normalization and resolution helpers for Neon holdings workflows."""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from backend.universe.registry_sync import policy_defaults_for_legacy_coverage_role

ACCOUNT_ID_RE = re.compile(r"^[a-z0-9_\-]{2,64}$")
QTY_SCALE = Decimal("0.000001")
IMPORT_MODES = {"replace_account", "upsert_absolute", "increment_delta"}


@dataclass(frozen=True)
class ResolvedImportRow:
    row_number: int
    account_id: str
    ric: str
    ticker: str | None
    quantity: Decimal
    source: str


_SUFFIX_RANK = {
    ".N": 0,
    ".OQ": 1,
    ".O": 2,
    ".K": 3,
    ".P": 4,
}


def _suffix_rank(ric: str) -> int:
    txt = str(ric or "").upper().strip()
    for suf, rank in _SUFFIX_RANK.items():
        if txt.endswith(suf):
            return int(rank)
    return 99


def normalize_account_id(value: str | None) -> str | None:
    if value is None:
        return None
    clean = str(value).strip().lower()
    if not clean:
        return None
    if not ACCOUNT_ID_RE.match(clean):
        return None
    return clean


def normalize_ric(value: str | None) -> str:
    return str(value or "").strip().upper()


def normalize_ticker(value: str | None) -> str:
    return str(value or "").strip().upper()


def parse_quantity(value: str | None) -> Decimal:
    raw = str(value or "").strip()
    if not raw:
        raise InvalidOperation("blank")
    parsed = Decimal(raw)
    return parsed.quantize(QTY_SCALE, rounding=ROUND_HALF_UP)


def _pg_table_exists(pg_conn, table: str) -> bool:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = %s
            LIMIT 1
            """,
            (str(table),),
        )
        return cur.fetchone() is not None


def _lookup_active_registry_ticker_for_ric(pg_conn, clean_ric: str):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT NULLIF(TRIM(reg.ticker), '') AS ticker
            FROM security_registry reg
            WHERE UPPER(reg.ric) = %s
              AND COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') = 'active'
            LIMIT 1
            """,
            (clean_ric,),
        )
        return cur.fetchone()


def _lookup_compat_ticker_for_ric(pg_conn, clean_ric: str):
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT ticker FROM security_master_compat_current WHERE UPPER(ric) = %s LIMIT 1",
            (clean_ric,),
        )
        return cur.fetchone()


def _fetch_registry_ticker_resolution_rows(pg_conn, ticker_norm: str):
    has_policy = _pg_table_exists(pg_conn, "security_policy_current")
    has_taxonomy = _pg_table_exists(pg_conn, "security_taxonomy_current")
    policy_join = (
        """
            LEFT JOIN security_policy_current pol
              ON pol.ric = reg.ric
        """
        if has_policy
        else ""
    )
    taxonomy_join = (
        """
            LEFT JOIN security_taxonomy_current tax
              ON tax.ric = reg.ric
        """
        if has_taxonomy
        else ""
    )
    taxonomy_core_fallback = (
        "CASE WHEN COALESCE(tax.is_single_name_equity, 0) = 1 "
        "AND COALESCE(tax.classification_ready, 0) = 1 THEN 1 ELSE 0 END"
    )
    if has_policy and has_taxonomy:
        allow_cuse_native_core = f"COALESCE(pol.allow_cuse_native_core, {taxonomy_core_fallback})"
        allow_cpar_core_target = f"COALESCE(pol.allow_cpar_core_target, {taxonomy_core_fallback})"
        allow_cuse_returns_projection = "COALESCE(pol.allow_cuse_returns_projection, 0)"
    elif has_policy:
        allow_cuse_native_core = "COALESCE(pol.allow_cuse_native_core, 0)"
        allow_cpar_core_target = "COALESCE(pol.allow_cpar_core_target, 0)"
        allow_cuse_returns_projection = "COALESCE(pol.allow_cuse_returns_projection, 0)"
    elif has_taxonomy:
        allow_cuse_native_core = taxonomy_core_fallback
        allow_cpar_core_target = taxonomy_core_fallback
        allow_cuse_returns_projection = "0"
    else:
        allow_cuse_native_core = "0"
        allow_cpar_core_target = "0"
        allow_cuse_returns_projection = "0"
    is_single_name_equity = "COALESCE(tax.is_single_name_equity, 0)" if has_taxonomy else "0"
    classification_ready = "COALESCE(tax.classification_ready, 0)" if has_taxonomy else "0"
    with pg_conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                reg.ric,
                NULLIF(TRIM(reg.ticker), '') AS ticker,
                {allow_cuse_native_core} AS allow_cuse_native_core,
                {allow_cpar_core_target} AS allow_cpar_core_target,
                {allow_cuse_returns_projection} AS allow_cuse_returns_projection,
                {is_single_name_equity} AS is_single_name_equity,
                {classification_ready} AS classification_ready
            FROM security_registry reg
            {policy_join}
            {taxonomy_join}
            WHERE UPPER(NULLIF(TRIM(reg.ticker), '')) = %s
              AND COALESCE(NULLIF(TRIM(reg.tracking_status), ''), 'active') = 'active'
            """,
            (ticker_norm,),
        )
        return cur.fetchall()


def _fetch_compat_ticker_resolution_rows(pg_conn, ticker_norm: str):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                ric,
                ticker,
                COALESCE(NULLIF(TRIM(coverage_role), ''), 'native_equity') AS coverage_role,
                CASE WHEN COALESCE(is_equity_eligible, 0) = 1 THEN 1 ELSE 0 END AS is_single_name_equity,
                COALESCE(classification_ok, 0) AS classification_ready
            FROM security_master_compat_current
            WHERE UPPER(ticker) = %s
            """,
            (ticker_norm,),
        )
        return cur.fetchall()


def ric_exists(pg_conn, ric: str) -> tuple[bool, str | None]:
    clean_ric = normalize_ric(ric)
    registry_available = _pg_table_exists(pg_conn, "security_registry")
    if registry_available:
        row = _lookup_active_registry_ticker_for_ric(pg_conn, clean_ric)
        if row:
            return True, (str(row[0]).upper().strip() if row[0] is not None else None)
        return False, None
    compat_available = _pg_table_exists(pg_conn, "security_master_compat_current")
    row = _lookup_compat_ticker_for_ric(pg_conn, clean_ric) if compat_available else None
    if not row:
        return False, None
    return True, (str(row[0]).upper().strip() if row[0] is not None else None)


def resolve_ticker_to_ric_internal(pg_conn, ticker: str) -> tuple[str | None, list[str]]:
    ticker_norm = normalize_ticker(ticker)
    registry_available = _pg_table_exists(pg_conn, "security_registry")
    if registry_available:
        rows = _fetch_registry_ticker_resolution_rows(pg_conn, ticker_norm)
    else:
        compat_available = _pg_table_exists(pg_conn, "security_master_compat_current")
        rows = _fetch_compat_ticker_resolution_rows(pg_conn, ticker_norm) if compat_available else []
    if not rows:
        return None, []

    candidates: list[dict[str, object]] = []
    for row in rows:
        if registry_available:
            (
                ric,
                tkr,
                allow_cuse_native_core,
                allow_cpar_core_target,
                allow_cuse_returns_projection,
                is_single_name_equity,
                classification_ready,
            ) = row
        else:
            ric, tkr, coverage_role, is_single_name_equity, classification_ready = row
            legacy_policy = policy_defaults_for_legacy_coverage_role(coverage_role)
            allow_cuse_native_core = legacy_policy["allow_cuse_native_core"]
            allow_cpar_core_target = legacy_policy["allow_cpar_core_target"]
            allow_cuse_returns_projection = legacy_policy["allow_cuse_returns_projection"]
        ric_txt = normalize_ric(ric)
        tkr_txt = normalize_ticker(tkr)
        native_core_rank = 0 if (
            int(allow_cuse_native_core or 0) == 1
            and int(is_single_name_equity or 0) == 1
            and int(classification_ready or 0) == 1
        ) else 1
        cpar_core_rank = 0 if int(allow_cpar_core_target or 0) == 1 else 1
        returns_projection_rank = 0 if int(allow_cuse_returns_projection or 0) == 1 else 1
        candidates.append(
            {
                "ric": ric_txt,
                "ticker": tkr_txt,
                "native_core_rank": native_core_rank,
                "cpar_core_rank": cpar_core_rank,
                "returns_projection_rank": returns_projection_rank,
                "suffix_rank": _suffix_rank(ric_txt),
            }
        )

    candidates.sort(
        key=lambda x: (
            int(x["native_core_rank"]),
            int(x["cpar_core_rank"]),
            int(x["returns_projection_rank"]),
            int(x["suffix_rank"]),
            str(x["ric"]),
        )
    )
    selected = str(candidates[0]["ric"])
    alternatives = [str(x["ric"]) for x in candidates[1:]]
    return selected, alternatives


def resolve_ticker_to_ric(pg_conn, ticker: str) -> tuple[str | None, list[str]]:
    """Public deterministic ticker->RIC resolver."""
    return resolve_ticker_to_ric_internal(pg_conn, normalize_ticker(ticker))
