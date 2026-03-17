"""Identifier normalization and resolution helpers for Neon holdings workflows."""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP


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


def ric_exists(pg_conn, ric: str) -> tuple[bool, str | None]:
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT ticker FROM security_master WHERE ric = %s LIMIT 1",
            (ric,),
        )
        row = cur.fetchone()
        if not row:
            return False, None
        return True, (str(row[0]).upper().strip() if row[0] is not None else None)


def resolve_ticker_to_ric_internal(pg_conn, ticker: str) -> tuple[str | None, list[str]]:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                ric,
                ticker,
                COALESCE(classification_ok, 0) AS classification_ok,
                COALESCE(is_equity_eligible, 0) AS is_equity_eligible
            FROM security_master
            WHERE UPPER(ticker) = %s
            """,
            (ticker,),
        )
        rows = cur.fetchall()
    if not rows:
        return None, []

    candidates: list[dict[str, object]] = []
    for ric, tkr, c_ok, e_ok in rows:
        ric_txt = normalize_ric(ric)
        tkr_txt = normalize_ticker(tkr)
        eligible = 0 if (int(c_ok or 0) == 1 and int(e_ok or 0) == 1) else 1
        candidates.append(
            {
                "ric": ric_txt,
                "ticker": tkr_txt,
                "eligible_rank": eligible,
                "suffix_rank": _suffix_rank(ric_txt),
            }
        )

    candidates.sort(
        key=lambda x: (
            int(x["eligible_rank"]),
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
