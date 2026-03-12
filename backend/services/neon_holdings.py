"""Neon holdings schema + CSV import behaviors."""

from __future__ import annotations

import csv
import re
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from backend.services.neon_stage2 import apply_sql_file


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


def _normalize_account_id(value: str | None) -> str | None:
    if value is None:
        return None
    clean = str(value).strip().lower()
    if not clean:
        return None
    if not ACCOUNT_ID_RE.match(clean):
        return None
    return clean


def _normalize_ric(value: str | None) -> str:
    return str(value or "").strip().upper()


def _normalize_ticker(value: str | None) -> str:
    return str(value or "").strip().upper()


def _parse_quantity(value: str | None) -> Decimal:
    raw = str(value or "").strip()
    if not raw:
        raise InvalidOperation("blank")
    parsed = Decimal(raw)
    return parsed.quantize(QTY_SCALE, rounding=ROUND_HALF_UP)


def ensure_holdings_schema(pg_conn, *, schema_sql_path: Path) -> dict[str, Any]:
    return apply_sql_file(pg_conn, sql_path=schema_sql_path)


def _ric_exists(pg_conn, ric: str) -> tuple[bool, str | None]:
    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT ticker FROM security_master WHERE ric = %s LIMIT 1",
            (ric,),
        )
        row = cur.fetchone()
        if not row:
            return False, None
        return True, (str(row[0]).upper().strip() if row[0] is not None else None)


def _resolve_ticker_to_ric(pg_conn, ticker: str) -> tuple[str | None, list[str]]:
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

    candidates: list[dict[str, Any]] = []
    for ric, tkr, c_ok, e_ok in rows:
        ric_txt = _normalize_ric(ric)
        tkr_txt = _normalize_ticker(tkr)
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
    return _resolve_ticker_to_ric(pg_conn, _normalize_ticker(ticker))


def build_rows_from_ticker_quantities(
    pg_conn,
    *,
    account_id: str,
    ticker_to_qty: dict[str, float],
    source: str,
) -> dict[str, Any]:
    accepted: list[ResolvedImportRow] = []
    rejected: list[dict[str, Any]] = []
    warnings: list[str] = []
    seen: set[tuple[str, str]] = set()
    acct = _normalize_account_id(account_id)
    if acct is None:
        raise ValueError("invalid account_id")

    for idx, (ticker_raw, qty_raw) in enumerate(sorted(ticker_to_qty.items()), start=1):
        ticker = _normalize_ticker(ticker_raw)
        if not ticker:
            rejected.append(
                {
                    "row_number": idx,
                    "reason_code": "unknown_ticker",
                    "message": f"invalid ticker: {ticker_raw!r}",
                }
            )
            continue
        try:
            qty = _parse_quantity(str(qty_raw))
        except (InvalidOperation, ValueError):
            rejected.append(
                {
                    "row_number": idx,
                    "reason_code": "invalid_quantity",
                    "message": f"invalid quantity for {ticker}: {qty_raw!r}",
                }
            )
            continue

        ric, alternatives = _resolve_ticker_to_ric(pg_conn, ticker)
        if not ric:
            rejected.append(
                {
                    "row_number": idx,
                    "reason_code": "unknown_ticker",
                    "message": f"ticker not found in security_master: {ticker}",
                }
            )
            continue
        if alternatives:
            warnings.append(
                f"ticker {ticker} resolved to {ric}; alternatives={','.join(alternatives[:10])}"
            )
        key = (acct, ric)
        if key in seen:
            rejected.append(
                {
                    "row_number": idx,
                    "reason_code": "duplicate_row_in_file",
                    "message": f"duplicate account_id+ric: {acct}/{ric}",
                }
            )
            continue
        seen.add(key)
        accepted.append(
            ResolvedImportRow(
                row_number=idx,
                account_id=acct,
                ric=ric,
                ticker=ticker,
                quantity=qty,
                source=str(source or "seed_mock"),
            )
        )

    rejection_counts: dict[str, int] = {}
    for r in rejected:
        code = str(r.get("reason_code") or "unknown")
        rejection_counts[code] = int(rejection_counts.get(code, 0) + 1)
    return {
        "accepted": accepted,
        "rejected": rejected,
        "warnings": warnings,
        "rejection_counts": rejection_counts,
        "mode": "replace_account",
        "csv_path": "<mock_positions_store>",
    }


def _insert_batch(pg_conn, *, account_id: str, mode: str, filename: str | None, row_count: int, requested_by: str | None, notes: str | None) -> str:
    batch_id = str(uuid.uuid4())
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO holdings_import_batches (
                import_batch_id,
                account_id,
                mode,
                filename,
                row_count,
                requested_by,
                created_at,
                notes
            ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s)
            """,
            (batch_id, account_id, mode, filename, int(row_count), requested_by, notes),
        )
    return batch_id


def _ensure_account(pg_conn, *, account_id: str) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO holdings_accounts (
                account_id,
                account_name,
                is_active,
                created_at,
                updated_at
            ) VALUES (%s, %s, TRUE, NOW(), NOW())
            ON CONFLICT (account_id) DO UPDATE
            SET
                is_active = TRUE,
                updated_at = NOW()
            """,
            (account_id, account_id),
        )


def _load_current_positions(pg_conn, *, account_id: str) -> dict[str, dict[str, Any]]:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT ric, ticker, quantity
            FROM holdings_positions_current
            WHERE account_id = %s
            """,
            (account_id,),
        )
        rows = cur.fetchall()
    out: dict[str, dict[str, Any]] = {}
    for ric, ticker, qty in rows:
        out[_normalize_ric(ric)] = {
            "ticker": (_normalize_ticker(ticker) if ticker is not None else None),
            "quantity": Decimal(str(qty)).quantize(QTY_SCALE, rounding=ROUND_HALF_UP),
        }
    return out


def _upsert_position(
    pg_conn,
    *,
    account_id: str,
    ric: str,
    ticker: str | None,
    quantity: Decimal,
    source: str,
    import_batch_id: str,
) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO holdings_positions_current (
                account_id,
                ric,
                ticker,
                quantity,
                source,
                import_batch_id,
                updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (account_id, ric) DO UPDATE
            SET
                ticker = EXCLUDED.ticker,
                quantity = EXCLUDED.quantity,
                source = EXCLUDED.source,
                import_batch_id = EXCLUDED.import_batch_id,
                updated_at = NOW()
            """,
            (account_id, ric, ticker, quantity, source, import_batch_id),
        )


def _delete_position(pg_conn, *, account_id: str, ric: str) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM holdings_positions_current WHERE account_id = %s AND ric = %s",
            (account_id, ric),
        )


def _insert_event(
    pg_conn,
    *,
    import_batch_id: str,
    account_id: str,
    ric: str,
    ticker: str | None,
    event_type: str,
    quantity_before: Decimal | None,
    quantity_delta: Decimal | None,
    quantity_after: Decimal,
    created_by: str | None,
) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO holdings_position_events (
                import_batch_id,
                account_id,
                ric,
                ticker,
                event_type,
                quantity_before,
                quantity_delta,
                quantity_after,
                created_at,
                created_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
            """,
            (
                import_batch_id,
                account_id,
                ric,
                ticker,
                event_type,
                quantity_before,
                quantity_delta,
                quantity_after,
                created_by,
            ),
        )


def parse_holdings_csv(
    pg_conn,
    *,
    csv_path: Path,
    mode: str,
    default_account_id: str | None,
    default_source: str,
) -> dict[str, Any]:
    mode_norm = str(mode).strip()
    if mode_norm not in IMPORT_MODES:
        raise ValueError(f"invalid mode: {mode_norm}")

    path = Path(csv_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    accepted: list[ResolvedImportRow] = []
    rejected: list[dict[str, Any]] = []
    warnings: list[str] = []

    default_acct = _normalize_account_id(default_account_id)
    seen_keys: set[tuple[str, str]] = set()

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV has no header row")

        fields = {str(h).strip().lower(): h for h in reader.fieldnames if h is not None}
        quantity_col = fields.get("quantity")
        if not quantity_col:
            raise ValueError("CSV missing required column: quantity")
        account_col = fields.get("account_id")
        ric_col = fields.get("ric")
        ticker_col = fields.get("ticker")
        source_col = fields.get("source")

        for idx, row in enumerate(reader, start=2):
            account_raw = row.get(account_col) if account_col else None
            account_id = _normalize_account_id(account_raw) or default_acct
            if account_id is None:
                rejected.append(
                    {
                        "row_number": idx,
                        "reason_code": "invalid_account_id",
                        "message": "account_id missing or invalid",
                    }
                )
                continue

            try:
                qty = _parse_quantity(row.get(quantity_col))
            except (InvalidOperation, ValueError):
                rejected.append(
                    {
                        "row_number": idx,
                        "reason_code": "invalid_quantity",
                        "message": f"invalid quantity: {row.get(quantity_col)!r}",
                    }
                )
                continue

            ric = _normalize_ric(row.get(ric_col) if ric_col else None)
            ticker = _normalize_ticker(row.get(ticker_col) if ticker_col else None)

            resolved_ric = ""
            resolved_ticker: str | None = ticker or None
            if ric:
                ok, mapped_ticker = _ric_exists(pg_conn, ric)
                if not ok:
                    rejected.append(
                        {
                            "row_number": idx,
                            "reason_code": "unknown_ric",
                            "message": f"RIC not found in security_master: {ric}",
                        }
                    )
                    continue
                resolved_ric = ric
                resolved_ticker = mapped_ticker or resolved_ticker
            elif ticker:
                picked_ric, alternatives = _resolve_ticker_to_ric(pg_conn, ticker)
                if not picked_ric:
                    rejected.append(
                        {
                            "row_number": idx,
                            "reason_code": "unknown_ticker",
                            "message": f"Ticker not found in security_master: {ticker}",
                        }
                    )
                    continue
                resolved_ric = picked_ric
                if alternatives:
                    warnings.append(
                        f"row {idx}: ticker {ticker} resolved to {picked_ric}; alternatives={','.join(alternatives[:10])}"
                    )
            else:
                rejected.append(
                    {
                        "row_number": idx,
                        "reason_code": "missing_identifier",
                        "message": "need ric or ticker",
                    }
                )
                continue

            dup_key = (account_id, resolved_ric)
            if dup_key in seen_keys:
                rejected.append(
                    {
                        "row_number": idx,
                        "reason_code": "duplicate_row_in_file",
                        "message": f"duplicate account_id+ric in file: {account_id}/{resolved_ric}",
                    }
                )
                continue
            seen_keys.add(dup_key)

            row_source = str(row.get(source_col) or default_source).strip() if source_col else str(default_source)
            accepted.append(
                ResolvedImportRow(
                    row_number=idx,
                    account_id=account_id,
                    ric=resolved_ric,
                    ticker=resolved_ticker,
                    quantity=qty,
                    source=row_source or default_source,
                )
            )

    rejection_counts: dict[str, int] = {}
    for r in rejected:
        code = str(r.get("reason_code") or "unknown")
        rejection_counts[code] = int(rejection_counts.get(code, 0) + 1)

    return {
        "csv_path": str(path),
        "mode": mode_norm,
        "accepted": accepted,
        "rejected": rejected,
        "warnings": warnings,
        "rejection_counts": rejection_counts,
    }


def apply_holdings_import(
    pg_conn,
    *,
    parsed: dict[str, Any],
    mode: str,
    account_id: str,
    requested_by: str | None,
    filename: str | None,
    notes: str | None,
    dry_run: bool,
) -> dict[str, Any]:
    acct = _normalize_account_id(account_id)
    if acct is None:
        raise ValueError("invalid account_id")

    accepted_rows: list[ResolvedImportRow] = [r for r in parsed.get("accepted", []) if r.account_id == acct]
    rejected_rows = list(parsed.get("rejected", []))
    warnings = list(parsed.get("warnings", []))

    _ensure_account(pg_conn, account_id=acct)
    batch_id = _insert_batch(
        pg_conn,
        account_id=acct,
        mode=mode,
        filename=filename,
        row_count=len(accepted_rows),
        requested_by=requested_by,
        notes=notes,
    )

    existing = _load_current_positions(pg_conn, account_id=acct)
    accepted_map: dict[str, ResolvedImportRow] = {r.ric: r for r in accepted_rows}

    applied_upserts = 0
    applied_deletes = 0

    def _apply_set_absolute(ric: str, row: ResolvedImportRow | None, *, remove_if_missing: bool) -> None:
        nonlocal applied_upserts, applied_deletes
        before = existing.get(ric, {})
        before_qty = before.get("quantity")
        before_ticker = before.get("ticker")

        if row is None:
            if remove_if_missing and before_qty is not None:
                _delete_position(pg_conn, account_id=acct, ric=ric)
                _insert_event(
                    pg_conn,
                    import_batch_id=batch_id,
                    account_id=acct,
                    ric=ric,
                    ticker=(before_ticker or None),
                    event_type="remove_position",
                    quantity_before=before_qty,
                    quantity_delta=(Decimal("0") - before_qty),
                    quantity_after=Decimal("0"),
                    created_by=requested_by,
                )
                applied_deletes += 1
            return

        target_qty = row.quantity
        if target_qty == Decimal("0"):
            if before_qty is not None:
                _delete_position(pg_conn, account_id=acct, ric=ric)
                _insert_event(
                    pg_conn,
                    import_batch_id=batch_id,
                    account_id=acct,
                    ric=ric,
                    ticker=(row.ticker or before_ticker),
                    event_type="remove_position",
                    quantity_before=before_qty,
                    quantity_delta=(Decimal("0") - before_qty),
                    quantity_after=Decimal("0"),
                    created_by=requested_by,
                )
                applied_deletes += 1
            return

        _upsert_position(
            pg_conn,
            account_id=acct,
            ric=ric,
            ticker=row.ticker,
            quantity=target_qty,
            source=row.source,
            import_batch_id=batch_id,
        )
        qty_before = before_qty if before_qty is not None else Decimal("0")
        _insert_event(
            pg_conn,
            import_batch_id=batch_id,
            account_id=acct,
            ric=ric,
            ticker=row.ticker,
            event_type="set_absolute",
            quantity_before=before_qty,
            quantity_delta=(target_qty - qty_before),
            quantity_after=target_qty,
            created_by=requested_by,
        )
        applied_upserts += 1

    def _apply_increment(ric: str, row: ResolvedImportRow) -> None:
        nonlocal applied_upserts, applied_deletes
        before = existing.get(ric, {})
        before_qty = before.get("quantity")
        before_ticker = before.get("ticker")
        qty_before = before_qty if before_qty is not None else Decimal("0")
        qty_after = (qty_before + row.quantity).quantize(QTY_SCALE, rounding=ROUND_HALF_UP)

        if qty_after == Decimal("0"):
            if before_qty is not None:
                _delete_position(pg_conn, account_id=acct, ric=ric)
                _insert_event(
                    pg_conn,
                    import_batch_id=batch_id,
                    account_id=acct,
                    ric=ric,
                    ticker=(row.ticker or before_ticker),
                    event_type="remove_position",
                    quantity_before=before_qty,
                    quantity_delta=row.quantity,
                    quantity_after=Decimal("0"),
                    created_by=requested_by,
                )
                applied_deletes += 1
            return

        _upsert_position(
            pg_conn,
            account_id=acct,
            ric=ric,
            ticker=(row.ticker or before_ticker),
            quantity=qty_after,
            source=row.source,
            import_batch_id=batch_id,
        )
        _insert_event(
            pg_conn,
            import_batch_id=batch_id,
            account_id=acct,
            ric=ric,
            ticker=(row.ticker or before_ticker),
            event_type="increment_delta",
            quantity_before=before_qty,
            quantity_delta=row.quantity,
            quantity_after=qty_after,
            created_by=requested_by,
        )
        applied_upserts += 1

    if mode == "replace_account":
        all_rics = sorted(set(existing.keys()) | set(accepted_map.keys()))
        for ric in all_rics:
            _apply_set_absolute(ric, accepted_map.get(ric), remove_if_missing=True)
    elif mode == "upsert_absolute":
        for ric in sorted(accepted_map):
            _apply_set_absolute(ric, accepted_map[ric], remove_if_missing=False)
    elif mode == "increment_delta":
        for ric in sorted(accepted_map):
            _apply_increment(ric, accepted_map[ric])
    else:
        raise ValueError(f"invalid mode: {mode}")

    if dry_run:
        pg_conn.rollback()
        status = "dry_run"
    else:
        pg_conn.commit()
        status = "ok"

    return {
        "status": status,
        "mode": mode,
        "account_id": acct,
        "import_batch_id": batch_id,
        "accepted_rows": len(accepted_rows),
        "rejected_rows": len(rejected_rows),
        "rejection_counts": parsed.get("rejection_counts", {}),
        "warnings": warnings,
        "applied_upserts": int(applied_upserts),
        "applied_deletes": int(applied_deletes),
    }


def parse_holdings_rows(
    pg_conn,
    *,
    rows: list[dict[str, Any]],
    mode: str,
    default_account_id: str | None,
    default_source: str,
) -> dict[str, Any]:
    mode_norm = str(mode).strip()
    if mode_norm not in IMPORT_MODES:
        raise ValueError(f"invalid mode: {mode_norm}")

    accepted: list[ResolvedImportRow] = []
    rejected: list[dict[str, Any]] = []
    warnings: list[str] = []
    default_acct = _normalize_account_id(default_account_id)
    seen_keys: set[tuple[str, str]] = set()

    for idx, row in enumerate(list(rows or []), start=1):
        account_id = _normalize_account_id(row.get("account_id")) or default_acct
        if account_id is None:
            rejected.append(
                {
                    "row_number": idx,
                    "reason_code": "invalid_account_id",
                    "message": "account_id missing or invalid",
                }
            )
            continue

        try:
            qty = _parse_quantity(str(row.get("quantity")))
        except (InvalidOperation, ValueError):
            rejected.append(
                {
                    "row_number": idx,
                    "reason_code": "invalid_quantity",
                    "message": f"invalid quantity: {row.get('quantity')!r}",
                }
            )
            continue

        ric = _normalize_ric(row.get("ric"))
        ticker = _normalize_ticker(row.get("ticker"))
        resolved_ric = ""
        resolved_ticker: str | None = ticker or None
        if ric:
            ok, mapped_ticker = _ric_exists(pg_conn, ric)
            if not ok:
                rejected.append(
                    {
                        "row_number": idx,
                        "reason_code": "unknown_ric",
                        "message": f"RIC not found in security_master: {ric}",
                    }
                )
                continue
            resolved_ric = ric
            resolved_ticker = mapped_ticker or resolved_ticker
        elif ticker:
            picked_ric, alternatives = _resolve_ticker_to_ric(pg_conn, ticker)
            if not picked_ric:
                rejected.append(
                    {
                        "row_number": idx,
                        "reason_code": "unknown_ticker",
                        "message": f"Ticker not found in security_master: {ticker}",
                    }
                )
                continue
            resolved_ric = picked_ric
            if alternatives:
                warnings.append(
                    f"row {idx}: ticker {ticker} resolved to {picked_ric}; alternatives={','.join(alternatives[:10])}"
                )
        else:
            rejected.append(
                {
                    "row_number": idx,
                    "reason_code": "missing_identifier",
                    "message": "need ric or ticker",
                }
            )
            continue

        dup_key = (account_id, resolved_ric)
        if dup_key in seen_keys:
            rejected.append(
                {
                    "row_number": idx,
                    "reason_code": "duplicate_row_in_file",
                    "message": f"duplicate account_id+ric in payload: {account_id}/{resolved_ric}",
                }
            )
            continue
        seen_keys.add(dup_key)
        src = str(row.get("source") or default_source).strip() or str(default_source)
        accepted.append(
            ResolvedImportRow(
                row_number=idx,
                account_id=account_id,
                ric=resolved_ric,
                ticker=resolved_ticker,
                quantity=qty,
                source=src,
            )
        )

    rejection_counts: dict[str, int] = {}
    for r in rejected:
        code = str(r.get("reason_code") or "unknown")
        rejection_counts[code] = int(rejection_counts.get(code, 0) + 1)

    return {
        "mode": mode_norm,
        "accepted": accepted,
        "rejected": rejected,
        "warnings": warnings,
        "rejection_counts": rejection_counts,
        "csv_path": "<payload_rows>",
    }


def list_holdings_accounts(pg_conn) -> list[dict[str, Any]]:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                a.account_id,
                a.account_name,
                a.is_active,
                COUNT(p.ric) AS positions_count,
                COALESCE(SUM(ABS(CAST(p.quantity AS DOUBLE PRECISION))), 0) AS gross_quantity,
                MAX(p.updated_at) AS last_position_updated_at
            FROM holdings_accounts a
            LEFT JOIN holdings_positions_current p
              ON p.account_id = a.account_id
            GROUP BY a.account_id, a.account_name, a.is_active
            ORDER BY a.account_id ASC
            """
        )
        rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for account_id, account_name, is_active, positions_count, gross_qty, last_updated in rows:
        out.append(
            {
                "account_id": str(account_id),
                "account_name": str(account_name or account_id),
                "is_active": bool(is_active),
                "positions_count": int(positions_count or 0),
                "gross_quantity": float(gross_qty or 0.0),
                "last_position_updated_at": str(last_updated) if last_updated is not None else None,
            }
        )
    return out


def list_holdings_positions(pg_conn, *, account_id: str | None = None) -> list[dict[str, Any]]:
    acct = _normalize_account_id(account_id) if account_id is not None else None
    with pg_conn.cursor() as cur:
        if acct:
            cur.execute(
                """
                SELECT
                    p.account_id,
                    p.ric,
                    COALESCE(NULLIF(TRIM(p.ticker), ''), sm.ticker) AS ticker,
                    p.quantity,
                    sm.instrument_type,
                    p.source,
                    p.updated_at
                FROM holdings_positions_current p
                LEFT JOIN security_master sm
                  ON sm.ric = p.ric
                WHERE account_id = %s
                ORDER BY p.account_id, COALESCE(NULLIF(TRIM(p.ticker), ''), sm.ticker), p.ric
                """,
                (acct,),
            )
        else:
            cur.execute(
                """
                SELECT
                    p.account_id,
                    p.ric,
                    COALESCE(NULLIF(TRIM(p.ticker), ''), sm.ticker) AS ticker,
                    p.quantity,
                    sm.instrument_type,
                    p.source,
                    p.updated_at
                FROM holdings_positions_current p
                LEFT JOIN security_master sm
                  ON sm.ric = p.ric
                ORDER BY p.account_id, COALESCE(NULLIF(TRIM(p.ticker), ''), sm.ticker), p.ric
                """
            )
        rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for account_id, ric, ticker, quantity, instrument_type, source, updated_at in rows:
        out.append(
            {
                "account_id": str(account_id),
                "ric": _normalize_ric(ric),
                "ticker": _normalize_ticker(ticker),
                "quantity": float(quantity or 0.0),
                "instrument_type": str(instrument_type) if instrument_type is not None else None,
                "source": str(source or ""),
                "updated_at": str(updated_at) if updated_at is not None else None,
            }
        )
    return out


def apply_single_position_edit(
    pg_conn,
    *,
    account_id: str,
    quantity: Any,
    ric: str | None = None,
    ticker: str | None = None,
    source: str = "ui_edit",
    requested_by: str | None = None,
    notes: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    acct = _normalize_account_id(account_id)
    if acct is None:
        raise ValueError("invalid account_id")
    qty = _parse_quantity(str(quantity))
    ric_norm = _normalize_ric(ric)
    ticker_norm = _normalize_ticker(ticker)
    if not ric_norm and not ticker_norm:
        raise ValueError("need ric or ticker")

    resolved_ric = ric_norm
    resolved_ticker: str | None = ticker_norm or None
    if resolved_ric:
        ok, mapped_ticker = _ric_exists(pg_conn, resolved_ric)
        if not ok:
            raise ValueError(f"unknown ric: {resolved_ric}")
        resolved_ticker = mapped_ticker or resolved_ticker
    else:
        picked_ric, _alts = _resolve_ticker_to_ric(pg_conn, ticker_norm)
        if not picked_ric:
            raise ValueError(f"unknown ticker: {ticker_norm}")
        resolved_ric = picked_ric
        ok, mapped_ticker = _ric_exists(pg_conn, resolved_ric)
        resolved_ticker = mapped_ticker if ok else resolved_ticker

    _ensure_account(pg_conn, account_id=acct)
    batch_id = _insert_batch(
        pg_conn,
        account_id=acct,
        mode="upsert_absolute",
        filename="<ui_edit>",
        row_count=1,
        requested_by=requested_by,
        notes=notes,
    )
    existing = _load_current_positions(pg_conn, account_id=acct).get(resolved_ric, {})
    before_qty = existing.get("quantity")
    before_ticker = existing.get("ticker")

    action = "none"
    if qty == Decimal("0"):
        if before_qty is not None:
            _delete_position(pg_conn, account_id=acct, ric=resolved_ric)
            _insert_event(
                pg_conn,
                import_batch_id=batch_id,
                account_id=acct,
                ric=resolved_ric,
                ticker=(resolved_ticker or before_ticker),
                event_type="remove_position",
                quantity_before=before_qty,
                quantity_delta=(Decimal("0") - before_qty),
                quantity_after=Decimal("0"),
                created_by=requested_by,
            )
            action = "deleted"
    else:
        _upsert_position(
            pg_conn,
            account_id=acct,
            ric=resolved_ric,
            ticker=(resolved_ticker or before_ticker),
            quantity=qty,
            source=str(source or "ui_edit"),
            import_batch_id=batch_id,
        )
        qty_before = before_qty if before_qty is not None else Decimal("0")
        _insert_event(
            pg_conn,
            import_batch_id=batch_id,
            account_id=acct,
            ric=resolved_ric,
            ticker=(resolved_ticker or before_ticker),
            event_type="ui_edit",
            quantity_before=before_qty,
            quantity_delta=(qty - qty_before),
            quantity_after=qty,
            created_by=requested_by,
        )
        action = "upserted"

    if dry_run:
        pg_conn.rollback()
        status = "dry_run"
    else:
        pg_conn.commit()
        status = "ok"

    return {
        "status": status,
        "action": action,
        "account_id": acct,
        "ric": resolved_ric,
        "ticker": resolved_ticker,
        "quantity": float(qty),
        "import_batch_id": batch_id,
    }


def remove_single_position(
    pg_conn,
    *,
    account_id: str,
    ric: str | None = None,
    ticker: str | None = None,
    requested_by: str | None = None,
    notes: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    return apply_single_position_edit(
        pg_conn,
        account_id=account_id,
        quantity="0",
        ric=ric,
        ticker=ticker,
        source="ui_edit",
        requested_by=requested_by,
        notes=notes,
        dry_run=dry_run,
    )
