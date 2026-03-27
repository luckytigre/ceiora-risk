"""Neon holdings workflow behaviors."""

from __future__ import annotations

import csv
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from backend.services import neon_holdings_identifiers, neon_holdings_store


ResolvedImportRow = neon_holdings_identifiers.ResolvedImportRow
QTY_SCALE = neon_holdings_identifiers.QTY_SCALE
IMPORT_MODES = neon_holdings_identifiers.IMPORT_MODES

_normalize_account_id = neon_holdings_identifiers.normalize_account_id
_normalize_ric = neon_holdings_identifiers.normalize_ric
_normalize_ticker = neon_holdings_identifiers.normalize_ticker
_parse_quantity = neon_holdings_identifiers.parse_quantity
_ric_exists = neon_holdings_identifiers.ric_exists
_resolve_ticker_to_ric = neon_holdings_identifiers.resolve_ticker_to_ric_internal
resolve_ticker_to_ric = neon_holdings_identifiers.resolve_ticker_to_ric

ensure_holdings_schema = neon_holdings_store.ensure_holdings_schema
_insert_batch = neon_holdings_store.insert_batch
_ensure_account = neon_holdings_store.ensure_account
_load_current_positions = neon_holdings_store.load_current_positions
_load_current_positions_for_ticker = neon_holdings_store.load_current_positions_for_ticker
_upsert_position = neon_holdings_store.upsert_position
_delete_position = neon_holdings_store.delete_position
_insert_event = neon_holdings_store.insert_event
list_holdings_accounts = neon_holdings_store.list_holdings_accounts
list_holdings_positions = neon_holdings_store.list_holdings_positions


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
                            "message": f"RIC not found in security registry: {ric}",
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
                            "message": f"Ticker not found in security registry: {ticker}",
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


def apply_ticker_bucket_scenario(
    pg_conn,
    *,
    scenario_rows: list[dict[str, Any]],
    requested_by: str | None = None,
    default_source: str = "what_if",
    dry_run: bool = False,
) -> dict[str, Any]:
    accepted_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    warnings: list[str] = []
    seen_keys: set[tuple[str, str]] = set()

    for idx, raw in enumerate(list(scenario_rows or []), start=1):
        account_id = _normalize_account_id(raw.get("account_id"))
        ticker = _normalize_ticker(raw.get("ticker"))
        if account_id is None:
            rejected_rows.append(
                {
                    "row_number": idx,
                    "reason_code": "invalid_account_id",
                    "message": "account_id missing or invalid",
                }
            )
            continue
        if not ticker:
            rejected_rows.append(
                {
                    "row_number": idx,
                    "reason_code": "missing_identifier",
                    "message": "need ticker",
                }
            )
            continue
        try:
            qty = _parse_quantity(str(raw.get("quantity")))
        except (InvalidOperation, ValueError):
            rejected_rows.append(
                {
                    "row_number": idx,
                    "reason_code": "invalid_quantity",
                    "message": f"invalid quantity: {raw.get('quantity')!r}",
                }
            )
            continue
        dup_key = (account_id, ticker)
        if dup_key in seen_keys:
            rejected_rows.append(
                {
                    "row_number": idx,
                    "reason_code": "duplicate_row_in_file",
                    "message": f"duplicate account_id+ticker in payload: {account_id}/{ticker}",
                }
            )
            continue
        seen_keys.add(dup_key)
        resolved_ric, alternatives = _resolve_ticker_to_ric(pg_conn, ticker)
        if not resolved_ric:
            rejected_rows.append(
                {
                    "row_number": idx,
                    "reason_code": "unknown_ticker",
                    "message": f"Ticker not found in security registry: {ticker}",
                }
            )
            continue
        if alternatives:
            warnings.append(
                f"row {idx}: ticker {ticker} resolved to {resolved_ric}; alternatives={','.join(alternatives[:10])}"
            )
        accepted_rows.append(
            {
                "row_number": idx,
                "account_id": account_id,
                "ticker": ticker,
                "ric": resolved_ric,
                "quantity": qty,
                "source": str(raw.get("source") or default_source).strip() or str(default_source),
            }
        )

    rejection_counts: dict[str, int] = {}
    for row in rejected_rows:
        code = str(row.get("reason_code") or "unknown")
        rejection_counts[code] = int(rejection_counts.get(code, 0) + 1)

    if rejected_rows:
        if dry_run:
            pg_conn.rollback()
        return {
            "status": "rejected",
            "accepted_rows": len(accepted_rows),
            "rejected_rows": len(rejected_rows),
            "rejection_counts": rejection_counts,
            "warnings": warnings,
            "applied_upserts": 0,
            "applied_deletes": 0,
            "row_results": [],
            "rejected": rejected_rows,
        }

    batches_by_account: dict[str, str] = {}
    for account_id in sorted({str(row["account_id"]) for row in accepted_rows}):
        _ensure_account(pg_conn, account_id=account_id)
        batches_by_account[account_id] = _insert_batch(
            pg_conn,
            account_id=account_id,
            mode="replace_ticker_bucket",
            filename="<what_if_apply>",
            row_count=sum(1 for row in accepted_rows if str(row["account_id"]) == account_id),
            requested_by=requested_by,
            notes="Apply staged what-if scenario rows",
        )

    applied_upserts = 0
    applied_deletes = 0
    row_results: list[dict[str, Any]] = []

    for row in accepted_rows:
        account_id = str(row["account_id"])
        ticker = str(row["ticker"])
        resolved_ric = str(row["ric"])
        delta_qty = Decimal(str(row["quantity"])).quantize(QTY_SCALE, rounding=ROUND_HALF_UP)
        source = str(row["source"])
        batch_id = batches_by_account[account_id]
        existing_rows = _load_current_positions_for_ticker(pg_conn, account_id=account_id, ticker=ticker)
        current_total = sum((Decimal(str(item["quantity"])) for item in existing_rows), start=Decimal("0")).quantize(QTY_SCALE, rounding=ROUND_HALF_UP)
        target_qty = (current_total + delta_qty).quantize(QTY_SCALE, rounding=ROUND_HALF_UP)

        if len(existing_rows) == 1 and str(existing_rows[0]["ric"]) == resolved_ric and current_total == target_qty:
            row_results.append(
                {
                    "account_id": account_id,
                    "ticker": ticker,
                    "ric": resolved_ric,
                    "current_quantity": float(current_total),
                    "applied_quantity": float(target_qty),
                    "delta_quantity": float(delta_qty),
                    "action": "none",
                }
            )
            continue

        for existing in existing_rows:
            _delete_position(pg_conn, account_id=account_id, ric=str(existing["ric"]))
            _insert_event(
                pg_conn,
                import_batch_id=batch_id,
                account_id=account_id,
                ric=str(existing["ric"]),
                ticker=ticker,
                event_type="replace_ticker_bucket_delete",
                quantity_before=Decimal(str(existing["quantity"])),
                quantity_delta=Decimal(str(existing["quantity"])) * Decimal("-1"),
                quantity_after=Decimal("0"),
                created_by=requested_by,
            )
            applied_deletes += 1

        action = "remove" if abs(target_qty) <= 0 else "replace"
        if abs(target_qty) > 0:
            _upsert_position(
                pg_conn,
                account_id=account_id,
                ric=resolved_ric,
                ticker=ticker,
                quantity=target_qty,
                source=source,
                import_batch_id=batch_id,
            )
            _insert_event(
                pg_conn,
                import_batch_id=batch_id,
                account_id=account_id,
                ric=resolved_ric,
                ticker=ticker,
                event_type="replace_ticker_bucket_set",
                quantity_before=current_total,
                quantity_delta=delta_qty,
                quantity_after=target_qty,
                created_by=requested_by,
            )
            applied_upserts += 1

        row_results.append(
            {
                "account_id": account_id,
                "ticker": ticker,
                "ric": resolved_ric,
                "current_quantity": float(current_total),
                "applied_quantity": float(target_qty),
                "delta_quantity": float(delta_qty),
                "action": action,
            }
        )

    if dry_run:
        pg_conn.rollback()
        status = "dry_run"
    else:
        pg_conn.commit()
        status = "ok"

    return {
        "status": status,
        "accepted_rows": len(accepted_rows),
        "rejected_rows": len(rejected_rows),
        "rejection_counts": rejection_counts,
        "warnings": warnings,
        "applied_upserts": int(applied_upserts),
        "applied_deletes": int(applied_deletes),
        "row_results": row_results,
        "rejected": rejected_rows,
        "import_batch_ids": batches_by_account,
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
                        "message": f"RIC not found in security registry: {ric}",
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
                        "message": f"Ticker not found in security registry: {ticker}",
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
