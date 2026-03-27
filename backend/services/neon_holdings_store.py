"""Persistence primitives for Neon holdings workflows."""

from __future__ import annotations

import uuid
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from backend.services.neon_holdings_identifiers import QTY_SCALE, normalize_account_id, normalize_ric, normalize_ticker
from backend.services.neon_stage2 import apply_sql_file


_REGISTRY_TICKER_EXPR = "COALESCE(NULLIF(TRIM(p.ticker), ''), NULLIF(TRIM(reg.ticker), ''))"
_REGISTRY_JOIN_SQL = """
            LEFT JOIN security_registry reg
              ON reg.ric = p.ric
"""


def ensure_holdings_schema(pg_conn, *, schema_sql_path: Path) -> dict[str, Any]:
    return apply_sql_file(pg_conn, sql_path=schema_sql_path)


def insert_batch(
    pg_conn,
    *,
    account_id: str,
    mode: str,
    filename: str | None,
    row_count: int,
    requested_by: str | None,
    notes: str | None,
) -> str:
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


def ensure_account(pg_conn, *, account_id: str) -> None:
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


def load_current_positions(pg_conn, *, account_id: str) -> dict[str, dict[str, Any]]:
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
        out[normalize_ric(ric)] = {
            "ticker": (normalize_ticker(ticker) if ticker is not None else None),
            "quantity": Decimal(str(qty)).quantize(QTY_SCALE, rounding=ROUND_HALF_UP),
        }
    return out


def load_current_positions_for_ticker(pg_conn, *, account_id: str, ticker: str) -> list[dict[str, Any]]:
    acct = normalize_account_id(account_id)
    tkr = normalize_ticker(ticker)
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                p.ric,
                """ + _REGISTRY_TICKER_EXPR + """ AS ticker,
                p.quantity,
                p.source
            FROM holdings_positions_current p
            """ + _REGISTRY_JOIN_SQL + """
            WHERE p.account_id = %s
              AND UPPER(""" + _REGISTRY_TICKER_EXPR + """) = %s
            ORDER BY p.ric
            """,
            (acct, tkr),
        )
        rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for ric, row_ticker, qty, source in rows:
        out.append(
            {
                "account_id": str(acct),
                "ric": normalize_ric(ric),
                "ticker": normalize_ticker(row_ticker),
                "quantity": Decimal(str(qty)).quantize(QTY_SCALE, rounding=ROUND_HALF_UP),
                "source": str(source or ""),
            }
        )
    return out


def upsert_position(
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


def delete_position(pg_conn, *, account_id: str, ric: str) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            "DELETE FROM holdings_positions_current WHERE account_id = %s AND ric = %s",
            (account_id, ric),
        )


def insert_event(
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
    acct = normalize_account_id(account_id) if account_id is not None else None
    with pg_conn.cursor() as cur:
        if acct:
            cur.execute(
                """
                SELECT
                    p.account_id,
                    p.ric,
                    """ + _REGISTRY_TICKER_EXPR + """ AS ticker,
                    p.quantity,
                    p.source,
                    p.updated_at
                FROM holdings_positions_current p
                """ + _REGISTRY_JOIN_SQL + """
                WHERE account_id = %s
                ORDER BY p.account_id, """ + _REGISTRY_TICKER_EXPR + """, p.ric
                """,
                (acct,),
            )
        else:
            cur.execute(
                """
                SELECT
                    p.account_id,
                    p.ric,
                    """ + _REGISTRY_TICKER_EXPR + """ AS ticker,
                    p.quantity,
                    p.source,
                    p.updated_at
                FROM holdings_positions_current p
                """ + _REGISTRY_JOIN_SQL + """
                ORDER BY p.account_id, """ + _REGISTRY_TICKER_EXPR + """, p.ric
                """
            )
        rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for account_id, ric, ticker, quantity, source, updated_at in rows:
        out.append(
            {
                "account_id": str(account_id),
                "ric": normalize_ric(ric),
                "ticker": normalize_ticker(ticker),
                "quantity": float(quantity or 0.0),
                "source": str(source or ""),
                "updated_at": str(updated_at) if updated_at is not None else None,
            }
        )
    return out
