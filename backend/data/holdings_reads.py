"""Read-only holdings/account adapter for shared Neon-backed holdings surfaces."""

from __future__ import annotations

from typing import Any

from backend.data.neon import connect, resolve_dsn


class HoldingsReadError(RuntimeError):
    """Raised when the shared holdings read surface is unavailable."""


def _normalize_account_id(value: str | None) -> str:
    return str(value or "").strip().lower()


def _normalize_ric(value: str | None) -> str:
    return str(value or "").strip().upper()


def _normalize_ticker(value: str | None) -> str | None:
    clean = str(value or "").strip().upper()
    return clean or None


def _shape_position_rows(rows: list[tuple[object, object, object, object, object, object]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for raw_account_id, ric, ticker, quantity, source, updated_at in rows:
        out.append(
            {
                "account_id": str(raw_account_id),
                "ric": _normalize_ric(ric),
                "ticker": _normalize_ticker(ticker),
                "quantity": float(quantity or 0.0),
                "source": str(source or ""),
                "updated_at": str(updated_at) if updated_at is not None else None,
            }
        )
    return out


def load_holdings_accounts() -> list[dict[str, Any]]:
    conn = None
    try:
        conn = connect(dsn=resolve_dsn(None), autocommit=True)
        with conn.cursor() as cur:
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
    except Exception as exc:
        raise HoldingsReadError(
            f"Shared holdings account read failed: {type(exc).__name__}: {exc}"
        ) from exc
    finally:
        if conn is not None:
            conn.close()

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


def load_holdings_positions(*, account_id: str) -> list[dict[str, Any]]:
    account = _normalize_account_id(account_id)
    conn = None
    try:
        conn = connect(dsn=resolve_dsn(None), autocommit=True)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    p.account_id,
                    p.ric,
                    COALESCE(NULLIF(TRIM(p.ticker), ''), sm.ticker) AS ticker,
                    p.quantity,
                    p.source,
                    p.updated_at
                FROM holdings_positions_current p
                LEFT JOIN security_master sm
                  ON sm.ric = p.ric
                WHERE p.account_id = %s
                ORDER BY p.account_id, COALESCE(NULLIF(TRIM(p.ticker), ''), sm.ticker), p.ric
                """,
                (account,),
            )
            rows = cur.fetchall()
    except Exception as exc:
        raise HoldingsReadError(
            f"Shared holdings position read failed for account_id={account}: {type(exc).__name__}: {exc}"
        ) from exc
    finally:
        if conn is not None:
            conn.close()

    return _shape_position_rows(rows)


def load_all_holdings_positions() -> list[dict[str, Any]]:
    conn = None
    try:
        conn = connect(dsn=resolve_dsn(None), autocommit=True)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    p.account_id,
                    p.ric,
                    COALESCE(NULLIF(TRIM(p.ticker), ''), sm.ticker) AS ticker,
                    p.quantity,
                    p.source,
                    p.updated_at
                FROM holdings_positions_current p
                LEFT JOIN security_master sm
                  ON sm.ric = p.ric
                ORDER BY p.account_id, COALESCE(NULLIF(TRIM(p.ticker), ''), sm.ticker), p.ric
                """
            )
            rows = cur.fetchall()
    except Exception as exc:
        raise HoldingsReadError(
            f"Shared holdings position read failed for all accounts: {type(exc).__name__}: {exc}"
        ) from exc
    finally:
        if conn is not None:
            conn.close()

    return _shape_position_rows(rows)


def load_contributing_holdings_accounts() -> list[dict[str, Any]]:
    conn = None
    try:
        conn = connect(dsn=resolve_dsn(None), autocommit=True)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    p.account_id,
                    COALESCE(a.account_name, p.account_id) AS account_name
                FROM holdings_positions_current p
                LEFT JOIN holdings_accounts a
                  ON a.account_id = p.account_id
                GROUP BY p.account_id, a.account_name
                ORDER BY p.account_id ASC
                """
            )
            rows = cur.fetchall()
    except Exception as exc:
        raise HoldingsReadError(
            f"Shared holdings contributing-account read failed: {type(exc).__name__}: {exc}"
        ) from exc
    finally:
        if conn is not None:
            conn.close()

    out: list[dict[str, Any]] = []
    for account_id, account_name in rows:
        out.append(
            {
                "account_id": str(account_id),
                "account_name": str(account_name or account_id),
            }
        )
    return out


def load_aggregate_holdings_positions() -> list[dict[str, Any]]:
    conn = None
    try:
        conn = connect(dsn=resolve_dsn(None), autocommit=True)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    agg.account_id,
                    agg.ric,
                    agg.ticker,
                    agg.quantity,
                    agg.source,
                    agg.updated_at
                FROM (
                    SELECT
                        'all_accounts' AS account_id,
                        p.ric,
                        (
                            ARRAY_AGG(
                                COALESCE(NULLIF(TRIM(p.ticker), ''), sm.ticker)
                                ORDER BY p.account_id, COALESCE(NULLIF(TRIM(p.ticker), ''), sm.ticker), p.ric
                            )
                        )[1] AS ticker,
                        SUM(CAST(p.quantity AS DOUBLE PRECISION)) AS quantity,
                        'aggregate' AS source,
                        MAX(p.updated_at) AS updated_at
                    FROM holdings_positions_current p
                    LEFT JOIN security_master sm
                      ON sm.ric = p.ric
                    GROUP BY p.ric
                    HAVING ABS(SUM(CAST(p.quantity AS DOUBLE PRECISION))) > %s
                ) agg
                ORDER BY COALESCE(agg.ticker, agg.ric), agg.ric
                """,
                (1e-12,),
            )
            rows = cur.fetchall()
    except Exception as exc:
        raise HoldingsReadError(
            f"Shared holdings aggregate-position read failed: {type(exc).__name__}: {exc}"
        ) from exc
    finally:
        if conn is not None:
            conn.close()

    return _shape_position_rows(rows)
