"""Transactional persistence for authenticated portfolio classification workspaces.

Schema is installed explicitly by migration; this module performs no DDL or commits.
"""
from __future__ import annotations

import json
import uuid
from decimal import Decimal


class ThesisError(Exception):
    status = 400
    code = "invalid_thesis_request"


class ThesisUnavailable(ThesisError):
    status = 404
    code = "theses_unavailable"


class ThesisDenied(ThesisError):
    status = 403
    code = "thesis_account_denied"


class ThesisNotFound(ThesisError):
    status = 404
    code = "thesis_not_found"


class ThesisConflict(ThesisError):
    status = 409
    code = "stale_thesis_revision"


def lock_portfolio(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT portfolio_thesis_lock()")


def open_workspace(conn, *, subject: str) -> tuple[str, list[str]]:
    """Resolve existing identity/memberships under the writer lock; never bootstrap auth."""
    lock_portfolio(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT user_id FROM app_users WHERE auth_user_id = %s", (subject,))
        user = cur.fetchone()
        if not user:
            raise ThesisUnavailable("An existing app identity is required for theses.")
        user_id = user[0]
        cur.execute("""SELECT m.account_id FROM account_memberships m
            JOIN holdings_accounts a ON a.account_id = m.account_id
            WHERE m.user_id = %s AND a.is_active AND a.archived_at IS NULL ORDER BY m.account_id""", (user_id,))
        accounts = [row[0] for row in cur.fetchall()]
        if not accounts:
            raise ThesisUnavailable("No active account memberships are available for theses.")
        cur.execute("""INSERT INTO portfolio_workspaces(workspace_id, owner_user_id)
            VALUES (%s, %s) ON CONFLICT (owner_user_id) DO NOTHING""", (str(uuid.uuid4()), user_id))
        cur.execute("SELECT workspace_id FROM portfolio_workspaces WHERE owner_user_id = %s", (user_id,))
        workspace = str(cur.fetchone()[0])
        cur.execute("DELETE FROM portfolio_workspace_accounts WHERE workspace_id = %s AND NOT (account_id = ANY(%s))", (workspace, accounts))
        for account in accounts:
            cur.execute("""INSERT INTO portfolio_workspace_accounts(workspace_id, account_id)
                VALUES (%s, %s) ON CONFLICT DO NOTHING""", (workspace, account))
        return workspace, accounts


def assert_revision(conn, workspace: str, revision: int) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT revision FROM portfolio_workspaces WHERE workspace_id = %s FOR UPDATE", (workspace,))
        if cur.fetchone()[0] != revision:
            raise ThesisConflict("Thesis data changed in another view. Refresh and try again.")


def read_state(conn, workspace: str) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT revision FROM portfolio_workspaces WHERE workspace_id = %s", (workspace,))
        revision = cur.fetchone()[0]
        cur.execute("SELECT thesis_id, name, color, created_at FROM portfolio_theses WHERE workspace_id = %s ORDER BY created_at, thesis_id", (workspace,))
        theses = [{"id": str(i), "name": name, "color": color, "created_at": created.isoformat()} for i, name, color, created in cur.fetchall()]
        cur.execute("""SELECT a.thesis_id, a.account_id, a.ric, coalesce(nullif(p.ticker, ''), p.ric), a.quantity
            FROM portfolio_thesis_allocations a JOIN holdings_positions_current p USING(account_id, ric)
            WHERE a.workspace_id = %s ORDER BY a.thesis_id, a.account_id, a.ric""", (workspace,))
        allocations = [{"thesis_id": str(i), "account_id": account, "ric": ric, "ticker": ticker, "quantity": str(qty)} for i, account, ric, ticker, qty in cur.fetchall()]
        cur.execute("SELECT scope, keys FROM portfolio_manual_orders WHERE workspace_id = %s ORDER BY scope", (workspace,))
        orders = {scope: keys for scope, keys in cur.fetchall()}
    return {"version": 1, "workspace_id": workspace, "revision": revision, "theses": theses, "allocations": allocations, "manual_orders": orders}


def create_thesis(conn, workspace: str, name: str) -> None:
    colors = ("#739eb8", "#b49bd0", "#8fba9e", "#c8aa74", "#c78c9f", "#78b7b7")
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM portfolio_theses WHERE workspace_id = %s", (workspace,))
        count = cur.fetchone()[0]
        if count >= 1000:
            raise ThesisError("A workspace can contain at most 1000 theses.")
        cur.execute("INSERT INTO portfolio_theses(workspace_id, thesis_id, name, color) VALUES (%s, %s, %s, %s)", (workspace, str(uuid.uuid4()), name, colors[count % len(colors)]))


def change_thesis(conn, workspace: str, thesis_id: str, name: str | None) -> None:
    with conn.cursor() as cur:
        if name is None:
            cur.execute("DELETE FROM portfolio_theses WHERE workspace_id = %s AND thesis_id = %s", (workspace, thesis_id))
        else:
            cur.execute("UPDATE portfolio_theses SET name = %s WHERE workspace_id = %s AND thesis_id = %s", (name, workspace, thesis_id))
        if cur.rowcount != 1:
            raise ThesisNotFound("Thesis does not exist in this workspace.")


def replace_allocations(conn, workspace: str, accounts: list[str], *, account_id: str, ric: str, allocations: list[dict]) -> None:
    if account_id not in accounts:
        raise ThesisDenied("Account is outside this workspace.")
    ids = [str(row["thesis_id"]) for row in allocations]
    if len(ids) != len(set(ids)):
        raise ThesisError("Duplicate thesis allocations are not allowed.")
    with conn.cursor() as cur:
        cur.execute("SELECT quantity FROM holdings_positions_current WHERE account_id = %s AND ric = %s", (account_id, ric))
        holding = cur.fetchone()
        if not holding:
            raise ThesisNotFound("Holding does not exist.")
        quantity = Decimal(str(holding[0]))
        if not quantity.is_finite() or any(not row["quantity"].is_finite() or row["quantity"] == 0 or row["quantity"].is_signed() != quantity.is_signed() for row in allocations):
            raise ThesisError("Allocations must be finite, nonzero and match the holding direction.")
        if sum((abs(row["quantity"]) for row in allocations), Decimal(0)) > abs(quantity):
            raise ThesisConflict("Allocations exceed the available holding quantity.")
        cur.execute("SELECT thesis_id::text FROM portfolio_theses WHERE workspace_id = %s", (workspace,))
        known = {row[0] for row in cur.fetchall()}
        if not set(ids) <= known:
            raise ThesisNotFound("Thesis does not exist in this workspace.")
        # Upsert retained legs before removing obsolete ones. A temporary total
        # can exceed the holding when redistributing, so delete changed legs first.
        # Order keys are retained across this replacement and pruned only afterward.
        cur.execute("SELECT scope, keys FROM portfolio_manual_orders WHERE workspace_id = %s", (workspace,))
        previous_orders = dict(cur.fetchall())
        cur.execute("DELETE FROM portfolio_thesis_allocations WHERE workspace_id = %s AND account_id = %s AND ric = %s", (workspace, account_id, ric))
        for row in allocations:
            cur.execute("INSERT INTO portfolio_thesis_allocations(workspace_id, thesis_id, account_id, ric, quantity) VALUES (%s, %s, %s, %s, %s)", (workspace, str(row["thesis_id"]), account_id, ric, row["quantity"]))
        for scope, keys in previous_orders.items():
            valid = valid_order_keys(conn, workspace, scope)
            retained = [key for key in keys if key in valid]
            cur.execute("""INSERT INTO portfolio_manual_orders(workspace_id, scope, keys) VALUES (%s, %s, %s::jsonb)
                ON CONFLICT (workspace_id, scope) DO UPDATE SET keys = EXCLUDED.keys""", (workspace, scope, json.dumps(retained)))


def valid_order_keys(conn, workspace: str, scope: str) -> set[str]:
    with conn.cursor() as cur:
        if scope == "theses":
            cur.execute("SELECT thesis_id::text FROM portfolio_theses WHERE workspace_id = %s", (workspace,))
        elif scope.startswith("legs:"):
            try:
                thesis = str(uuid.UUID(scope[5:]))
            except ValueError as exc:
                raise ThesisError("Invalid thesis order scope.") from exc
            cur.execute("SELECT 1 FROM portfolio_theses WHERE workspace_id = %s AND thesis_id = %s", (workspace, thesis))
            if not cur.fetchone():
                raise ThesisNotFound("Thesis does not exist in this workspace.")
            cur.execute("SELECT account_id || ':' || ric FROM portfolio_thesis_allocations WHERE workspace_id = %s AND thesis_id = %s", (workspace, thesis))
        elif scope in {f"{kind}:{page}" for kind in ("positions", "unallocated") for page in ("holdings", "cuse-risk", "cpar-risk", "dash")}:
            cur.execute("""SELECT p.account_id || ':' || p.ric FROM holdings_positions_current p
                JOIN portfolio_workspace_accounts wa USING(account_id) WHERE wa.workspace_id = %s""", (workspace,))
        else:
            raise ThesisError("Unknown manual-order scope.")
        return {row[0] for row in cur.fetchall()}


def save_order(conn, workspace: str, scope: str, keys: list[str]) -> None:
    if len(keys) != len(set(keys)) or not set(keys) <= valid_order_keys(conn, workspace, scope):
        raise ThesisError("Manual order contains duplicate or foreign keys.")
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO portfolio_manual_orders(workspace_id, scope, keys) VALUES (%s, %s, %s::jsonb)
            ON CONFLICT (workspace_id, scope) DO UPDATE SET keys = EXCLUDED.keys""", (workspace, scope, json.dumps(keys)))
