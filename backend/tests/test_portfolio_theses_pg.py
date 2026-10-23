"""Real PostgreSQL contract/race tests, only for a dedicated disposable database.

CEIORA_THESIS_TEST_DSN must point at localhost/127.0.0.1, database ceiora_thesis_test.
No fixture reads DATABASE_URL/Neon credentials. The test database is reset per test.
"""
import os
import threading
import time
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict

from backend.data import portfolio_thesis_store as store

DSN = os.getenv("CEIORA_THESIS_TEST_DSN", "")
pytestmark = pytest.mark.skipif(not DSN, reason="Dedicated local PostgreSQL test DSN not supplied")
MIGRATIONS = Path(__file__).resolve().parents[2] / "docs/reference/migrations/neon"
OWNER = str(uuid4())
OTHER = str(uuid4())


@pytest.fixture
def pg():
    parts = conninfo_to_dict(DSN)
    assert parts.get("host") in {"localhost", "127.0.0.1"}
    assert parts.get("dbname") == "ceiora_thesis_test"
    admin = psycopg.connect(DSN, autocommit=True)
    assert admin.execute("SELECT current_database()").fetchone()[0] == "ceiora_thesis_test"
    admin.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    for name in ["NEON_HOLDINGS_SCHEMA.sql", "NEON_APP_AUTH_SCHEMA.sql", "NEON_PORTFOLIO_THESES_SCHEMA.sql"]:
        admin.execute((MIGRATIONS / name).read_text())
    admin.execute("INSERT INTO holdings_accounts(account_id, account_name) VALUES ('acct_main','Main'),('acct_short','Short')")
    admin.execute("INSERT INTO app_users(user_id,auth_user_id) VALUES (%s,'test-owner'),(%s,'test-other')", (OWNER, OTHER))
    admin.execute("INSERT INTO account_memberships(membership_id,account_id,user_id,role) VALUES (%s,'acct_main',%s,'owner'),(%s,'acct_short',%s,'owner'),(%s,'acct_main',%s,'member')", (str(uuid4()), OWNER, str(uuid4()), OWNER, str(uuid4()), OTHER))
    admin.execute("INSERT INTO holdings_positions_current(account_id,ric,ticker,quantity,source) VALUES ('acct_main','X.O','X',10,'test'),('acct_short','Y.O','Y',-10,'test')")
    admin.execute("CREATE TABLE security_registry(ric TEXT PRIMARY KEY, ticker TEXT)")
    admin.execute("INSERT INTO security_registry VALUES ('X.O','X'),('Y.O','Y'),('X.N','X')")
    conn = psycopg.connect(DSN)
    workspace, accounts = store.open_workspace(conn, subject="test-owner")
    store.create_thesis(conn, workspace, "Thesis A")
    store.create_thesis(conn, workspace, "Thesis B")
    state = store.read_state(conn, workspace)
    conn.commit()
    yield conn, workspace, state["theses"][0]["id"], state["theses"][1]["id"], admin
    conn.close()
    admin.close()


def allocate(conn, workspace, thesis, qty, account="acct_main", ric="X.O"):
    store.lock_portfolio(conn)
    store.replace_allocations(conn, workspace, [account], account_id=account, ric=ric, allocations=[{"thesis_id": thesis, "quantity": Decimal(qty)}])
    conn.commit()


def test_migration_reapply_persistence_and_independent_shared_workspaces(pg):
    conn, w, a, b, admin = pg
    allocate(conn, w, a, "8.000001")
    admin.execute((MIGRATIONS / "NEON_PORTFOLIO_THESES_SCHEMA.sql").read_text())
    conn.close()
    with psycopg.connect(DSN) as fresh:
        other, _ = store.open_workspace(fresh, subject="test-other")
        store.create_thesis(fresh, other, "Independent")
        t = store.read_state(fresh, other)["theses"][0]["id"]
        allocate(fresh, other, t, "9")
        assert store.read_state(fresh, w)["allocations"][0]["quantity"] == "8.000001"
        assert len(store.read_state(fresh, other)["allocations"]) == 1


def test_long_short_constraints_and_atomic_holding_shrink(pg):
    conn, w, a, b, admin = pg
    allocate(conn, w, a, "8")
    before = store.read_state(conn, w)["revision"]
    conn.commit()
    with pytest.raises(psycopg.Error) as exc:
        admin.execute("UPDATE holdings_positions_current SET quantity=2 WHERE account_id='acct_main'")
    assert exc.value.sqlstate == "PTH01"
    assert admin.execute("SELECT quantity FROM holdings_positions_current WHERE account_id='acct_main'").fetchone()[0] == 10
    allocate(conn, w, b, "-3.000001", "acct_short", "Y.O")
    with pytest.raises(store.ThesisError):
        allocate(conn, w, b, "3", "acct_short", "Y.O")
    conn.rollback()
    admin.execute("UPDATE holdings_positions_current SET quantity=9 WHERE account_id='acct_main'")
    state = store.read_state(conn, w)
    assert state["revision"] > before
    with pytest.raises(store.ThesisConflict):
        store.assert_revision(conn, w, before)
    conn.rollback()


def test_foreign_ownership_duplicate_and_manual_order_validation(pg):
    conn, w, a, b, admin = pg
    with pytest.raises(store.ThesisDenied):
        store.replace_allocations(conn, w, ["acct_main"], account_id="foreign", ric="X.O", allocations=[])
    with pytest.raises(store.ThesisError):
        store.replace_allocations(conn, w, ["acct_main"], account_id="acct_main", ric="X.O", allocations=[{"thesis_id": a, "quantity": Decimal(1)}]*2)
    for scope, keys in [("arbitrary", []), ("positions:dash", ["foreign:X.O"]), ("theses", [a,a])]:
        with pytest.raises(store.ThesisError):
            store.save_order(conn, w, scope, keys)
    store.save_order(conn, w, "theses", [b,a])
    conn.commit()
    assert store.read_state(conn, w)["manual_orders"]["theses"] == [b,a]


def test_delete_cleanup_and_allocation_replace_keep_valid_order(pg):
    conn, w, a, b, admin = pg
    allocate(conn, w, a, "8")
    store.save_order(conn, w, f"legs:{a}", ["acct_main:X.O"])
    store.save_order(conn, w, "positions:dash", ["acct_short:Y.O", "acct_main:X.O"])
    conn.commit()
    allocate(conn, w, a, "7")
    assert store.read_state(conn, w)["manual_orders"][f"legs:{a}"] == ["acct_main:X.O"]
    conn.commit()
    admin.execute("DELETE FROM holdings_positions_current WHERE account_id='acct_main'")
    state = store.read_state(conn, w)
    assert state["allocations"] == []
    assert state["manual_orders"]["positions:dash"] == ["acct_short:Y.O"]
    assert state["manual_orders"][f"legs:{a}"] == []


def test_revocation_cleans_only_affected_workspace_account(pg):
    conn, w, a, b, admin = pg
    allocate(conn, w, a, "8")
    allocate(conn, w, b, "-4", "acct_short", "Y.O")
    admin.execute("DELETE FROM account_memberships WHERE user_id=%s AND account_id='acct_main'", (OWNER,))
    state = store.read_state(conn, w)
    assert [row["account_id"] for row in state["allocations"]] == ["acct_short"]
    conn.commit()
    with pytest.raises(psycopg.Error) as exc:
        admin.execute("INSERT INTO portfolio_thesis_allocations(workspace_id,thesis_id,account_id,ric,quantity) VALUES (%s,%s,'acct_main','X.O',1)", (w,a))
    assert exc.value.sqlstate in {"PTH02", "23503"}


def test_raw_sql_guards_nan_rekey_repeatable_read_and_truncate(pg):
    conn, w, a, b, admin = pg
    for sql, params, code in [
        ("UPDATE holdings_positions_current SET ric='OTHER.O' WHERE account_id='acct_main'", (), "PTH03"),
        ("TRUNCATE holdings_positions_current CASCADE", (), "PTH04"),
        ("INSERT INTO portfolio_thesis_allocations(workspace_id,thesis_id,account_id,ric,quantity) VALUES (%s,%s,'acct_main','X.O','NaN')", (w,a), "23514"),
    ]:
        with pytest.raises(psycopg.Error) as exc:
            admin.execute(sql, params)
        assert exc.value.sqlstate == code
    with psycopg.connect(DSN) as repeatable:
        repeatable.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        with pytest.raises(psycopg.Error) as exc:
            repeatable.execute("UPDATE holdings_positions_current SET quantity=9 WHERE account_id='acct_main'")
        assert exc.value.sqlstate == "PTH04"
        repeatable.rollback()


def test_two_client_holding_shrink_vs_raw_allocation_never_both_commit(pg):
    conn, w, a, b, admin = pg
    outcomes = []
    started = threading.Event()
    def concurrent_allocation():
        with psycopg.connect(DSN) as second:
            started.set()
            try:
                second.execute("INSERT INTO portfolio_thesis_allocations(workspace_id,thesis_id,account_id,ric,quantity) VALUES (%s,%s,'acct_main','X.O',8)", (w,a))
                second.commit(); outcomes.append("allocated")
            except psycopg.Error as exc:
                outcomes.append(exc.sqlstate); second.rollback()
    conn.execute("SELECT portfolio_thesis_lock()")
    conn.execute("UPDATE holdings_positions_current SET quantity=5 WHERE account_id='acct_main'")
    worker = threading.Thread(target=concurrent_allocation)
    worker.start(); assert started.wait(2)
    time.sleep(.1); assert worker.is_alive()
    conn.commit(); worker.join(5); assert not worker.is_alive()
    assert outcomes == ["PTH01"]
    assert store.read_state(conn, w)["allocations"] == []


def test_concurrent_allocations_cannot_overallocate(pg):
    conn, w, a, b, admin = pg
    outcomes = []
    def second():
        with psycopg.connect(DSN) as other:
            try:
                other.execute("INSERT INTO portfolio_thesis_allocations(workspace_id,thesis_id,account_id,ric,quantity) VALUES (%s,%s,'acct_main','X.O',6)", (w,b))
                other.commit(); outcomes.append("committed")
            except psycopg.Error as exc:
                outcomes.append(exc.sqlstate); other.rollback()
    conn.execute("INSERT INTO portfolio_thesis_allocations(workspace_id,thesis_id,account_id,ric,quantity) VALUES (%s,%s,'acct_main','X.O',6)", (w,a))
    worker = threading.Thread(target=second); worker.start(); time.sleep(.1); conn.commit()
    worker.join(5); assert not worker.is_alive(); assert outcomes == ["PTH01"]


def test_revocation_wins_against_waiting_allocation(pg):
    conn, w, a, b, admin = pg
    outcomes = []
    def allocate_after_lock():
        with psycopg.connect(DSN) as other:
            try:
                other.execute("INSERT INTO portfolio_thesis_allocations(workspace_id,thesis_id,account_id,ric,quantity) VALUES (%s,%s,'acct_main','X.O',1)", (w,a))
                other.commit(); outcomes.append("committed")
            except psycopg.Error as exc:
                outcomes.append(exc.sqlstate); other.rollback()
    conn.execute("DELETE FROM account_memberships WHERE user_id=%s AND account_id='acct_main'", (OWNER,))
    worker = threading.Thread(target=allocate_after_lock); worker.start(); time.sleep(.1); conn.commit()
    worker.join(5); assert not worker.is_alive()
    assert outcomes[0] in {"PTH02", "23503"}
    assert store.read_state(conn,w)["allocations"] == []


def test_whatif_surviving_identity_preserves_allocations_and_failed_batch_rolls_back(pg, monkeypatch):
    from dataclasses import replace
    from backend.services import neon_holdings, cuse4_holdings_service as holdings
    from backend.data.portfolio_write_guard import PortfolioAllocationConflict
    conn, w, a, b, admin = pg
    allocate(conn, w, a, "8")
    monkeypatch.setattr(neon_holdings, "_ric_exists", lambda c, ric: (True, ric.split(".")[0]))
    deps = replace(holdings.get_holdings_dependencies(), dsn_resolver=lambda _: DSN, dirty_recorder=lambda **kw: None, refresh_requester=lambda trigger: None)
    result = holdings.run_whatif_apply(scenario_rows=[{"account_id":"acct_main","ticker":"X","ric":"X.O","quantity":1}], dependencies=deps)
    assert result["status"] == "ok" and result["applied_deletes"] == 0
    assert store.read_state(conn,w)["allocations"][0]["quantity"] == "8.000000"
    conn.commit()
    with pytest.raises(PortfolioAllocationConflict):
        holdings.run_whatif_apply(scenario_rows=[{"account_id":"acct_short","ticker":"Y","ric":"Y.O","quantity":1}, {"account_id":"acct_main","ticker":"X","ric":"X.O","quantity":-7}], dependencies=deps)
    quantities = dict(admin.execute("SELECT account_id,quantity FROM holdings_positions_current").fetchall())
    assert quantities == {"acct_main":Decimal(11),"acct_short":Decimal(-10)}
    # A ticker consolidation may not silently erase allocated RIC legs.
    with pytest.raises(PortfolioAllocationConflict):
        holdings.run_whatif_apply(scenario_rows=[{"account_id":"acct_main","ticker":"X","ric":"X.N","quantity":1}], dependencies=deps)
    assert admin.execute("SELECT count(*) FROM holdings_positions_current WHERE ric='X.N'").fetchone()[0] == 0


@pytest.mark.parametrize("mode,qty,expected", [("upsert_absolute","9","9"), ("increment_delta","1","11"), ("replace_account","9","9")])
def test_import_modes_preserve_saved_allocations_and_reject_invalid_reductions(pg, mode, qty, expected):
    from backend.services import neon_holdings
    from backend.services.neon_holdings_identifiers import ResolvedImportRow
    conn, w, a, b, admin = pg
    allocate(conn,w,a,"8")
    row = ResolvedImportRow(1,"acct_main","X.O","X",Decimal(qty),"test")
    result = neon_holdings.apply_holdings_import(conn, parsed={"accepted":[row]}, mode=mode, account_id="acct_main", requested_by=None,filename=None,notes=None,dry_run=False)
    assert result["status"] == "ok"
    assert admin.execute("SELECT quantity FROM holdings_positions_current WHERE account_id='acct_main'").fetchone()[0] == Decimal(expected)
    assert store.read_state(conn,w)["allocations"][0]["quantity"] == "8.000000"
    conn.commit()
    bad = ResolvedImportRow(1,"acct_main","X.O","X",Decimal("-8") if mode == "increment_delta" else Decimal(1),"test")
    with pytest.raises(psycopg.Error) as exc:
        neon_holdings.apply_holdings_import(conn, parsed={"accepted":[bad]}, mode=mode, account_id="acct_main", requested_by=None,filename=None,notes=None,dry_run=False)
    assert exc.value.sqlstate == "PTH01"
    conn.rollback()
    assert admin.execute("SELECT quantity FROM holdings_positions_current WHERE account_id='acct_main'").fetchone()[0] == Decimal(expected)


def test_rollout_explicit_restricted_writer_and_readonly_preflight(pg):
    conn, w, a, b, admin = pg
    admin.execute("DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ceiora_test_writer') THEN CREATE ROLE ceiora_test_writer NOLOGIN; END IF; END $$")
    admin.execute("GRANT USAGE ON SCHEMA public TO ceiora_test_writer")
    admin.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON holdings_accounts, holdings_positions_current, app_users, account_memberships TO ceiora_test_writer")
    # Hardened function defaults must also work; migration grants explicit execution.
    admin.execute("REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC")
    admin.execute("SELECT set_config('ceiora.portfolio_writer_roles', '[\"ceiora_test_writer\"]', false)")
    admin.execute((MIGRATIONS / 'NEON_PORTFOLIO_THESES_SCHEMA.sql').read_text())
    admin.execute((MIGRATIONS / 'NEON_PORTFOLIO_THESES_PREFLIGHT.sql').read_text())
    admin.execute('SET ROLE ceiora_test_writer')
    try:
        with admin.transaction():
            opened, _ = store.open_workspace(admin, subject='test-owner')
            assert opened == w
            admin.execute("UPDATE holdings_positions_current SET quantity=11 WHERE account_id='acct_main'")
            admin.execute("UPDATE app_users SET display_name='Synthetic' WHERE user_id=%s", (OWNER,))
            admin.execute("DELETE FROM account_memberships WHERE user_id=%s AND account_id='acct_main'", (OWNER,))
        assert admin.execute('SELECT count(*) FROM portfolio_workspace_accounts WHERE workspace_id=%s AND account_id=%s', (w, 'acct_main')).fetchone()[0] == 0
        assert not admin.execute("SELECT has_table_privilege(current_user,'portfolio_schema_versions','UPDATE')").fetchone()[0]
    finally:
        admin.execute('RESET ROLE')
        admin.execute("RESET ceiora.portfolio_writer_roles")


@pytest.mark.parametrize('roles', ['{}', '[7]', '["PUBLIC"]', '["ceiora_missing_role"]'])
def test_rollout_invalid_roles_abort_transaction(pg, roles):
    conn, w, a, b, admin = pg
    before = admin.execute('SELECT applied_at FROM portfolio_schema_versions WHERE version=1').fetchone()
    admin.execute("SELECT set_config('ceiora.portfolio_writer_roles', %s, false)", (roles,))
    with pytest.raises(psycopg.Error):
        admin.execute((MIGRATIONS / 'NEON_PORTFOLIO_THESES_SCHEMA.sql').read_text())
    admin.execute('ROLLBACK')
    admin.execute('RESET ceiora.portfolio_writer_roles')
    assert admin.execute('SELECT applied_at FROM portfolio_schema_versions WHERE version=1').fetchone() == before


def test_rollout_public_search_path_and_bounded_lock_wait(pg):
    conn, w, a, b, admin = pg
    admin.execute('CREATE SCHEMA operator_private')
    admin.execute('SET search_path=operator_private,public')
    admin.execute((MIGRATIONS / 'NEON_PORTFOLIO_THESES_SCHEMA.sql').read_text())
    assert admin.execute("SELECT to_regclass('operator_private.portfolio_theses')").fetchone()[0] is None
    assert admin.execute("SELECT current_setting('search_path')").fetchone()[0] == 'operator_private, public'
    admin.execute('RESET search_path')
    conn.execute('SELECT pg_advisory_xact_lock(1128614226,1)')
    started = time.monotonic()
    try:
        with pytest.raises(psycopg.errors.LockNotAvailable):
            admin.execute((MIGRATIONS / 'NEON_PORTFOLIO_THESES_SCHEMA.sql').read_text())
        assert 4 <= time.monotonic() - started < 15
    finally:
        admin.execute('ROLLBACK')
        conn.rollback()
    assert admin.execute('SELECT count(*) FROM portfolio_schema_versions WHERE version=1').fetchone()[0] == 1
