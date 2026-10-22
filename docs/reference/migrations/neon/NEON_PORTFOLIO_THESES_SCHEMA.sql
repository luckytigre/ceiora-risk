-- Additive thesis-workspace schema v1. Requires holdings and app-auth schemas.
-- Run explicitly on a direct connection, never from serving reads.
BEGIN;
SELECT pg_advisory_xact_lock(1128614226, 1);

CREATE TABLE IF NOT EXISTS portfolio_schema_versions (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS portfolio_workspaces (
    workspace_id UUID PRIMARY KEY,
    owner_user_id UUID NOT NULL UNIQUE REFERENCES app_users(user_id) ON DELETE CASCADE,
    revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS portfolio_workspace_accounts (
    workspace_id UUID NOT NULL REFERENCES portfolio_workspaces(workspace_id) ON DELETE CASCADE,
    account_id TEXT NOT NULL REFERENCES holdings_accounts(account_id) ON DELETE CASCADE,
    PRIMARY KEY (workspace_id, account_id)
);
CREATE TABLE IF NOT EXISTS portfolio_theses (
    workspace_id UUID NOT NULL REFERENCES portfolio_workspaces(workspace_id) ON DELETE CASCADE,
    thesis_id UUID NOT NULL,
    name TEXT NOT NULL CHECK (length(btrim(name)) BETWEEN 1 AND 80 AND name = btrim(name)),
    color TEXT NOT NULL CHECK (color ~ '^#[0-9a-fA-F]{6}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (workspace_id, thesis_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_portfolio_thesis_name
    ON portfolio_theses(workspace_id, lower(name));
CREATE TABLE IF NOT EXISTS portfolio_thesis_allocations (
    workspace_id UUID NOT NULL,
    thesis_id UUID NOT NULL,
    account_id TEXT NOT NULL,
    ric TEXT NOT NULL,
    quantity NUMERIC(20,6) NOT NULL CHECK (quantity <> 0 AND quantity::text NOT IN ('NaN', 'Infinity', '-Infinity')),
    PRIMARY KEY (workspace_id, thesis_id, account_id, ric),
    FOREIGN KEY (workspace_id, thesis_id) REFERENCES portfolio_theses(workspace_id, thesis_id) ON DELETE CASCADE,
    FOREIGN KEY (workspace_id, account_id) REFERENCES portfolio_workspace_accounts(workspace_id, account_id) ON DELETE CASCADE,
    FOREIGN KEY (account_id, ric) REFERENCES holdings_positions_current(account_id, ric) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_thesis_allocations_holding
    ON portfolio_thesis_allocations(account_id, ric, workspace_id);
CREATE TABLE IF NOT EXISTS portfolio_manual_orders (
    workspace_id UUID NOT NULL REFERENCES portfolio_workspaces(workspace_id) ON DELETE CASCADE,
    scope TEXT NOT NULL CHECK (scope IN ('theses', 'positions:holdings', 'positions:cuse-risk', 'positions:cpar-risk', 'positions:dash', 'unallocated:holdings', 'unallocated:cuse-risk', 'unallocated:cpar-risk', 'unallocated:dash') OR scope ~ '^legs:[0-9a-f-]{36}$'),
    keys JSONB NOT NULL CHECK (jsonb_typeof(keys) = 'array' AND jsonb_array_length(keys) <= 10000),
    PRIMARY KEY (workspace_id, scope)
);

-- One small-app writer lane protects holdings, classifications, and membership
-- changes. The lock is transaction-scoped and is also taken BEFORE read/modify/write
-- in application callers. VOLATILE trigger queries get fresh READ COMMITTED views.
CREATE OR REPLACE FUNCTION portfolio_thesis_lock() RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path = public, pg_temp AS $$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'Portfolio writes require READ COMMITTED isolation' USING ERRCODE = 'PTH04';
    END IF;
    PERFORM pg_advisory_xact_lock(1128614226, 1);
END;
$$;
CREATE OR REPLACE FUNCTION portfolio_lock_statement() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path = public, pg_temp AS $$
BEGIN
    PERFORM portfolio_thesis_lock();
    RETURN NULL;
END;
$$;
CREATE OR REPLACE FUNCTION portfolio_deny_truncate() RETURNS trigger
LANGUAGE plpgsql SET search_path = public, pg_temp AS $$
BEGIN
    RAISE EXCEPTION 'TRUNCATE would bypass portfolio allocation safeguards; use transactional DELETE' USING ERRCODE = 'PTH04';
END;
$$;
DO $$
DECLARE target TEXT;
BEGIN
    FOREACH target IN ARRAY ARRAY['holdings_positions_current', 'holdings_accounts', 'app_users', 'account_memberships', 'portfolio_workspaces', 'portfolio_workspace_accounts', 'portfolio_theses', 'portfolio_thesis_allocations', 'portfolio_manual_orders'] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS portfolio_write_lock ON %I', target);
        EXECUTE format('CREATE TRIGGER portfolio_write_lock BEFORE INSERT OR UPDATE OR DELETE ON %I FOR EACH STATEMENT EXECUTE FUNCTION portfolio_lock_statement()', target);
        EXECUTE format('DROP TRIGGER IF EXISTS portfolio_no_truncate ON %I', target);
        EXECUTE format('CREATE TRIGGER portfolio_no_truncate BEFORE TRUNCATE ON %I FOR EACH STATEMENT EXECUTE FUNCTION portfolio_deny_truncate()', target);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION portfolio_touch_revision() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path = public, pg_temp AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.workspace_id <> NEW.workspace_id THEN
        RAISE EXCEPTION 'Workspace identity is immutable' USING ERRCODE = 'PTH03';
    END IF;
    UPDATE portfolio_workspaces SET revision = revision + 1
    WHERE workspace_id = CASE WHEN TG_OP = 'DELETE' THEN OLD.workspace_id ELSE NEW.workspace_id END;
    RETURN NULL;
END;
$$;
DO $$
DECLARE target TEXT;
BEGIN
    FOREACH target IN ARRAY ARRAY['portfolio_theses', 'portfolio_thesis_allocations', 'portfolio_manual_orders', 'portfolio_workspace_accounts'] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS portfolio_revision ON %I', target);
        EXECUTE format('CREATE TRIGGER portfolio_revision AFTER INSERT OR UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION portfolio_touch_revision()', target);
    END LOOP;
END;
$$;
CREATE OR REPLACE FUNCTION portfolio_validate_allocation() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path = public, pg_temp AS $$
DECLARE holding NUMERIC; allocated NUMERIC;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM portfolio_workspaces w
        JOIN account_memberships m ON m.user_id = w.owner_user_id AND m.account_id = NEW.account_id
        JOIN holdings_accounts a ON a.account_id = m.account_id AND a.is_active AND a.archived_at IS NULL
        WHERE w.workspace_id = NEW.workspace_id
    ) THEN
        RAISE EXCEPTION 'Account is outside the workspace membership' USING ERRCODE = 'PTH02';
    END IF;
    SELECT quantity INTO holding FROM holdings_positions_current WHERE account_id = NEW.account_id AND ric = NEW.ric;
    SELECT sum(abs(quantity)) INTO allocated FROM portfolio_thesis_allocations
    WHERE workspace_id = NEW.workspace_id AND account_id = NEW.account_id AND ric = NEW.ric;
    IF holding IS NULL OR holding::text IN ('NaN', 'Infinity', '-Infinity') OR sign(holding) <> sign(NEW.quantity) OR allocated > abs(holding) THEN
        RAISE EXCEPTION 'Allocations must match holding direction and available quantity' USING ERRCODE = 'PTH01';
    END IF;
    RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS portfolio_allocation_guard ON portfolio_thesis_allocations;
CREATE TRIGGER portfolio_allocation_guard AFTER INSERT OR UPDATE ON portfolio_thesis_allocations
FOR EACH ROW EXECUTE FUNCTION portfolio_validate_allocation();

CREATE OR REPLACE FUNCTION portfolio_validate_holding() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path = public, pg_temp AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND (OLD.account_id <> NEW.account_id OR OLD.ric <> NEW.ric) THEN
        RAISE EXCEPTION 'Holding identity is immutable; explicitly remove and add holdings instead' USING ERRCODE = 'PTH03';
    END IF;
    IF EXISTS (
        SELECT workspace_id FROM portfolio_thesis_allocations
        WHERE account_id = NEW.account_id AND ric = NEW.ric
        GROUP BY workspace_id
        HAVING sum(abs(quantity)) > abs(NEW.quantity)
           OR bool_or(sign(quantity) <> sign(NEW.quantity))
           OR NEW.quantity::text IN ('NaN', 'Infinity', '-Infinity')
    ) THEN
        RAISE EXCEPTION 'Adjust thesis allocations before reducing or reversing this holding' USING ERRCODE = 'PTH01';
    END IF;
    UPDATE portfolio_workspaces w SET revision = revision + 1
    WHERE w.workspace_id IN (SELECT workspace_id FROM portfolio_workspace_accounts WHERE account_id = NEW.account_id);
    RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS portfolio_holding_guard ON holdings_positions_current;
CREATE TRIGGER portfolio_holding_guard AFTER INSERT OR UPDATE ON holdings_positions_current
FOR EACH ROW EXECUTE FUNCTION portfolio_validate_holding();

CREATE OR REPLACE FUNCTION portfolio_prune_orders(target_workspace UUID) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path = public, pg_temp AS $$
BEGIN
    DELETE FROM portfolio_manual_orders o WHERE workspace_id = target_workspace AND scope LIKE 'legs:%'
      AND NOT EXISTS (SELECT 1 FROM portfolio_theses t WHERE t.workspace_id = o.workspace_id AND t.thesis_id::text = substring(o.scope FROM 6));
    UPDATE portfolio_manual_orders o SET keys = (
        SELECT coalesce(jsonb_agg(item ORDER BY ord), '[]'::jsonb)
        FROM jsonb_array_elements_text(o.keys) WITH ORDINALITY AS k(item, ord)
        WHERE (o.scope = 'theses' AND EXISTS (SELECT 1 FROM portfolio_theses t WHERE t.workspace_id = o.workspace_id AND t.thesis_id::text = item))
        OR (o.scope LIKE 'legs:%' AND EXISTS (SELECT 1 FROM portfolio_thesis_allocations a WHERE a.workspace_id = o.workspace_id AND a.thesis_id::text = substring(o.scope FROM 6) AND a.account_id || ':' || a.ric = item))
        OR (o.scope LIKE 'positions:%' OR o.scope LIKE 'unallocated:%') AND EXISTS (
            SELECT 1 FROM holdings_positions_current p JOIN portfolio_workspace_accounts wa ON wa.account_id = p.account_id
            WHERE wa.workspace_id = o.workspace_id AND p.account_id || ':' || p.ric = item
        )
    ) WHERE o.workspace_id = target_workspace AND o.keys IS DISTINCT FROM (
        SELECT coalesce(jsonb_agg(item ORDER BY ord), '[]'::jsonb)
        FROM jsonb_array_elements_text(o.keys) WITH ORDINALITY AS k(item, ord)
        WHERE (o.scope = 'theses' AND EXISTS (SELECT 1 FROM portfolio_theses t WHERE t.workspace_id = o.workspace_id AND t.thesis_id::text = item))
        OR (o.scope LIKE 'legs:%' AND EXISTS (SELECT 1 FROM portfolio_thesis_allocations a WHERE a.workspace_id = o.workspace_id AND a.thesis_id::text = substring(o.scope FROM 6) AND a.account_id || ':' || a.ric = item))
        OR (o.scope LIKE 'positions:%' OR o.scope LIKE 'unallocated:%') AND EXISTS (
            SELECT 1 FROM holdings_positions_current p JOIN portfolio_workspace_accounts wa ON wa.account_id = p.account_id
            WHERE wa.workspace_id = o.workspace_id AND p.account_id || ':' || p.ric = item
        )
    );
END;
$$;
CREATE OR REPLACE FUNCTION portfolio_cleanup_deleted() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path = public, pg_temp AS $$
DECLARE target UUID;
BEGIN
    IF TG_TABLE_NAME = 'holdings_positions_current' THEN
        FOR target IN SELECT workspace_id FROM portfolio_workspace_accounts WHERE account_id = OLD.account_id LOOP
            PERFORM portfolio_prune_orders(target);
            UPDATE portfolio_workspaces SET revision = revision + 1 WHERE workspace_id = target;
        END LOOP;
    ELSE
        PERFORM portfolio_prune_orders(OLD.workspace_id);
    END IF;
    RETURN NULL;
END;
$$;
DO $$
DECLARE target TEXT;
BEGIN
    FOREACH target IN ARRAY ARRAY['holdings_positions_current', 'portfolio_theses', 'portfolio_thesis_allocations', 'portfolio_workspace_accounts'] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS portfolio_delete_cleanup ON %I', target);
        EXECUTE format('CREATE TRIGGER portfolio_delete_cleanup AFTER DELETE ON %I FOR EACH ROW EXECUTE FUNCTION portfolio_cleanup_deleted()', target);
    END LOOP;
END;
$$;
CREATE OR REPLACE FUNCTION portfolio_cleanup_membership() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path = public, pg_temp AS $$
BEGIN
    IF TG_OP = 'DELETE' OR OLD.user_id <> NEW.user_id OR OLD.account_id <> NEW.account_id THEN
        DELETE FROM portfolio_workspace_accounts wa USING portfolio_workspaces w
        WHERE wa.workspace_id = w.workspace_id AND w.owner_user_id = OLD.user_id AND wa.account_id = OLD.account_id;
    END IF;
    RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS portfolio_membership_cleanup ON account_memberships;
CREATE TRIGGER portfolio_membership_cleanup AFTER UPDATE OR DELETE ON account_memberships
FOR EACH ROW EXECUTE FUNCTION portfolio_cleanup_membership();
CREATE OR REPLACE FUNCTION portfolio_cleanup_inactive_account() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path = public, pg_temp AS $$
BEGIN
    IF NOT NEW.is_active OR NEW.archived_at IS NOT NULL THEN
        DELETE FROM portfolio_workspace_accounts WHERE account_id = NEW.account_id;
    END IF;
    RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS portfolio_inactive_account_cleanup ON holdings_accounts;
CREATE TRIGGER portfolio_inactive_account_cleanup AFTER UPDATE ON holdings_accounts
FOR EACH ROW EXECUTE FUNCTION portfolio_cleanup_inactive_account();
CREATE OR REPLACE FUNCTION portfolio_validate_workspace_account() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path = public, pg_temp AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM portfolio_workspaces w
        JOIN account_memberships m ON m.user_id = w.owner_user_id AND m.account_id = NEW.account_id
        JOIN holdings_accounts a ON a.account_id = m.account_id AND a.is_active AND a.archived_at IS NULL
        WHERE w.workspace_id = NEW.workspace_id) THEN
        RAISE EXCEPTION 'Account is outside the workspace membership' USING ERRCODE = 'PTH02';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS portfolio_workspace_account_guard ON portfolio_workspace_accounts;
CREATE TRIGGER portfolio_workspace_account_guard BEFORE INSERT OR UPDATE ON portfolio_workspace_accounts
FOR EACH ROW EXECUTE FUNCTION portfolio_validate_workspace_account();
CREATE OR REPLACE FUNCTION portfolio_validate_order() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path = public, pg_temp AS $$
DECLARE item TEXT;
BEGIN
    IF jsonb_typeof(NEW.keys) <> 'array' OR jsonb_array_length(NEW.keys) > 10000 THEN
        RAISE EXCEPTION 'Invalid manual order' USING ERRCODE = 'PTH03';
    END IF;
    IF EXISTS (SELECT 1 FROM jsonb_array_elements(NEW.keys) AS k(value) WHERE jsonb_typeof(value) <> 'string')
       OR (SELECT count(*) <> count(DISTINCT value) FROM jsonb_array_elements_text(NEW.keys) AS k(value)) THEN
        RAISE EXCEPTION 'Manual order requires unique string keys' USING ERRCODE = 'PTH03';
    END IF;
    IF NEW.scope LIKE 'legs:%' AND NOT EXISTS (SELECT 1 FROM portfolio_theses WHERE workspace_id = NEW.workspace_id AND thesis_id::text = substring(NEW.scope FROM 6)) THEN
        RAISE EXCEPTION 'Unknown thesis order scope' USING ERRCODE = 'PTH03';
    END IF;
    FOR item IN SELECT value FROM jsonb_array_elements_text(NEW.keys) AS k(value) LOOP
        IF NEW.scope = 'theses' THEN
            IF EXISTS (SELECT 1 FROM portfolio_theses WHERE workspace_id = NEW.workspace_id AND thesis_id::text = item) THEN CONTINUE; END IF;
        ELSIF NEW.scope LIKE 'legs:%' THEN
            IF EXISTS (SELECT 1 FROM portfolio_thesis_allocations WHERE workspace_id = NEW.workspace_id AND thesis_id::text = substring(NEW.scope FROM 6) AND account_id || ':' || ric = item) THEN CONTINUE; END IF;
        ELSE
            IF EXISTS (SELECT 1 FROM holdings_positions_current p JOIN portfolio_workspace_accounts wa USING(account_id) WHERE wa.workspace_id = NEW.workspace_id AND p.account_id || ':' || p.ric = item) THEN CONTINUE; END IF;
        END IF;
        RAISE EXCEPTION 'Foreign manual order key' USING ERRCODE = 'PTH03';
    END LOOP;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS portfolio_order_guard ON portfolio_manual_orders;
CREATE TRIGGER portfolio_order_guard BEFORE INSERT OR UPDATE ON portfolio_manual_orders
FOR EACH ROW EXECUTE FUNCTION portfolio_validate_order();

INSERT INTO portfolio_schema_versions(version) VALUES (1) ON CONFLICT DO NOTHING;
COMMIT;
