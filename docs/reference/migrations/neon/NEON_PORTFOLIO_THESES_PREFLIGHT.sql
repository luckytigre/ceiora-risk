-- Read-only catalog/aggregate audit. No user, account, RIC or subject rows.
-- Set ceiora.portfolio_writer_roles to the reviewed JSON role list on this session.
-- Run before migration and again afterward; this reports evidence, not an automatic approval.
BEGIN READ ONLY;
SET LOCAL search_path = public, pg_temp;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
SELECT current_database() AS database_name, current_user AS inspected_role,
       current_setting('server_version_num') AS server_version_num,
       current_setting('default_transaction_isolation') AS default_isolation,
       current_setting('transaction_isolation') AS audit_isolation;

SELECT nspname AS schema_name, pg_get_userbyid(nspowner) AS owner, nspacl AS acl
FROM pg_namespace WHERE nspname = 'public';

SELECT c.relname, pg_get_userbyid(c.relowner) AS owner, c.relrowsecurity,
       c.relforcerowsecurity, c.relacl
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
  AND (c.relname IN ('holdings_accounts', 'holdings_positions_current', 'app_users', 'account_memberships')
       OR c.relname LIKE 'portfolio\_%' ESCAPE '\') ORDER BY c.relname;

SELECT table_name, column_name, data_type, numeric_precision, numeric_scale, is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND ((table_name = 'holdings_positions_current' AND column_name IN ('account_id', 'ric', 'quantity'))
       OR (table_name = 'app_users' AND column_name IN ('user_id', 'auth_user_id'))
       OR (table_name = 'account_memberships' AND column_name IN ('account_id', 'user_id')))
ORDER BY table_name, ordinal_position;

SELECT conrelid::regclass AS relation, conname, contype, convalidated,
       pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE conrelid IN ('public.holdings_accounts'::regclass, 'public.holdings_positions_current'::regclass,
                  'public.app_users'::regclass, 'public.account_memberships'::regclass)
ORDER BY conrelid::regclass::text, conname;

SELECT count(*) AS holdings_rows,
       count(*) FILTER (WHERE quantity::text IN ('NaN', 'Infinity', '-Infinity')) AS nonfinite_holdings
FROM public.holdings_positions_current;

WITH requested AS (
    SELECT jsonb_array_elements_text(COALESCE(NULLIF(current_setting('ceiora.portfolio_writer_roles', true), ''), '[]')::jsonb) AS role_name
    UNION SELECT current_user
), relations AS (
    SELECT c.oid, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
      AND (c.relname IN ('holdings_accounts', 'holdings_positions_current', 'app_users', 'account_memberships')
           OR c.relname LIKE 'portfolio\_%' ESCAPE '\')
)
SELECT requested.role_name, r.rolname IS NOT NULL AS role_exists, relations.relname,
       CASE WHEN r.oid IS NOT NULL THEN has_schema_privilege(r.oid, 'public', 'USAGE') END AS schema_usage,
       CASE WHEN r.oid IS NOT NULL THEN has_table_privilege(r.oid, relations.oid, 'SELECT') END AS can_select,
       CASE WHEN r.oid IS NOT NULL THEN has_table_privilege(r.oid, relations.oid, 'INSERT') END AS can_insert,
       CASE WHEN r.oid IS NOT NULL THEN has_table_privilege(r.oid, relations.oid, 'UPDATE') END AS can_update,
       CASE WHEN r.oid IS NOT NULL THEN has_table_privilege(r.oid, relations.oid, 'DELETE') END AS can_delete
FROM requested LEFT JOIN pg_roles r ON r.rolname = requested.role_name CROSS JOIN relations
ORDER BY requested.role_name, relations.relname;

SELECT pg_get_userbyid(d.defaclrole) AS creating_role, n.nspname, d.defaclobjtype, d.defaclacl
FROM pg_default_acl d LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace;
SELECT c.relname, t.tgname, t.tgenabled, p.proname, p.prosecdef, p.proconfig
FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
JOIN pg_proc p ON p.oid = t.tgfoid JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE NOT t.tgisinternal AND n.nspname = 'public' AND p.proname LIKE 'portfolio\_%' ESCAPE '\'
ORDER BY c.relname, t.tgname;
WITH requested AS (
    SELECT jsonb_array_elements_text(COALESCE(NULLIF(current_setting('ceiora.portfolio_writer_roles', true), ''), '[]')::jsonb) AS role_name
    UNION SELECT current_user
)
SELECT requested.role_name, p.oid::regprocedure AS function,
       CASE WHEN r.oid IS NOT NULL THEN has_function_privilege(r.oid, p.oid, 'EXECUTE') END AS can_execute
FROM requested LEFT JOIN pg_roles r ON r.rolname = requested.role_name
CROSS JOIN pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'public' AND p.proname LIKE 'portfolio\_%' ESCAPE '\'
ORDER BY requested.role_name, p.proname;
SELECT count(*) FILTER (WHERE state IS NULL) AS activity_rows_with_hidden_state,
       count(*) FILTER (WHERE state = 'idle in transaction') AS idle_transactions,
       count(*) FILTER (WHERE xact_start < now() - interval '60 seconds') AS transactions_over_60s
FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid();
ROLLBACK;
