-- Final destructive Neon cleanup after registry-first cutover.
-- This removes any lingering live security_master relation and related legacy artifacts.
-- Local SQLite compatibility rehearsal is handled separately by
-- backend/scripts/demote_security_master_to_compat_view.py.

DO $cleanup$
DECLARE
    master_relkind "char";
    legacy_relkind "char";
BEGIN
    SELECT c.relkind
    INTO master_relkind
    FROM pg_class c
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relname = 'security_master'
    LIMIT 1;

    IF master_relkind = 'v' THEN
        EXECUTE 'DROP VIEW public.security_master';
    ELSIF master_relkind IN ('r', 'p') THEN
        EXECUTE 'DROP TABLE public.security_master';
    ELSIF master_relkind = 'm' THEN
        EXECUTE 'DROP MATERIALIZED VIEW public.security_master';
    END IF;

    SELECT c.relkind
    INTO legacy_relkind
    FROM pg_class c
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relname = 'security_master_legacy'
    LIMIT 1;

    IF legacy_relkind = 'v' THEN
        EXECUTE 'DROP VIEW public.security_master_legacy';
    ELSIF legacy_relkind IN ('r', 'p') THEN
        EXECUTE 'DROP TABLE public.security_master_legacy';
    ELSIF legacy_relkind = 'm' THEN
        EXECUTE 'DROP MATERIALIZED VIEW public.security_master_legacy';
    END IF;
END
$cleanup$;

DROP INDEX IF EXISTS public.idx_security_master_ticker;
DROP INDEX IF EXISTS public.idx_security_master_permid;
DROP INDEX IF EXISTS public.idx_security_master_sid;
