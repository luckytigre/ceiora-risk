# Portfolio thesis workspace rollout

Status: UI port merged into original local main at `776002a`; no remote push or live rollout.
Rollout preparation from `feat/thesis-rollout-prep` is locally accepted after
19 PostgreSQL and 51 cloud/thesis contracts; see the rollout evidence for merge details.

## Runtime ownership

The shared Positions/DASH and cUSE/cPAR Risk thesis views classify real holdings by
account + RIC. Theses and manual order belong to an authenticated owner workspace.
Risk instrument views keep their existing model-owned aggregates. Risk thesis views
load account holdings separately; they do not allocate aggregate risk contributions
back to account legs. Missing/mismatched account-level risk mixes remain unavailable.
Prices and risk metadata remain last-served snapshots, separate from ledger quantities.

The serve surface owns session-authenticated /api/portfolio/theses (+ capability,
create/rename/delete), /api/portfolio/thesis-allocations/position and
/api/portfolio/manual-order. Control surface does not expose these routes. The
frontend portfolio proxy permits only the supported paths/methods. Classification
changes never dispatch a model refresh or mutate holdings.

## Deployment prerequisites

1. Verify current remote main before any push. Last read-only check: remote main
   `23c6270`; original local main `776002a`. Capture the exact reviewed release SHA.
2. Retain original cloud auth/session/proxy settings. Do not copy local test launch
   environments, fixture credentials, test DB URLs, or test harness entry points.
3. Use an isolated Neon branch with existing holdings and app-auth schemas to
   validate the migration with production-like schema/role privileges. Local
   PostgreSQL tests do not verify Neon permissions or deployed IAM topology.
4. Identify every serve/control/bootstrap/job/CLI database writer role. The new
   triggers are SECURITY INVOKER and affect existing writes immediately, even with
   the feature disabled. Do not assume the migration owner is the runtime role.
   Inspect inherited/default ACLs and rehearse legacy writes as each intended role.
5. New migration requires the existing NEON_HOLDINGS_SCHEMA.sql and
   NEON_APP_AUTH_SCHEMA.sql. Runtime role needs normal table CRUD and function
   execution privileges; migrate with the schema owner's direct/unpooled connection.
   No schema changes run from serving reads. Prepare both code and migration; when
   enabling for the first time, keep feature flags off until both are installed.

Run the read-only `NEON_PORTFOLIO_THESES_PREFLIGHT.sql` on an explicitly selected
connection before and after migration. It reports catalogs, aggregate nonfinite
holdings counts, actual table/function privileges and long-transaction counts. It
never selects subject, email, account or holding-detail rows. Review NUMERIC(20,6),
validated parent PK/FKs, RLS, trigger enablement, READ COMMITTED writers and ACLs.
The audit reports hidden activity rows; zero long-transaction counts cannot prove
absence of blockers when the inspection role lacks statistics visibility. A successful
audit query is evidence to review, not automatic rollout permission.

For separate writer roles, supply an explicitly reviewed JSON array through the
session setting `ceiora.portfolio_writer_roles` before running the migration.
Example structure for an approved direct connection (role names are placeholders):

```sql
SELECT set_config('ceiora.portfolio_writer_roles', '["reviewed_writer_role"]', false);
-- Same session: execute NEON_PORTFOLIO_THESES_PREFLIGHT.sql, then reviewed migration.
```

The migration validates existing non-PUBLIC role names and grants schema USAGE,
five workspace tables' CRUD, schema-version SELECT and explicit function EXECUTE
inside its transaction. No new base-table grants or blanket default ACL changes
are made. Existing/inherited ACLs, including default PUBLIC function execution,
are retained; this does not claim database-level owner isolation. The application's
trusted backend role enforces owner authorization. An empty role array is allowed
only after proving every actual writer is the owner or already authorized.

Migration schema resolution is pinned to public. Each lock wait is bounded to five
seconds and each statement to sixty seconds; timeout or invalid roles rolls back
the transaction. Investigate blockers before retrying. There is no automatic retry
or serving-time DDL. Rehearse existing holding edits/imports/what-if, membership
revocation/account archive, bootstrap and concurrent classification transactions as
restricted writer roles, plus reapplication, before requesting live execution.

## Target verification and release sequence

Read-only connector metadata found candidate Neon project `barra-dashboard`
(`dark-wildflower-28441812`), branch `main` (`br-autumn-fire-aifkc7z2`), database
`neondb`, PostgreSQL 17. The base holdings/auth tables exist; thesis tables do not.
The connector inspected as `neondb_owner`. This has NOT been correlated with the
deployed Cloud Run database secret and does NOT establish actual runtime roles.
No branch or cloud resource has been created or changed.

1. With separately approved credential access, compare the sanitized deployed
   endpoint/database/role to this candidate; never print the DSN or secret values.
   Inventory current Cloud Run origins, image digests, job image pins and settings.
2. Obtain approval for an isolated Neon rehearsal branch and its cleanup. A normal
   branch contains production data; a schema-only branch requires synthetic seeds
   and cannot prove existing-data compatibility. Confirm the intended choice and
   exact parent before creating it; use expiry when supported.
3. Run preflight, migration with reviewed role grants, and complete restricted-role
   legacy-write/concurrency rehearsal on that branch. Obtain adversarial acceptance.
4. Prepare a concrete production plan preserving deployed authentication, IAM,
   `run_app` origins and Firebase Hosting configuration. Existing image build scripts
   default to custom-domain mode: pass verified current origins/mode explicitly.
   Include serve, control and all jobs that write holdings/auth; their writer-lane
   changes must be deployed before live allocations can be created. Capture all
   current image pins for rollback; keep all three thesis flags disabled.
5. Request explicit approval for the reviewed push/build/deployment and live schema
   operation. Apply the bounded migration with all runtime grants, deploy compatible
   writers and frontend, then verify feature-off existing flows. Schema guards become
   active at migration commit; feature-off does not postpone permissions requirements.
6. Request/obtain approval for the exact audited owner activation. Terraform carries
   the three serve-only settings below, default off; no frontend feature secret is
   needed. Retain existing auth gates and monitor errors, lock waits and conflicts.

Local Terraform formatting/isolated contract checks do not replace a credentialed
production plan. Do not run init/apply or reuse the example topology as live inputs.

## Owner rollout gates

- PORTFOLIO_THESES_ENABLED defaults false.
- PORTFOLIO_THESES_OWNER_SUBJECTS is an explicit comma-separated allowlist of trusted
  app-auth subject IDs; empty means no owner is enabled. Use subject IDs, not email
  display labels. Memberships and existing app_users identity remain mandatory.
- Neon sessions retain the canonical live identity/allowlist checks before workspace
  access. Do not bootstrap users or commit authentication provisioning inside a
  classification transaction.
- PORTFOLIO_THESES_ALLOW_SHARED_AUTH defaults false. Only enable shared auth after
  auditing existing app_users mappings for the exact shared subject; the current
  schema does not distinguish auth providers by subject. Existing
  APP_SHARED_AUTH_ACCEPT_LEGACY must also allow shared access. No automatic shared
  identity creation is performed.
- Frontend checks capability only inside protected authenticated pages. Its stable
  provider scopes/reset thesis state by identity, account set and workspace, masking
  and discarding obsolete responses. A real principal change reloads the document
  to clear the global SWR cache; same-principal refresh preserves the page.
  Disabled/unavailable capability retains instrument views.

Enable for one audited owner first. Verify fresh/restarted sessions, two-client
conflicts, both model selectors, saved ordering, long/short partial moves, and all
holding edit/import/apply routes before broader enablement.

## Transaction and data policies

- A transaction-scoped global advisory writer lock is intentionally used for this
  small application. It serializes holdings/classification/membership writes. Shared
  low-level holdings entry points acquire it before read-modify-write; statement
  triggers also acquire it before DML row locks. Keep future writers on that lane.
- READ COMMITTED is required for these writes. Other isolation levels and TRUNCATE
  are rejected rather than relying on stale validation snapshots. Ordinary SQL
  triggers do not protect against privileged DROP/ALTER or disabled-trigger writes.
- Each owner workspace may independently categorize a shared holding up to its
  signed quantity. Limits are checked per workspace, not summed across owners.
- All holding update paths preserve allocation limits and direction. Removal of a
  holding deletes its allocations and prunes order keys in the same transaction.
- What-if application upserts the surviving account/RIC instead of deleting and
  recreating it. Consolidation away from an allocated RIC is rejected until its
  allocations are adjusted. Multi-account changes remain one atomic transaction.
- Membership revocation/account archive removes that account's workspace mapping,
  allocations and corresponding order keys atomically. Other permitted accounts and
  thesis names remain. Re-granting membership does not restore removed allocations.
- Existing holdings precision is numeric(20,6); backend allocation arithmetic uses
  Decimal and rejects nonfinite/over-precision values. The Number-based frontend
  editor rejects quantities outside its tested range (one billion shares) and more
  than six decimal places instead of silently rounding. Backend precision remains
  unchanged; unsupported editor values retain instrument access.

## Rollback

Disable thesis owner rollout/capability to return to instrument views. Retain the
additive tables and saved data. Schema guards remain active to protect existing
allocations even with the UI disabled: a holding reduction/reversal that invalidates
saved allocations will still be rejected. Re-enable the compatible owner editor to
adjust classifications before such a holdings change. Do not drop tables or bypass
triggers as an application rollback shortcut. DASH model selection can roll back
independently of saved classification data.

## Local evidence

See ../implementation/P5_MERGE_HANDOFF.md and ../implementation/ADVERSARIAL_REVIEW.md.
Rollout-preparation verification is recorded in ../implementation/ROLLOUT_PREP_EVIDENCE.md.
The disposable local browser harness is backend.tests.thesis_http_app, explicitly
gated by CEIORA_THESIS_TEST_MODE and a local-only read fixture origin. It rejects
non-thesis writes and must not be used as a deployed app entry point.
