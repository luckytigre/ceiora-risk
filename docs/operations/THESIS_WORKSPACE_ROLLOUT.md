# Portfolio thesis workspace rollout

Status: local integration only. Live migration, merge and deployment are separate actions.

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

1. Verify current remote main and reconcile the integration branch before merge.
2. Retain original cloud auth/session/proxy settings. Do not copy local test launch
   environments, fixture credentials, test DB URLs, or test harness entry points.
3. Use an isolated Neon branch with existing holdings and app-auth schemas to
   validate the migration with production-like schema/role privileges. Local
   PostgreSQL tests do not verify Neon permissions or deployed IAM topology.
4. New migration requires the existing NEON_HOLDINGS_SCHEMA.sql and
   NEON_APP_AUTH_SCHEMA.sql. Runtime role needs normal table CRUD and function
   execution privileges; migrate with the schema owner's direct/unpooled connection.
   No schema changes run from serving reads. Prepare both code and migration; when
   enabling for the first time, keep feature flags off until both are installed.

Apply explicitly with the selected isolated branch DSN, never an inferred default:

```sh
psql "$CEIORA_THESIS_MIGRATION_DSN" -v ON_ERROR_STOP=1 \
  -f docs/reference/migrations/neon/NEON_PORTFOLIO_THESES_SCHEMA.sql
```

The migration is transactional and re-runnable. Validate on a populated branch and
perform CRUD/precision/account-isolation/concurrency tests before any live execution.
Production credentials and live schema/deployment actions require explicit approval.

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
- Frontend checks capability only inside protected authenticated pages, keys its
  provider by identity/account set/workspace, and discards obsolete or mismatched
  responses. Disabled/unavailable capability retains instrument views.

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

See ../implementation/P2_P4_EVIDENCE.md and ../implementation/ADVERSARIAL_REVIEW.md.
The disposable local browser harness is backend.tests.thesis_http_app, explicitly
gated by CEIORA_THESIS_TEST_MODE and a local-only read fixture origin. It rejects
non-thesis writes and must not be used as a deployed app entry point.
