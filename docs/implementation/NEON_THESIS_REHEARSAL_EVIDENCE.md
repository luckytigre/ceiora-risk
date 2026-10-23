# Neon thesis migration rehearsal — 2026-10-23

Result: rehearsal passed; temporary branch and endpoint deleted and their absence
verified. No live schema change, push, image publication, deployment or owner
activation occurred. Application code tested: original local main `5451413`.

## Authorization and verified target

Shaun answered “go on” directly to the request to retrieve deployed credentials,
verify the target, then create/test/delete an isolated branch containing production
data. This authorizes that rehearsal and cleanup, not live rollout.

GCP project `project-4e18de12-63a3-4206-aaa`, region `us-east4`:

- Serve `ceiora-prod-serve` and control `ceiora-prod-control`, plus all four jobs
  (`cold-core`, `core-weekly`, `cpar-build`, `serve-refresh`), reference Secret Manager
  `ceiora-prod-neon-database-url`, version selector `latest`.
- Only enabled database secret version is 1, created 2026-03-23. The inspected serve
  and control revisions were created 2026-08-24 and each receives 100% of traffic.
- Credential value was retrieved into memory and parsed without printing it.
  Its host is `ep-ancient-mode-ai9rw5ae.c-4.us-east-1.aws.neon.tech`, database `neondb`,
  role `neondb_owner`. Neon compute metadata maps that exact host to project
  `barra-dashboard` / `dark-wildflower-28441812`, parent `br-autumn-fire-aifkc7z2`.
- The actual configured writer role owns all four holdings/auth parent tables and
  has CRUD privileges. Their RLS flags are false. Default isolation is READ COMMITTED;
  holdings quantity is NUMERIC(20,6), with 62 rows and zero nonfinite quantities.
  Required parent constraints are validated. No thesis schema existed before work.
- The Neon CLI was unauthenticated; its probe was stopped. The connected Neon
  plugin supplied branch/catalog operations without changing local app env files.

These observations establish configured Cloud Run service/job writers, not the
credentials of arbitrary external operator scripts. Audit any additional writer
before live migration; do not assume it uses the same role.

## Isolated branch and migration

- Child: `br-shy-dew-aia1sv9d`, name `ceiora-thesis-rehearsal-20261023`.
- Exact parent: `br-autumn-fire-aifkc7z2`, parent LSN `13/75AC4538`.
- Compute: `ep-restless-queen-ai7wjxsh`, direct host
  `ep-restless-queen-ai7wjxsh.c-4.us-east-1.aws.neon.tech`, 0.25–1 CU, 300s suspend.
- Created at 14:54:56 UTC and removed during this session after testing.
- Full read-only preflight ran before and after migration. The reviewed migration
  was applied to this explicit child in one transaction; native direct-connection
  reapplication also succeeded. Version 1 appears once; all 32 guards are enabled.
- Runtime owner needs no additional grants. Separate restricted-role and malformed
  grant-list behavior remains supported by the 19-test local PostgreSQL suite.

## Checks actually repeated on Neon

Synthetic records were added only on the child alongside the copied data. No real
subject/email/holding detail rows or credentials were printed. The native harness
required the exact child direct host, database and role before connecting.

1. SQL checks passed for six-decimal signed allocations, independent owners sharing
   a holding, foreign-membership rejection, over-allocation/wrong-sign rejection,
   and invalid holding shrink/rekey rollback.
2. Current Python `portfolio_thesis_store` passed workspace resolution, thesis
   create/rename/delete, manual order and fresh-connection persistence.
3. Exact allocated holding rekey was rejected by FK `23503` (or custom `PTH03` is
   also valid); the entire workspace state, including allocation/revision, stayed
   unchanged. An unrelated FK error is not accepted as proof.
4. Actual `run_whatif_apply` preserved surviving account/RIC allocations. An invalid
   later account change rolled back the earlier short-account change atomically.
5. Actual `apply_holdings_import` passed `upsert_absolute`, `increment_delta` and
   `replace_account`, and rejected invalid reductions without partial writes.
6. A real second Python connection held the writer lock, waited, then reduced the
   holding. The waiting allocation writer saw the committed lower quantity and
   raised a conflict. This was a two-session race, not a sequential mock.
7. Stale revision, thesis/manual-order deletion, membership revocation, account
   archive and holding deletion all preserved/pruned classifications as intended.
8. Copied original holdings count remained 62. Other child client transactions
   were zero before deletion.

The native backend ran from the tracked-source snapshot using the already cached
Python 3.12 image and existing dependencies. The only service dependency stubs were
synthetic security-registry lookup and external refresh/dirty-recording dispatch.
Database connections, persistence, imports, what-if transactions and concurrency
were real Neon operations. The fixture initially omitted ticker metadata and then
returned an incorrect canonical short ticker; those harness-only errors were fixed
before the successful full run. No product implementation was changed.

Local evidence is separate: 19 PostgreSQL and 51 cloud/thesis tests passed before
this rehearsal. Neon checks above are not claimed as an additional pytest count.
This rehearsal does not replace deployed-session/auth smoke tests after release.

## Cleanup and parent verification

All native sessions closed; no other child transaction remained. The child-only
synthetic fixtures were intentionally retained until the authorized branch deletion.
The exact child was deleted through the Neon plugin; subsequent branch and endpoint
listings showed only the original parent and its original compute.

A final explicitly read-only parent transaction verified:

- Thesis and schema-version tables absent; zero portfolio guard triggers.
- 62 holdings, 7 app users and 7 memberships, matching the original aggregate counts.
- Zero synthetic rehearsal holdings.

The temporary credential was supplied through no-echo stdin, never a command-line
argument, repository env file or credential file. Its in-session tool result was
cleared after branch deletion. See `docs/operations/THESIS_PRODUCTION_RELEASE_PLAN.md`
for the next reviewable step and exact rollback targets.

## Adversarial acceptance

The reviewer accepted the actual Neon rehearsal, cleanup and parent invariance.
The release proposal now requires two separately reviewed Terraform plans so the
frontend cannot advance during backend/job rollout before migration. The exact
source commit and repository/ref must be named in publication approval. Rehearsal
acceptance does not authorize those release actions.
