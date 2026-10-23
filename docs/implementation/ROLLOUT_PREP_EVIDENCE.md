# Thesis rollout preparation — 2026-10-23

Scope: local preparation on `feat/thesis-rollout-prep`, based on original local
main `776002a`. No push, deployment, credential retrieval, live migration or Neon
branch creation was performed. The earlier merged UI and P5 evidence are unchanged.

## Changes

- Terraform exposes the three thesis settings only on serve, all default off/empty.
  Serve lifecycle preconditions reject activation without owners and shared thesis
  auth without the existing shared-auth gate. Subject serialization trims and
  deduplicates while retaining case; commas, line breaks and blank entries fail.
- Migration pins public schema resolution and bounds lock/statement waits. Reviewed
  writer-role JSON can grant five-table CRUD, version SELECT, schema USAGE and
  explicit function execution atomically with schema installation. Invalid role
  entries abort the entire transaction. Existing PUBLIC/inherited ACLs remain.
- A read-only preflight reports prerequisite precision/constraints, table/function
  ACLs, enabled guards and aggregate transaction/holdings checks. It includes hidden
  activity-row counts to expose limited statistics visibility.
- The runbook identifies the unverified candidate Neon target and separates target
  verification, temporary branch rehearsal, release, live migration and activation.
  The stable provider/principal-change cache description now matches implementation.

## Passed checks

1. Terraform fmt and git diff --check.
2. Eight actual isolated Terraform plans, using only the builtin terraform_data
   provider, with no cloud backend/provider download: defaults, empty activation,
   explicit owner activation, comma/newline/blank subjects, shared gate closed/open.
   Deduplication retained the case-sensitive `TestOwner` subject.
3. Direct PostgreSQL 17 rehearsal on a second disposable local database,
   `ceiora_rollout_test`, inside the already approved localhost-only test container:
   - Migration under a private-first search_path installed tables in public.
   - Explicit restricted writer worked with PUBLIC function EXECUTE revoked.
   - Workspace/allocation writes, legacy holdings and auth writes, and membership
     revoke cleanup succeeded; schema-version UPDATE remained unavailable.
   - Read-only catalog/role preflight executed successfully.
   - Object, numeric-element, PUBLIC and missing-role configurations were rejected.
   - A valid-role grant followed by an invalid role rolled back the earlier grant.
   - A second client holding the writer lane caused the configured five-second lock
     timeout; migration reapplication succeeded after that client released its lock.
   Reproducible local driver: `/Users/shaun/tmp/verify-ceiora-rollout-sql.py`.
4. The added pytest cases parse successfully. The direct SQL rehearsal above
   exercised their database behaviors; it does not constitute a pytest-suite pass.

## Runtime limitation

The attempted full PostgreSQL pytest rerun could not start: new Docker runs and
new Python executions/copies in the existing amd64 backend container stalled.
Native PostgreSQL container commands remained responsive. The stalled client
commands were terminated without restarting the user's preview or Docker service.
No dependencies or system configuration were changed. The new migration was
validated directly with psql as described above; prior 13-test PostgreSQL evidence
predates these rollout-preparation edits. Rerun the focused pytest suite once the
backend test runtime is healthy, before release acceptance. No frontend files
changed, so the already accepted P5 browser/build evidence was not repeated.

## Adversarial review

`/root/adversarial_review` reviewed the final Terraform, migration, preflight and
rollout sequence. Static preparation accepted with no safety blocker. Its required
conditions remain: actual target proof, all real writer roles, isolated Neon branch
rehearsal and explicit live approvals. The statistics-visibility caveat was fixed.
This is preparation acceptance, not production rollout acceptance.
