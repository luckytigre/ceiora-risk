# Thesis production release proposal

Status: live release not authorized or executed. The isolated Neon rehearsal passed
and was cleaned up. Approved source `ea42999` was published and three images/Plan A
prepared, but locally accepted corrections at `da5b6c7` now supersede those artifacts.
CI has 25 failures all reproduced on original baseline; no green-CI claim or waiver.
See `../implementation/RELEASE_PREPARATION_EVIDENCE.md` before further action.
Record and approve the replacement source SHA before publication. Proposed source destination is
`https://github.com/luckytigre/ceiora-risk.git`, branch `main`; confirm the exact
reviewed commit in the publication approval.

## Verified existing deployment — 2026-10-23

Project `project-4e18de12-63a3-4206-aaa`, region `us-east4`, registry `ceiora-images`.
All service traffic is on the following ready revisions:

| Surface | Revision | Immutable rollback image |
| --- | --- | --- |
| Serve | ceiora-prod-serve-00037-w7v | us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images/serve@sha256:197c0eab04f12a4248b15ef901b4fd88641b362573fc6694f9d92f877f2d58e5 |
| Control | ceiora-prod-control-00041-whn | us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images/control@sha256:fd44f89fb59f25d71158bc051bd64ddf5b2a0b69133e0b91572538f4432e8aec |
| Frontend | ceiora-prod-frontend-00060-vtj | us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images/frontend@sha256:73057d777300d8894a071ebb2d618eea51b4f1bb684e85da57f0f197e9ed9036 |

All four jobs currently reference `control:c9b9e83`. Artifact Registry resolves that
configured tag to
`us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images/control@sha256:5257921ea4af1a78cac34df7215efc3a158acbeeaa7a105defb9ecaffffc75f8`.
Keep this job-config target distinct from the service's resolved image digest above;
do not assume mutable tags and revision digests are interchangeable. Re-resolve at
release time before changing jobs. Inventory drift invalidates observations: re-read
revisions, image digests and job refs immediately before planning/applying.

Frontend backend origins:

- Serve: `https://ceiora-prod-serve-i5znti5joq-uk.a.run.app`
- Control: `https://ceiora-prod-control-i5znti5joq-uk.a.run.app`
- Frontend Cloud Run: `https://ceiora-prod-frontend-i5znti5joq-uk.a.run.app`
- Keep the existing public `app.ceiora.com` / Firebase Hosting rewrite to
  `ceiora-prod-frontend` (`firebase.json`); no Hosting or DNS change is proposed.

Keep Neon auth, account enforcement, disabled legacy shared auth and Cloud Run
backend IAM authentication. Never copy the local test launcher or credentials.
The verified database is Neon project `dark-wildflower-28441812`, main branch
`br-autumn-fire-aifkc7z2`, database `neondb`, writer role `neondb_owner`.

## Proposed sequence after explicit release approval

1. Recheck clean source and remote-main divergence. Publish the reviewed source and
   build all three images, preserving the verified backend origins explicitly with
   `ENDPOINT_MODE=run_app`. Build scripts otherwise default to custom-domain mode.
   Normal build dependency downloads and image publication are part of this release
   action. Capture immutable digests; use them in the infrastructure plan.
2. Inspect existing Terraform inputs/state privately, initialize the pinned providers
   if needed. Prepare two separately reviewed saved plans. Plan A changes only
   serve/control/all four job image pins to the new digests and explicit off/empty/
   false thesis settings; retain the captured existing frontend image digest.
   Preserve origins, IAM, auth, Firebase, unrelated settings and the database secret.
   No provider upgrades or unrelated resource replacement. Include all four jobs
   through `local.control_image_ref`. Do not prepare/apply one plan with all three
   new image refs: that could advance the frontend before the migration.
3. Set thesis rollout explicitly off/empty/false. Deploy compatible serve/control
   and all writer-job images using Plan A before allocations can be created. Verify readiness
   and existing holdings/auth flows with flags off. Coordinate a quiet cutover:
   wait for any in-flight old writer jobs to finish, verify service traffic has no
   old writer revision, and confirm subsequent executions use the new job image.
   Do not launch or cancel model jobs merely to test image-reference changes.
4. Recheck read-only database prerequisites and every known writer role. Apply the
   reviewed bounded migration once through an explicit direct connection on the
   verified main branch, never on a guessed/default branch. Stop on timeout/error;
   do not retry automatically or change lock limits to force it through.
5. After migration succeeds, generate and review Plan B from current state:
   change only the frontend image digest, retaining the verified Plan A backend/job
   digests and flags. Apply Plan B and verify feature-off login, holdings, instrument
   Risk and model-neutral DASH, both cUSE/cPAR selectors, account permissions and
   backend IAM proxying on the real public origin. Inspect error/lock/conflict logs.
6. Keep owner activation off. Select and verify an exact canonical Neon subject and
   its memberships in a separate approved activation step. Shared auth remains off.
   Only then test durable thesis UI across fresh sessions and two clients.

Migration before compatible writer deployment is also technically possible, but
this proposal installs compatible writers first, while the schema is absent and
capability is disabled. Existing code probes for schema guards and remains usable.
All writes acquire the shared lane once the explicit migration commits.

## Rollback and stop conditions

Before creating saved allocations, restore captured service/job image digests if
feature-off smoke fails. Keep the additive schema. If any allocations have been
created, retain compatible backend writer images/guards and disable thesis rollout;
rolling back those writers could lose their pre-read writer lock. Frontend rollback
can proceed independently. Never drop saved tables or disable triggers as rollback.

Stop if target, permissions, source, origin or plan differs; if migration times out;
or if authentication, account access, holdings/import behavior or error rates regress.
Keep the live change disabled until the discrepancy is understood.

Required approvals still outstanding: reviewed source/image publication, build-time
package downloads/provider initialization, production deployment and live migration.
Owner activation is a subsequent separate approval. Rehearsal authorization covered
only credentialed verification, the temporary branch, tests and its cleanup.

Evidence: `docs/implementation/NEON_THESIS_REHEARSAL_EVIDENCE.md` and
`docs/implementation/ROLLOUT_PREP_EVIDENCE.md`.
