# Release preparation — 2026-10-23

Status: publication/build/plan preparation completed for approved source `ea42999`.
New locally accepted boundary fixes at `da5b6c7` supersede that source for rollout.
No Terraform apply, live migration, deployment or owner activation occurred.

## Published approved source and images

`ea42999968615f8397b125265f25f02c97394fe9` was fast-forwarded to
`https://github.com/luckytigre/ceiora-risk.git`, `main`, from `23c6270`. The repository
has one CI workflow and no automatic deployment workflow.

Build source was a clean tracked `git archive` of that exact commit, SHA256
`c74ec2d5a2e2cb0ee5e48a4973997ea7fc81b940de5673b73d051e6c0ee6e081`.
All three images built and published for linux/amd64 using explicit `run_app` mode
and the verified serve origin. CSS, contrast, type and production Next build gates
passed. Registry tags are `ea42999` under
`us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images`:

| Image | Published manifest-list digest |
| --- | --- |
| frontend | sha256:ecbbd983c0a764eb3078f647043561f34ca9badb532688693a7b7a8c1d4550de |
| serve | sha256:af6b7983ea2286e88c65cd954af38422b529bd319a449d39e3c0f48ca85b6178 |
| control | sha256:3e0299636f9f0363dd9d8c2323531e0ab49fbd79f035a392b3cedf69b3490609 |

Manifest contents were inspected: each has its linux/amd64 image plus attestation.
The immutable manifest-list digest differs from its platform-specific image digest;
these are separate references, not evidence of an unexpected source change.
These published artifacts are now superseded and must not be deployed.

## Saved Terraform plan

Terraform initialized outside Dropbox using the existing GCS backend and readonly
lockfile. Locked providers: Google/Google-beta 6.50.0 and Cloudflare 5.18.0. Existing
private inputs and plan/state JSON stay in a private local directory; no secrets
were committed or printed. The production input file was not edited.

The baseline plan added only the three disabled/empty thesis settings on serve.
Plan A then contained exactly six in-place updates:

- Serve and control image digests.
- All four jobs' control image digest.
- Serve thesis enabled=false, owner subjects empty, shared auth=false.

All existing environment values, auth, secret refs, IAM and origins were unchanged;
frontend was no-op. No additions, deletions or replacements were planned. Retaining
its existing `frontend:2a704b6` config reference preserves the running frontend;
changing that reference to an equivalent digest would itself create a revision.

The plan scope passed adversarial review, but the artifact is held/superseded by
local fixes. It was renamed to prevent confusion with a ready release:
`/Users/shaun/tmp/ceiora-release-ea42999-superseded.tfplan`.
Sanitized review is `/Users/shaun/tmp/ceiora-release-ea42999-plan-a-review.json`;
manifest is `/Users/shaun/tmp/ceiora-release-ea42999-manifest.json`.
No apply occurred. Plan B is only a variable template; a genuine frontend-only
saved plan must be generated from current state after Plan A and migration succeed.

## CI failure audit

[GitHub run 40005067618](https://github.com/luckytigre/ceiora-risk/actions/runs/40005067618):
frontend passed; full backend reported **25 failed, 982 passed, 20 skipped**.
The adversarial reviewer executed those exact 25 failed node IDs against a pristine
tracked `23c6270` snapshot, with network disabled and the newly built dependencies.
**All 25 also failed on that baseline.** This is disclosed baseline debt, not green
CI or an automatic waiver for deployment.

Categories: golden payload shapes (3), FastAPI lazy-router introspection (4),
architecture/ownership contracts (5), cPAR fixtures/policy prerequisites (6),
diagnostics table inventory (1), parity fixtures/status (2), orchestration stable
core/cache prerequisites (2), lineage old-symbol mock (1), universe-search mock (1).
Architecture, policy and parity failures require proper review; do not loosen
runtime fail-closed behavior or allowlists merely to pass tests.

Local logs: `/Users/shaun/tmp/ceiora-release-ea42999-ci-backend.log` and
`/Users/shaun/tmp/ceiora-baseline-audit-25.log`.

## Accepted local corrections

Existing failing architecture nodes included additional port-introduced offenders.
Local code commit `da5b6c7` corrects these without changing test allowlists:

- Canonical principal/connection handling moved from the new thesis router to its
  service facade. HTTP error mapping, connection cleanup and pre-transaction checks
  remain intact. Holdings/what-if routes use the same guard exception via service.
- Shared error UI now uses a neutral `RequestErrorState` with sign-in/retry and
  sanitized diagnostics. It has no model hooks or model-owned maintenance actions.
  The cUSE DASH exposure chart uses its explicit cUSE wrapper.
- The PostgreSQL operator-schema fixture now tolerates repeated runs; its assertion
  that migration objects must not land in the private schema remains unchanged.

Verified on the new serve image's actual dependencies:

- 35 thesis/ownership/what-if route tests passed.
- 49 holdings/service/cloud tests passed.
- 19 real PostgreSQL tests passed (8.14s); synthetic browser database reseeded after.
- Production frontend build, UI error contract and full DASH browser smoke passed,
  including model isolation, auth/URL persistence, failure recovery and feature-off
  Positions/cUSE/cPAR pages. Smoke used localhost for Secure test cookies.
- Route data-import offender set exactly matches baseline's nine existing files;
  no new offender remains. Model-family ownership check now passes.
- React review: one unconditional auth-context hook; no extra model requests,
  effect chains or mutable request state; accessible alert, details and retry button.

The reviewer accepted these corrections and verification. The full backend suite
has not been rerun on unpublished corrections; prior baseline failures remain open.
The isolated port3104 browser process was stopped; original previews were retained.

## Remaining gates

Obtain approval for the exact replacement source SHA, image publication and fresh
Plan A generation. Do not reuse the superseded images/plan. Live migration and both
Terraform applies require later review/approval; owner activation remains separate.
