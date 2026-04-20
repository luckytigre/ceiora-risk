# Full Cloud Compute Cutover Plan (LSEG Ingest Local-Only)

Date: 2026-04-14  
Owner: Platform / Risk Ops  
Status: Active temporary execution plan; Phase 4 is in progress, with entry gate, Day 1, and rollback drill complete

## 1. Objective

Execute the repo's target operating model:

- Keep **direct LSEG ingest local** (`APP_RUNTIME_ROLE=local-ingest`).
- Run all cUSE/cPAR **compute lanes in cloud jobs** (`APP_RUNTIME_ROLE=cloud-job`).
- Keep serving/control in cloud (`APP_RUNTIME_ROLE=cloud-serve`) and Neon-authoritative.
- Eliminate day-to-day dependency on local SQLite for compute/serving paths outside ingest + archive + diagnostics.

Primary references:

- `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/operations/OPERATIONS_PLAYBOOK.md`
- `docs/operations/CLOUD_NATIVE_RUNBOOK.md`
- `docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/operations/CPAR_OPERATIONS_PLAYBOOK.md`

## 2. Scope and Non-Goals

In scope:

- cUSE compute cutover (`core-weekly`, `cold-core`, `serve-refresh` dispatch and verification).
- cPAR compute cutover (`cpar-weekly` / `cpar-package-date` through cloud-job path).
- Data-authority hardening: local ingest -> Neon sync -> cloud compute.
- Reliability hardening for known blockers from recent runs.

Out of scope:

- Replacing local LSEG ingest with cloud ingest.
- New factor methodology changes.
- UI redesign; only operational controls/visibility changes as needed for cutover.

## 3. Current Baseline (as of this plan)

- `run_app` no-edge production is live.
- Cloud Run jobs exist for `serve-refresh`, `core-weekly`, `cold-core`, `cpar-build`.
- Job headroom currently raised to `8 CPU / 32Gi` for all four jobs.
- cUSE publish path is functioning with corrected SPY/QQQ projected-returns mapping.
- Known blockers remain:
  - local PIT freshness lag vs Neon blocks `source_sync` fail-closed.
  - local `backend/runtime/cache.db` corruption on operator host.
  - broad parity reports mismatch and needs controlled triage.
  - cPAR latest date blocked by missing proxy anchor-week prices.

## 4. Cutover Principles (must remain true)

- `source_sync` stays local-ingest-owned.
- Cloud compute lanes must never silently fall back to local in-process execution.
- Fail-closed semantics remain in place for stale/missing core package, Neon outages, and cPAR package readiness.
- All cutover steps must be reversible with documented rollback commands.

## 5. Step-by-Step Execution Plan

## Phase 0 - Change Control and Safety Rails

0A. Freeze automation before manual execution:

- Pause all refresh-related Cloud Scheduler jobs.
- Confirm no in-flight Cloud Run executions for `serve-refresh`, `core-weekly`, `cold-core`, `cpar-build`.
- Confirm persisted refresh lock state is clear (no active run id or stale running status).
- Record paused scheduler job names and timestamps in cutover evidence.

1. Freeze non-cutover schema and model changes during execution window.
2. Capture starting state and immutable rollout contract:
   - `terraform output -json > backend/runtime/cloud_rollouts/<ts>/terraform-output.json`
   - record active image digests (not tags) for frontend/serve/control and all Cloud Run Jobs.
   - record Cloud Run service revisions and job template revisions.
   - record `git rev-parse HEAD` and Terraform state serial/version.
3. Verify auth/secret/runtime identity end-to-end:
   - GCP auth, `NEON_DATABASE_URL`, operator token.
   - positive token test on control endpoint and negative auth tests (`401` expected for anon/invalid token).
   - verify service-account IAM for serve/control/jobs on Secret Manager + Neon access path.
   - record secret version IDs mounted by each active service/job revision.
4. Cloud env-contract preflight (hard gate):
   - serve/control runtime: `APP_RUNTIME_ROLE=cloud-serve`, `DATA_BACKEND=neon`.
   - `NEON_AUTHORITATIVE_REBUILDS=true` for intended cutover path.
   - control dispatch env present: `CLOUD_RUN_JOBS_ENABLED=true`, project/region, all four job-name vars.
   - reject cutover execution if any contract field is missing or mismatched.
5. Tag baseline for rollback:
   - `git tag cutover-baseline-<YYYYMMDD-HHMM>`
   - `git push origin cutover-baseline-<...>`

Gate to proceed: automation paused, no in-flight executions, refresh lock clear, immutable contract captured, auth/secret checks green, env-contract preflight green.

## Phase 1 - Fix Data/State Blockers Before Final Cutover

### 1A. Repair local PIT recency so `source_sync` can run cleanly

1. Read Neon source watermark and local maxima for:
   - `security_fundamentals_pit`
   - `security_classification_pit`
2. Backfill local PIT anchors to close gap through Neon boundary.
3. Re-run local `source-daily` (ingest + PIT repair) with explicit `as_of_date`.
4. Re-run `source_sync` and verify success (no newer-than-target/older-local refusal).

Acceptance:

- `source_sync` stage status `completed`.
- Identifier-aware retained-window sync is healthy (not strict table-wide equality).
- Local archive may remain deeper than Neon retained publish window by design.

### 1B. Rebuild corrupted local cache DB

1. Quiesce local processes using `backend/runtime/cache.db`.
2. Archive corrupted file: `backend/runtime/cache.db.corrupt.<ts>`.
3. Rebuild cache schema using repo-owned initialization path.
4. Run local `serve-refresh` dry run against rebuilt cache for sanity.

Acceptance:

- No `database disk image is malformed`.
- Cache writes/reads succeed in smoke execution.

### 1C. Repair cPAR proxy anchor-week price coverage

1. Identify missing proxy list/date from cPAR failure payload.
2. Backfill required proxy prices in local source archive for missing week(s).
3. Publish to Neon through local `source_sync`.
4. Run `cpar-weekly` at latest intended date.

Acceptance:

- `cpar-weekly` completes with `status=ok`.
- `cpar_package_runs` shows latest successful package at intended date.

### 1D. Parity mismatch triage to operationally acceptable state

1. Run `repair_neon_sync_health --json` for latest core run.
2. Classify issues into:
   - expected retention-window deltas,
   - true data drift requiring sync repair,
   - false-positive comparison shape gaps.
3. Patch mismatch causes or codify expected deltas explicitly.

Acceptance:

- Parity status `ok` or approved bounded mismatch list documented in run artifact.

Gate to proceed: 1A/1B/1C complete; 1D either `ok` or signed bounded exceptions.

## Phase 1E - Cloud Readiness Gate Checkpoint (must pass before first cloud dispatch wave)

1. Confirm cloud-readiness gate set from canonical runbook is satisfied:
   - registry/policy/taxonomy parity for operating surfaces,
   - source-sync expectations met,
   - stable-core expectations met,
   - Neon readiness checks green for exposed lanes.
2. Confirm current topology and active origins from Terraform outputs:
   - `endpoint_mode`, `edge_enabled`, `public_origins`.
3. Confirm active service/job revisions still match captured immutable contract from Phase 0.

Gate to proceed: all readiness checks green and revision/digest equality preserved.

## Phase 2 - Enforce Cloud Compute as Default Operational Path

1. Confirm split-surface ownership and auth contract:
   - serve app does not own `/api/refresh`, `/api/refresh/status`, `/api/cpar/build`.
   - control app owns those routes and enforces token auth (`401` on anon/invalid token).
2. Confirm control-plane dispatch contract:
   - tokened `core-weekly` / `cold-core` / `cpar-build` => `202` (or `409` when lock active).
   - anonymous/invalid token => `401`.
   - tokened `POST /api/cpar/build?profile=not-a-profile` => `400`.
3. Validate dispatch backend records `cloud_run_job` and reconciles terminal status.
4. Source watermark contract for reproducible compute:
   - after `source_sync`, persist watermark tuple:
     - `as_of_date`, `source_sync_run_id`, per-table max dates/checksums.
   - all core/cPAR dispatches for the wave must reference this watermark.
   - if watermark changes during compute, abort promotion and rerun from new watermark.
5. Run full workflow using operational path:
   - local `source-daily` (local-ingest),
   - control dispatch `core-weekly`,
   - control dispatch `cpar-weekly`.
6. Verify cUSE/cPAR app-facing freshness and health endpoints.
7. Promotion guard:
   - treat cUSE publish pointer and cPAR package pointer as explicit promotion steps.
   - do not promote either pointer for a wave if required sibling lane for that wave failed.
   - record promoted pointer IDs and watermark tuple in evidence.

Acceptance:

- Compute lanes run via Cloud Run Jobs only.
- No local fallback execution for cloud-dispatched compute.
- cUSE and cPAR payload/package timestamps advance as expected.
- Cloud readiness gate remains satisfied post-dispatch.
- Promoted outputs are tied to the intended source watermark tuple.

## Phase 3 - Harden Runtime Guardrails and Operator Workflow

1. Update operator runbook snippets and scripts to make cloud-dispatch path primary.
2. Ensure local-core commands are documented as break-glass only.
3. Add/confirm explicit checks:
   - fail if dispatch env vars missing in cloud-serve.
   - fail if cloud compute request attempts source_sync in cloud-job.
4. Verify topology-aware checks:
   - `make cloud-topology-check`
   - `make operator-check` with live tokens.

Acceptance:

- Operators can run daily workflow without local core compute.
- Guardrails block ambiguous execution modes.

## Phase 4 - Stability Window and Rollback Readiness

### 4A. Entry Gate (must pass before Day 1)

Before the 5-business-day clock starts:

1. Recapture the immutable rollout bundle for the currently intended live state:
   - `terraform output -json`
   - current Cloud Run service revisions
   - current Cloud Run job revisions
   - current frontend/serve/control/job image digests
2. Run and save green transcripts for:
   - `make cloud-topology-check`
   - `make operator-check`
3. Record the current topology contract:
   - `endpoint_mode`
   - `edge_enabled`
   - `public_origins`
4. Record the current runtime truth:
   - `/api/operator/status`
   - `/api/refresh/status`
   - `/api/health/diagnostics`
   - `/api/data/diagnostics?include_paths=true`
   - `/api/cpar/meta`
5. Open one Phase 4 evidence log using:
   - `docs/operations/cutover_evidence/PHASE4_STABILITY_AND_ROLLBACK_TEMPLATE.md`

Gate to proceed:
- rollout bundle recaptured,
- topology/operator checks green,
- current topology contract and runtime truth captured.

This gate only authorizes Day 1 of the stability window.
It does **not** mean the phase is clean or signoff-ready.
Any degraded-but-reachable state discovered during the entry capture, such as stale diagnostics, a failed core lane, or disabled parity automation, must be recorded immediately as a Day 1 incident and cleared during the window.

### 4B. Daily Stability Window (5 business days)

For each business day in the window:

1. Run local ingest and source publication:
   - local `source-daily`
   - verify `source_sync` completion and source watermark tuple
2. Run cloud validation:
   - `make cloud-topology-check`
   - `make operator-check`
3. Capture current job execution state for:
   - `serve-refresh`
   - `core-weekly`
   - `cold-core`
   - `cpar-build`
4. Capture current runtime/readiness snapshots:
   - `/api/operator/status`
   - `/api/refresh/status`
   - `/api/health/diagnostics`
   - `/api/data/diagnostics?include_paths=true`
   - `/api/cpar/meta`
5. Update the incident log explicitly for any of:
   - job OOM/retries
   - stale running-state reconciliation
   - parity drift
   - cPAR package readiness misses
   - topology drift
   - authority/read-path failures
6. If any service/job image digest changes during the window:
   - recapture the rollout bundle the same day
   - record why the image changed

### 4C. Controlled Rollback Drill

Run one operator-controlled rollback drill during the window.

Required shape:

1. Use the saved rollout bundle as the rollback source of truth.
2. Induce one failed dispatch or equivalent controlled failure scenario.
3. Execute a cloud rollback to the prior known-good service/job image set and topology state.
4. Re-run:
   - `make cloud-topology-check`
   - `make operator-check`
5. Verify restored topology contract and runtime truth:
   - `endpoint_mode`
   - `edge_enabled`
   - `public_origins`
   - service/job image digests
   - `/api/operator/status`
   - `/api/refresh/status`
   - `/api/health/diagnostics`
   - `/api/cpar/meta`
6. Execute forward recovery back to the intended current topology.
7. Re-run the same checks after forward recovery.

Required evidence:
- rollback command transcript
- rollback verification transcript
- forward-recovery transcript
- timestamps for:
  - rollback start
  - rollback green
  - forward-recovery start
  - forward-recovery green

### 4D. RTO Target

For this cutover phase, the rollback drill target RTO is:

- rollback to restored green checks within 30 minutes
- forward recovery to restored green checks within 30 minutes

If either target is missed:
- hold cutover open,
- keep scheduler handback paused,
- record the root cause and remediation before signoff.

### 4E. Acceptance

Phase 4 is complete only when all of the following are true:

- 5-business-day stability window completed
- no Sev1/Sev2 operational regressions during the window
- no unexplained topology drift remains open
- one controlled rollback drill completed
- rollback green checks restored within target RTO
- forward recovery green checks restored within target RTO
- final evidence log contains:
  - incident summary
  - measured RTO
  - final topology snapshot
  - final rollout bundle reference

## Phase 5 - Cutover Signoff

Signoff checklist:

1. cUSE:
   - latest `core-weekly` cloud run successful.
   - `serve-refresh` cloud run successful.
   - projection-only ticker checks healthy in payloads.
2. cPAR:
   - latest `cpar-weekly` successful at current target date.
   - package banner/read surfaces reflect latest package.
3. Data authority:
   - local ingest + source_sync flow clean.
   - parity/health status accepted.
4. Operations:
   - runbooks updated to final-state commands.
   - topology checks and operator checks green.
5. Scheduler handback:
   - resume only approved scheduler mappings:
     - `source-daily` -> local-ingest surface,
     - core/cPAR lanes -> control dispatch to Cloud Run Jobs.
   - verify first scheduled cycle end-to-end before closing cutover.

Current status note:
- Phase 5 is not yet open for signoff.
- Remaining prerequisite is completion of the 5-business-day Phase 4 stability window.

Approvers: Engineering lead + Operations owner.

## 6. Rollback Plan

Use if cutover causes production instability:

1. Pause schedulers and block new dispatches.
2. Repoint serve/control/job revisions to prior known-good image digests and topology contract from rollout bundle.
3. Revert runtime publish pointers/snapshot IDs to last known-good cUSE and cPAR package states.
4. Restore prior env var contract from rollout bundle (including `NEON_AUTHORITATIVE_REBUILDS` policy).
5. If local-authority rollback is required:
   - shift execution to `local-ingest` role/host,
   - run `source-daily`/`source_sync` sequencing before local-authority core rebuild.
6. Validate health/read endpoints and run one no-op/expected dispatch check.
7. Re-enable schedulers only after validation gates pass.
8. Open incident note with exact failed gate, commands, and logs.

Abort condition:

- If pointer rollback cannot be verified, keep schedulers paused and hold cutover closed until incident resolution.

## 7. Evidence to Collect Per Phase

- Command output artifacts (`job_run_status`, dispatch responses, gcloud execution lists).
- Parity artifact JSON paths.
- `terraform output -json` snapshot.
- cUSE payload spot-checks for representative projected/ineligible names.
- cPAR package run rows and package date/source as-of values.

## 8. Self-Delete Instructions (Required at Completion)

This plan file is temporary and must be removed when cutover is complete.

Completion actions:

1. Move final execution record to archive:
   - create `docs/archive/cutover/full-cloud-compute-cutover-<YYYYMMDD>.md` with final timeline and evidence links.
2. Update canonical docs with steady-state truth:
   - `docs/operations/OPERATIONS_PLAYBOOK.md`
   - `docs/operations/CLOUD_NATIVE_RUNBOOK.md`
   - cPAR playbook/architecture docs if behavior changed.
3. Delete this temporary plan file:
   - `git rm docs/operations/FULL_CLOUD_COMPUTE_CUTOVER_PLAN.md`
4. Commit with message:
   - `Finalize full-cloud compute cutover and remove temporary execution plan`

Do not keep this file after cutover close unless cutover is officially re-opened.
