# Rollback Procedure

Date: 2026-04-14
Owner: Platform / Risk Ops
Status: Active rollback baseline for cloud cutover

## Purpose

This document defines two different rollback modes:

1. **Cloud rollback drill / incident rollback**
   - restore the prior known-good cloud topology and image set
   - keep the cloud-native operating model intact
2. **Break-glass return to local authority**
   - revert serving/rebuild authority to local SQLite
   - use only when the cloud path is fundamentally unavailable or corrupted

These modes are not interchangeable.
Phase 4 of the cutover plan requires the first mode.
The second mode is the emergency fallback.

## Mode A - Cloud Rollback Drill / Incident Rollback

### Objective

Restore the last known-good cloud deployment using the saved rollout bundle and prove the system is healthy again before resuming forward progress.

### Preconditions

- schedulers paused or dispatches otherwise frozen
- no new image rollout in progress
- latest rollout bundle available with:
  - `terraform output -json`
  - service revisions
  - job revisions
  - image digests
- operator token available

### Required Evidence

Record all of the following in the current Phase 4 evidence log:

- rollback start timestamp
- rollout bundle path / bundle timestamp
- service and job digests before rollback
- rollback command transcript
- topology and operator check transcripts after rollback
- forward-recovery transcript
- measured rollback and forward-recovery RTO

### Procedure

1. Freeze automation
   - pause schedulers if active
   - block new dispatches
2. Confirm the target rollback bundle
   - identify the prior known-good frontend/serve/control image digests
   - identify the prior known-good service/job revisions if needed
   - confirm the bundle matches the intended topology contract
3. Restore the cloud deployment to the target bundle
   - use the repo rollback helper, not an ad hoc Terraform invocation:

```bash
CUTOVER_ACTION=plan \
CUTOVER_PHASE=rollback \
ROLLOUT_BUNDLE_DIR=<bundle_dir> \
make cloud-run-app-cutover
```

```bash
CUTOVER_ACTION=apply \
CUTOVER_PHASE=rollback \
ROLLOUT_BUNDLE_DIR=<bundle_dir> \
ALLOW_TERRAFORM_APPLY=1 \
make cloud-run-app-cutover
```

   - this path renders `rollback_custom_domains.tfvars` from the bundle and applies it through `scripts/cloud/run_app_cutover.sh`
   - if the bundle is not self-consistent with the current live state, stop and recapture a fresh bundle rather than overriding drift by hand
4. Re-verify topology
   - run:

```bash
APP_BASE_URL=https://app.ceiora.com \
CONTROL_BASE_URL=https://control.ceiora.com \
OPERATOR_API_TOKEN=<operator-token> \
make cloud-topology-check
```

   - confirm `endpoint_mode`, `edge_enabled`, and `public_origins`
   - confirm service/job image digests match the target bundle
5. Re-verify operator/runtime behavior
   - run:

```bash
APP_BASE_URL=https://app.ceiora.com \
CONTROL_BASE_URL=https://control.ceiora.com \
OPERATOR_API_TOKEN=<operator-token> \
make operator-check
```

   - capture:
     - `/api/operator/status`
     - `/api/refresh/status`
     - `/api/health/diagnostics`
     - `/api/cpar/meta`
6. Only after rollback is green, execute forward recovery
   - if forward recovery is still intended, use the same helper path against the forward bundle:

```bash
CUTOVER_ACTION=plan \
CUTOVER_PHASE=<soak|no-edge> \
ROLLOUT_BUNDLE_DIR=<forward_bundle_dir> \
make cloud-run-app-cutover
```

```bash
CUTOVER_ACTION=apply \
CUTOVER_PHASE=<soak|no-edge> \
ROLLOUT_BUNDLE_DIR=<forward_bundle_dir> \
ALLOW_TERRAFORM_APPLY=1 \
make cloud-run-app-cutover
```

   - rerun the same topology/operator checks after forward recovery

### Operator Notes

- `CUTOVER_ACTION=verify ROLLOUT_BUNDLE_DIR=<bundle_dir> OPERATOR_API_TOKEN=<operator-token> make cloud-run-app-cutover` is the preferred wrapper when you want the helper to execute the topology-aware live verification path directly.
- The rollback bundle must reflect the **live** service and job image refs at capture time. A bundle derived only from stale Terraform state is not sufficient for a rollback drill.
- A Phase 4 entry-gate evidence file is not a rollback drill artifact. The rollback section must be populated with real command transcripts and restored-state evidence before the drill counts.
- The 2026-04-14 Phase 4 drill exposed one more operator rule:
  - immediately after a control revision cutover, direct control-surface probes may transiently timeout even when the proxied/operator surfaces are already healthy
  - if the first post-cutover verification fails only on that direct probe shape, retry once after the new revision settles before declaring rollback or forward recovery failed
- Recorded drill evidence:
  - `docs/operations/cutover_evidence/PHASE4_ROLLBACK_DRILL_20260414T215254Z.md`

### Success Criteria

- rollback checks green within target RTO
- forward-recovery checks green within target RTO
- final topology and digest state exactly match the intended bundle

### Failure Handling

If rollback pointer/digest verification fails:

- keep schedulers paused
- keep new dispatches blocked
- hold cutover open
- escalate as an incident
- do not proceed to forward recovery signoff until:
  - the pointer mismatch is explained
  - the restored topology is verified

## Mode B - Break-Glass Return To Local Authority

### Objective

Re-establish a local-ingest host as the authority for serving and rebuilds when the cloud/Neon path is fundamentally unavailable or untrustworthy.

### Environment Configuration

On the local-ingest host:

```bash
export DATA_BACKEND=sqlite
export NEON_AUTHORITATIVE_REBUILDS=false
export APP_RUNTIME_ROLE=local-ingest
export ORCHESTRATOR_ENABLE_INGEST=true
```

### Data Reconciliation

Ensure local `data.db` and `cache.db` are healthy.
If local state is behind, rebuild from the last known-good anchor:

```bash
python3 -m backend.orchestration.run_model_pipeline \
  --profile cold-core \
  --as-of-date <LAST_STABLE_DATE> \
  --force-core
```

### Frontend/API Pointing

If frontend or operator clients were pointed at cloud services, point them back to the local API surface as needed.

### Post-Rollback Verification

1. Run local operator checks
2. Verify holdings and risk views
3. Investigate the cloud-side issue before attempting any return to Neon/cloud authority

## Return To Cloud After Break-Glass Mode

Do not re-enable cloud/Neon authority until:

1. root cause is identified and fixed
2. `make cloud-topology-check` is green
3. `make operator-check` is green
4. a new rebuild / publish path has been successfully completed and verified
