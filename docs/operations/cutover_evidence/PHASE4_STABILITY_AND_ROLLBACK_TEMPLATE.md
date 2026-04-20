# Phase 4 Evidence Template - Stability Window and Rollback Readiness

Timestamp (UTC): `<fill>`
Plan reference: `docs/operations/FULL_CLOUD_COMPUTE_CUTOVER_PLAN.md` -> Phase 4

## 1. Entry Gate

### 1A. Rollout Bundle

- Bundle path: `<fill>`
- Terraform output artifact: `<fill>`
- Captured service revisions:
  - frontend: `<fill>`
  - serve: `<fill>`
  - control: `<fill>`
- Captured job revisions:
  - serve-refresh: `<fill>`
  - core-weekly: `<fill>`
  - cold-core: `<fill>`
  - cpar-build: `<fill>`
- Captured image digests:
  - frontend: `<fill>`
  - serve: `<fill>`
  - control: `<fill>`
  - jobs/control image: `<fill>`

### 1B. Topology Contract

- `endpoint_mode`: `<fill>`
- `edge_enabled`: `<fill>`
- `public_origins`: `<fill>`

### 1C. Required Green Checks

- `make cloud-topology-check`: `PASS|FAIL`
- `make operator-check`: `PASS|FAIL`

### 1D. Runtime Truth Snapshot

- `/api/operator/status`: `<artifact path>`
- `/api/refresh/status`: `<artifact path>`
- `/api/health/diagnostics`: `<artifact path>`
- `/api/data/diagnostics?include_paths=true`: `<artifact path>`
- `/api/cpar/meta`: `<artifact path>`

## 2. Daily Stability Log

Repeat this section once per business day.

### Day `<N>` - `<date>`

- local `source-daily`: `PASS|FAIL`
- `source_sync` watermark tuple:
  - `as_of_date`: `<fill>`
  - `source_sync_run_id`: `<fill>`
  - per-table maxima/checksums artifact: `<fill>`
- `make cloud-topology-check`: `PASS|FAIL`
- `make operator-check`: `PASS|FAIL`
- latest job executions artifact: `<fill>`
- runtime truth artifact set: `<fill>`

Incidents:
- OOM/retries: `<none|details>`
- stale running-state reconciliation: `<none|details>`
- parity drift: `<none|details>`
- cPAR package readiness miss: `<none|details>`
- topology drift: `<none|details>`
- authority/read-path failures: `<none|details>`

Image/bundle changes today:
- `<none|details>`

## 3. Controlled Rollback Drill

### 3A. Drill Setup

- induced failure scenario: `<fill>`
- rollback target bundle: `<fill>`
- rollback start timestamp: `<fill>`

### 3B. Rollback Execution

- rollback command transcript: `<artifact path>`
- rollback complete timestamp: `<fill>`
- rollback RTO minutes: `<fill>`

### 3C. Post-Rollback Verification

- `make cloud-topology-check`: `PASS|FAIL`
- `make operator-check`: `PASS|FAIL`
- restored topology contract artifact: `<fill>`
- restored image digest verification artifact: `<fill>`
- restored runtime truth artifact: `<fill>`

### 3D. Forward Recovery

- forward-recovery start timestamp: `<fill>`
- forward-recovery transcript: `<artifact path>`
- forward-recovery complete timestamp: `<fill>`
- forward-recovery RTO minutes: `<fill>`

### 3E. Post-Recovery Verification

- `make cloud-topology-check`: `PASS|FAIL`
- `make operator-check`: `PASS|FAIL`
- final topology contract artifact: `<fill>`
- final digest verification artifact: `<fill>`
- final runtime truth artifact: `<fill>`

## 4. Acceptance Summary

- 5-business-day window completed: `YES|NO`
- Sev1/Sev2 incidents: `<none|details>`
- unexplained topology drift open: `YES|NO`
- rollback drill completed: `YES|NO`
- rollback within target RTO: `YES|NO`
- forward recovery within target RTO: `YES|NO`

Final references:
- final rollout bundle: `<fill>`
- final topology snapshot: `<fill>`
- signoff note: `<fill>`
