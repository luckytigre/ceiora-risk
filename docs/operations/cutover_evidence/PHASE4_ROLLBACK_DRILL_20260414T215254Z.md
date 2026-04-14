# Phase 4 Rollback Drill - Direct Cloud Run Execution

Timestamp (UTC): `2026-04-14T21:52:54Z`
Plan reference: `docs/operations/FULL_CLOUD_COMPUTE_CUTOVER_PLAN.md` -> Phase 4 -> Controlled Rollback Drill
Rollback bundle reference: `backend/runtime/cloud_rollouts/phase4_entry_20260414T201917Z`

## Scope

- Drill target: `control` service and the four control-dispatched Cloud Run Jobs
  - `ceiora-prod-control`
  - `ceiora-prod-serve-refresh`
  - `ceiora-prod-core-weekly`
  - `ceiora-prod-cold-core`
  - `ceiora-prod-cpar-build`
- Rollback image: `us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images/control:2d8b11f`
- Forward-recovery image: `us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images/control:8458353-coldcoreguard1`

## Execution Path

- Intended helper path:
  - `make cloud-run-app-cutover` with the corrected bundle
- Constraint:
  - Terraform plan was blocked in this shell by missing Cloudflare auth required for the full custom-domain plan path
- Executed drill path:
  - direct `gcloud run services update`
  - direct `gcloud run jobs update`
- Rationale:
  - the rollback defect under review was on the control service/job image set, and that surface can be exercised directly without changing the frontend/serve topology

## Rollback

- Rollback start anchor:
  - control revision `ceiora-prod-control-00026-b2q`
  - creation timestamp `2026-04-14T21:50:49.939134Z`
  - artifact: `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/control-revision.rollback.yaml`
- Rolled-back live image state:
  - service + all four jobs on `control:2d8b11f`
  - artifact: `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/live-control-images.rollback.yaml`
- Post-rollback verification:
  - `make cloud-topology-check`: `PASS`
  - `make operator-check`: `PASS`
  - artifacts:
    - `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/cloud-topology-check.rollback.txt`
    - `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/operator-check.rollback.txt`
- Post-rollback runtime truth snapshots:
  - `operator/status`: `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/operator_status.json`
  - `refresh/status`: `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/refresh_status.json`
  - `health/diagnostics`: `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/health_diagnostics.json`
  - `data/diagnostics`: `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/data_diagnostics.json`
- Rollback complete verification anchor:
  - proxied operator status generated at `2026-04-14T21:53:31.541047Z`
- Measured rollback RTO:
  - `2m42s`

## Forward Recovery

- Forward-recovery start anchor:
  - control revision `ceiora-prod-control-00027-9gv`
  - creation timestamp `2026-04-14T21:53:50.929350Z`
  - artifact: `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/control-revision.forward.yaml`
- Forward-recovered live image state:
  - service image `control:8458353-coldcoreguard1`
  - all four jobs updated to `control:8458353-coldcoreguard1`
  - artifact: `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/live-control-images.forward.yaml`
- First verification attempt:
  - proxied checks remained healthy
  - direct control-surface probe inside the check scripts hit a transient timeout / truncated JSON immediately after revision cutover
  - artifacts:
    - `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/cloud-topology-check.forward.txt`
    - `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/operator-check.forward.txt`
- Retry verification after revision settled:
  - `make cloud-topology-check`: `PASS`
  - `make operator-check`: `PASS`
  - artifacts:
    - `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/cloud-topology-check.forward.retry.txt`
    - `docs/operations/cutover_evidence/phase4_20260414T201917Z/rollback_drill/20260414T215254Z/operator-check.forward.retry.txt`
- Direct runtime spot-check after recovery:
  - `/api/operator/status` -> `status=ok`
  - `/api/refresh/status` -> `status=ok`
  - `/api/health/diagnostics` -> `status=deferred`
- Forward-recovery complete verification anchor:
  - proxied operator status generated at `2026-04-14T22:16:45.353813Z`
- Measured forward-recovery RTO:
  - `22m54s`

## Result

- Rollback drill status: `COMPLETE`
- Rollback within 30-minute target: `YES`
- Forward recovery within 30-minute target: `YES`
- Open note:
  - the direct control-surface probe can fail transiently immediately after a control revision cutover even when the proxied/operator surfaces are already healthy; the retry cleared without further intervention
