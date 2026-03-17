# Repository Hygiene Inventory

Date: 2026-03-17
Status: Repository hygiene classification
Owner: Codex

This inventory covers non-code artifacts that looked like planning notes, audits, remediation notes, investigations, logs, scratch outputs, superseded architecture notes, or duplicate documentation.

Paths below are the original pre-cleanup locations used for classification.

## KEEP ACTIVE

| Path | Classification | Reason |
| --- | --- | --- |
| `docs/ARCHITECTURE_AND_OPERATING_MODEL.md` | KEEP ACTIVE | Canonical architecture and operating-model reference. |
| `docs/OPERATIONS_PLAYBOOK.md` | KEEP ACTIVE | Canonical operator runbook. |
| `docs/OPERATIONS_HARDENING_CHECKLIST.md` | KEEP ACTIVE | Active pre-run and rollback checklist. |
| `docs/README.md` | KEEP ACTIVE | Primary docs index. |
| `docs/architecture/architecture-invariants.md` | KEEP ACTIVE | Non-negotiable active architecture rules. |
| `docs/architecture/dependency-rules.md` | KEEP ACTIVE | Active dependency and documentation update rules. |
| `docs/architecture/maintainer-guide.md` | KEEP ACTIVE | Active maintainer guidance. |
| `docs/architecture/current-state.md` | KEEP ACTIVE | Current architecture diagnosis still useful for maintainers. |
| `docs/architecture/target-architecture.md` | KEEP ACTIVE | Canonical target ownership and structure reference. |
| `docs/architecture/module-inventory.md` | KEEP ACTIVE | Useful active module map. |
| `docs/data_protocols/TRBC_CLASSIFICATION_PIT_PROTOCOL.md` | KEEP ACTIVE | Canonical PIT data protocol. |
| `docs/data_protocols/UNIVERSE_ADD_RUNBOOK.md` | KEEP ACTIVE | Canonical universe-add workflow. |
| `docs/migrations/README.md` | KEEP ACTIVE | Entry point for migration docs. |
| `docs/migrations/neon/NEON_CANONICAL_SCHEMA.sql` | KEEP ACTIVE | Canonical Neon schema artifact. |
| `docs/migrations/neon/NEON_HOLDINGS_IMPORT_BEHAVIOR.md` | KEEP ACTIVE | Still-relevant holdings behavior reference. |
| `docs/migrations/neon/NEON_HOLDINGS_MODEL_SPEC.md` | KEEP ACTIVE | Still-relevant holdings model spec. |
| `docs/migrations/neon/NEON_HOLDINGS_SCHEMA.sql` | KEEP ACTIVE | Canonical Neon holdings schema artifact. |
| `docs/migrations/neon/NEON_MIGRATION_EXECUTION_PLAN.md` | KEEP ACTIVE | Still useful migration/runbook reference. |
| `docs/migrations/neon/NEON_STAGE1_OPERATOR_RUNBOOK.md` | KEEP ACTIVE | Still useful historical operator runbook for Neon migration surfaces. |
| `docs/specs/USE4_US_CORE_MARKET_ADR_2026-03-15.md` | KEEP ACTIVE | Accepted ADR and still-valid decision record. |
| `docs/specs/cUSE4_engine_spec.md` | KEEP ACTIVE | Canonical model specification. |
| `backend/runtime/audit_reports/neon_parity/latest_neon_mirror_report.json` | KEEP ACTIVE | Current parity report surface; active runtime readout. |

## ARCHIVE

| Path | Classification | Reason |
| --- | --- | --- |
| `docs/HEALTH_DIAGNOSTICS_REFRESH_PLAN.md` | ARCHIVE | Completed focused execution plan; historical context only. |
| `docs/NEON_AUTHORITATIVE_REBUILD_PLAN.md` | ARCHIVE | Historical migration plan; no longer active working guidance. |
| `docs/NEON_LEAN_CONSOLIDATION_PLAN.md` | ARCHIVE | Historical consolidation plan. |
| `docs/NEON_MAIN_PLATFORM_PLAN.md` | ARCHIVE | Historical migration plan; superseded by active operating docs. |
| `docs/NEON_STANDALONE_EXECUTION_PLAN.md` | ARCHIVE | Historical execution plan. |
| `docs/PROJECT_HARDENING_ORGANIZATION_PLAN.md` | ARCHIVE | Completed precursor plan; not active guidance anymore. |
| `docs/archive_notes/CLOUD_READINESS_EXECUTION_PLAN_2026-03-09.md` | ARCHIVE | Historical plan; archive should stay historical but consolidated. |
| `docs/archive_notes/CUSE4_EFFICIENCY_IMPLEMENTATION_PLAN_2026-03-09.md` | ARCHIVE | Historical plan; consolidate into archive tree. |
| `docs/archive_notes/README.md` | ARCHIVE | Historical archive index; replaced by unified archive README. |
| `docs/archive_notes/US_CORE_ONE_STAGE_EXECUTION_LOG_2026-03-15.md` | ARCHIVE | Historical execution log. |
| `docs/archive_notes/US_CORE_ONE_STAGE_MIGRATION_PLAN_2026-03-15.md` | ARCHIVE | Historical migration plan. |
| `docs/archive_notes/cUSE4_Backend_Execution_Plan_2026-03-04.md` | ARCHIVE | Historical execution plan. |
| `docs/architecture/audit-architecture.md` | ARCHIVE | Historical audit record, not active guidance. |
| `docs/architecture/audit-correctness.md` | ARCHIVE | Historical audit record, not active guidance. |
| `docs/architecture/audit-docs.md` | ARCHIVE | Historical audit record, not active guidance. |
| `docs/architecture/audit-final-architecture.md` | ARCHIVE | Historical acceptance audit. |
| `docs/architecture/audit-final-workflows.md` | ARCHIVE | Historical acceptance audit. |
| `docs/architecture/audit-plan-vs-reality.md` | ARCHIVE | Historical comparison note. |
| `docs/architecture/audit-simplification.md` | ARCHIVE | Historical audit note. |
| `docs/architecture/audit-summary.md` | ARCHIVE | Historical audit summary. |
| `docs/architecture/audit-workflows.md` | ARCHIVE | Historical workflow audit. |
| `docs/architecture/bug-investigation-risk-and-eligibility.md` | ARCHIVE | Correctness incident investigation; useful history, not active guidance. |
| `docs/architecture/core-cadence-investigation.md` | ARCHIVE | Investigation artifact; durable rules were incorporated into active docs. |
| `docs/architecture/core-cadence-target.md` | ARCHIVE | Target note; durable rules were incorporated into active docs. |
| `docs/architecture/data-corrective-area2-findings.md` | ARCHIVE | Historical corrective findings. |
| `docs/architecture/data-corrective-area3-findings.md` | ARCHIVE | Historical corrective findings. |
| `docs/architecture/data-corrective-plan.md` | ARCHIVE | Historical corrective plan. |
| `docs/architecture/data-health-check-findings.md` | ARCHIVE | Historical health-check findings. |
| `docs/architecture/data-health-check-lseg-spot-checks.md` | ARCHIVE | Historical spot-check evidence. |
| `docs/architecture/data-health-check-plan.md` | ARCHIVE | Historical health-check plan. |
| `docs/architecture/data-health-check-summary.md` | ARCHIVE | Historical health-check summary. |
| `docs/architecture/date-semantics-investigation.md` | ARCHIVE | Investigation artifact; canonical semantics live elsewhere now. |
| `docs/architecture/date-semantics-target.md` | ARCHIVE | Target note; canonical semantics now live in active docs. |
| `docs/architecture/final-acceptance-review.md` | ARCHIVE | Historical closeout review. |
| `docs/architecture/final-summary.md` | ARCHIVE | Historical closeout summary. |
| `docs/architecture/follow-up-remediation-plan.md` | ARCHIVE | Completed remediation tracker. |
| `docs/architecture/performance-inventory.md` | ARCHIVE | Historical performance pass record. |
| `docs/architecture/performance-priorities.md` | ARCHIVE | Historical performance prioritization. |
| `docs/architecture/performance-second-review.md` | ARCHIVE | Historical performance follow-up. |
| `docs/architecture/performance-summary.md` | ARCHIVE | Historical performance summary. |
| `docs/architecture/proposal-boundaries.md` | ARCHIVE | Superseded proposal note. |
| `docs/architecture/proposal-simplification.md` | ARCHIVE | Superseded proposal note. |
| `docs/architecture/proposal-workflows.md` | ARCHIVE | Superseded proposal note. |
| `docs/architecture/refactor-roadmap.md` | ARCHIVE | Historical roadmap; no longer active. |
| `docs/architecture/restructure-plan.md` | ARCHIVE | Completed restructure tracker. |
| `docs/architecture/semantic-audit.md` | ARCHIVE | Historical semantic audit. |
| `docs/architecture/semantic-summary.md` | ARCHIVE | Historical semantic summary. |
| `docs/architecture/semantic-target.md` | ARCHIVE | Historical semantic target note. |
| `docs/architecture/simplification-candidates.md` | ARCHIVE | Historical simplification pass artifact. |
| `docs/architecture/simplification-inventory.md` | ARCHIVE | Historical simplification pass artifact. |
| `docs/architecture/simplification-second-pass.md` | ARCHIVE | Historical simplification pass artifact. |
| `docs/architecture/simplification-summary.md` | ARCHIVE | Historical simplification summary. |
| `backend/runtime/audit_reports/neon_parity/manual_neon_factor_returns_recert_20260308T100000Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260306T071603Z_api_ff5c0b01985c.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260308T003033Z_api_db8c3315128c.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260308T003339Z_job_20260308T002324Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260308T095620Z_job_20260308T094425Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260308T095950Z_job_20260308T094425Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260309T060129Z_api_8d2b14654587.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260312T002927Z_api_4d2a3c6ef232.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260312T003217Z_api_9b44b4f4806b.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260312T005635Z_api_ff32b4542c6b.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260312T010826Z_api_2c5f62f7910f.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260312T041626Z_api_341f14641819.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260312T083410Z_api_32d2678f448b.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260312T155825Z_api_b4d12dd76a65.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260312T165122Z_api_013e2eb1ef13.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260313T025606Z_job_20260313T025414Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260313T035627Z_job_20260313T030253Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260313T044918Z_job_20260313T042749Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260313T045423Z_job_20260313T043254Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260313T052101Z_manual_neon_recheck_20260313T223400Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260313T054426Z_job_20260313T042514Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260314T063622Z_job_20260314T053452Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260314T064726Z_job_20260314T063829Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260314T070336Z_job_20260314T065544Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260314T102926Z_job_20260314T093535Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260315T001622Z_api_0a5e433efa9c.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260315T161948Z_api_07cf5c541467.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260315T170509Z_api_19b3c162ce46.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260315T175519Z_api_d6b92d3a333e.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260315T175530Z_api_d7218e0d3d58.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260315T175557Z_api_73cb75679c7b.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260315T202438Z_api_93bb7317bab2.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260316T011747Z_api_29008dcdf8ae.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260316T033615Z_api_61fa0077bde5.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260316T034003Z_api_6d8b6d72d55f.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260316T034636Z_job_20260316T034627Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260316T034637Z_job_20260316T034535Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260316T034637Z_job_20260316T034605Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260316T034651Z_job_20260316T034622Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260316T034655Z_job_20260316T034642Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260316T040921Z_job_20260316T040358Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260316T042424Z_job_20260316T042056Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260316T045055Z_job_20260316T044729Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260316T074141Z_job_20260316T073537Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260316T075249Z_manual_neon_parity_20260316T075241Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/audit_reports/neon_parity/neon_mirror_20260316T091248Z_job_20260316T082802Z.json` | ARCHIVE | Historical parity report. |
| `backend/runtime/recovery/cache.db.corrupt_20260316T161913.bak` | ARCHIVE | Historical recovery backup; not active runtime state. |
| `backend/runtime/recovery/data.db.corrupt_20260305T004556.bak` | ARCHIVE | Historical recovery backup; not active runtime state. |
| `backend/runtime/recovery/data.db.corrupt_live_20260305T005352.bak` | ARCHIVE | Historical recovery backup; not active runtime state. |
| `backend/runtime/recovery/data.db.pre_ric_keys_20260304T071709Z.bak` | ARCHIVE | Historical recovery backup; not active runtime state. |
| `backend/runtime/recovery/data.recovered_20260305T004612.db` | ARCHIVE | Historical recovery output; archive rather than keep in active runtime tree. |
| `backend/runtime/reports/DB_HEALTH_AUDIT_2026-03-05.json` | ARCHIVE | Historical runtime audit output. |
| `backend/neon_stage1_prep/neon_stage1_20260305T063517Z/sqlite_health_audit.json` | ARCHIVE | Historical stage-prep audit output. |

## DELETE

| Path | Classification | Reason |
| --- | --- | --- |
| `docs/.DS_Store` | DELETE | Finder artifact; pure clutter. |
| `backend/runtime/cache (Shaun Cho's conflicted copy 2026-03-16 1).db-wal` | DELETE | Conflict artifact; not a canonical runtime surface. |
| `backend/runtime/cache (Shaun Cho's conflicted copy 2026-03-16).db-wal` | DELETE | Conflict artifact; not a canonical runtime surface. |
| `backend/runtime/local_app/logs/backend.log` | DELETE | Transient runtime log; regenerated as needed. |
| `backend/runtime/local_app/logs/frontend.log` | DELETE | Transient runtime log; regenerated as needed. |
| `backend/runtime/manual_launch_logs/backend.log` | DELETE | Transient manual-launch log; not durable knowledge. |
| `backend/runtime/uvicorn.log` | DELETE | Transient server log; not durable knowledge. |
| `frontend/backend/runtime/manual_launch_logs/frontend.log` | DELETE | Stray transient log in the frontend tree. |
| `backend/runtime/local_app/pids/backend.pid` | DELETE | Stale process-id artifact; regenerated as needed. |
| `backend/runtime/local_app/pids/backend.session` | DELETE | Stale process/session artifact; regenerated as needed. |
| `backend/runtime/local_app/pids/frontend.pid` | DELETE | Stale process-id artifact; regenerated as needed. |
| `backend/tmp/archive/ticker_diff_summary_20260305.txt` | DELETE | Temporary scratch diff output with no lasting value. |
