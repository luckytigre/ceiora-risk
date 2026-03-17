# Audit: Correctness And Regression Risk

Date: 2026-03-16
Reviewer: Independent architecture audit
Status: Post-refactor correctness review

Update note:

- The `temporary_runtime_paths()` correctness risk documented here was remediated in follow-up Batch 2 later on 2026-03-16.

## Verification Performed

Targeted checks run during this audit:

- module imports in the active virtualenv for:
  - `backend.orchestration.run_model_pipeline`
  - `backend.services.refresh_manager`
  - `backend.services.operator_status_service`
  - `backend.analytics.pipeline`
  - `backend.data.core_reads`
  - `backend.data.model_outputs`
- targeted tests:
  - `backend/tests/test_refresh_profiles.py`
  - `backend/tests/test_operator_status_route.py`
  - `backend/tests/test_dashboard_payload_service.py`
  - `backend/tests/test_core_reads.py`

Result:

- imports succeeded
- `58` targeted tests passed

## Current Correctness Assessment

### Good news

- No obvious broken imports were found in the major restructured areas.
- The orchestration/data/dashboard seams exercised by the targeted tests are currently intact.
- The new helper modules are not obviously orphaned.

### Remaining correctness risks

These are structural risks more than active failures.

## Partially Migrated Or Transitional Areas

### 1. `backend/orchestration/__init__.py` still re-exports the main workflow

This is not broken, but it is an unnecessary package-level surface that can obscure the real entrypoint and encourage coupling.

### 2. `backend/api/routes/operator.py` still exposes compatibility aliases

The route re-exports service internals. That is not a runtime failure, but it is a correctness risk because it keeps tests and callers tied to a transport-layer module instead of the real owning service.

### 3. `core_reads.py` and `model_outputs.py` are structurally transitional

They are better than before, but they still represent a partly migrated state:

- split into helper modules
- still wrapper-heavy facades

That is not wrong, but it means future changes can still be made in the wrong layer if discipline slips.

## Naming / Structure Consistency

### Mostly consistent

- `*_service.py` usage is reasonable for the new application-facing services
- orchestration helper names are more specific than before
- the new docs package is consistently named

### Still inconsistent

- `backend/api/routes/presenters.py` exists, while the target architecture document describes `backend/api/presenters/`
- target docs describe `backend/data/model_outputs/` as a package target, while the code uses flat helper modules
- `backend/services/` still mixes application services and infrastructure-heavy Neon modules under one naming convention

## Dead References / Orphaned Paths

No obvious orphaned refactor modules were found in the inspected areas.

The more realistic risk is not dead code; it is that some files are still acting as soft compatibility surfaces after the real ownership moved elsewhere.

## Test Coverage Gaps That Matter Now

### 1. Route boundary enforcement is not directly tested as an architecture rule

Behavior is tested, but there is no architectural guard preventing routes from reaching directly into `backend.data`.

This is exactly the kind of drift that can return after a refactor.

### 2. Global path mutation behavior is only lightly protected

`temporary_runtime_paths()` is a high-risk mechanism because it mutates module globals across subsystems.

There is some test coverage around it, but not enough to make that pattern low-risk.

### 3. Remaining god modules are still risk concentration points

Large files like:

- `backend/services/neon_mirror.py`
- `backend/analytics/health.py`
- `backend/services/neon_holdings.py`
- `backend/risk_model/daily_factor_returns.py`

may have behavior tests, but their internal architecture is still complex enough that regressions can hide inside them.

## Bottom Line

The refactor does not appear to have left the repo in a partially broken state.

The main correctness concern now is not broken imports or dead modules. It is regression risk from:

- compatibility leftovers
- direct route-to-data imports
- hidden global state mutation
- a handful of still-large operational files
