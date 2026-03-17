# Simplification Second Pass

Date: 2026-03-17
Status: Completed second-pass rescan after first simplification wave
Owner: Codex

## What The First Pass Unlocked

After deleting the `backend.data.cache` alias layer and removing the wrapper-only seams in
`backend/analytics/services/cache_publisher.py` and `backend/analytics/pipeline.py`, the repo
became easier to scan for the next tier of low-risk simplifications.

The main follow-up opportunities were:

1. Remove genuinely unused helpers that were easier to prove dead after the wrapper cleanup.
2. Collapse repeated cache-selection logic where a canonical helper already existed.
3. Trim imports left behind after the first deletions.

## High-Confidence Opportunities Found

### 1. Delete `can_reuse_cached_health_payload`

- Module: `backend/analytics/health_payloads.py`
- Finding: the helper had no runtime callers and no test callers.
- Why it became clearer now: once `cache_publisher.py` stopped wrapping health-payload helpers,
  the reuse surface became small enough to verify directly with a repo-wide search.
- Action: deleted.

### 2. Collapse duplicated live-first fallback in `portfolio_whatif.py`

- Module: `backend/services/portfolio_whatif.py`
- Finding: the module manually implemented `cache_get_live(...) or cache_get(...)` in two places
  even though `backend.data.sqlite.cache_get_live_first(...)` already owned that policy.
- Risk profile: low. The helper semantics were identical.
- Action: switched both sites to `cache_get_live_first(...)`.

### 3. Remove import leftovers exposed by the earlier deletions

- Modules:
  - `backend/analytics/publish_payloads.py`
  - `backend/analytics/services/cache_publisher.py`
- Finding: the first pass left a few imports that no longer had any real use.
- Action: removed the dead imports instead of carrying forward stale type and module references.

## Opportunities Considered And Rejected

### Additional private wrappers in `backend/analytics/pipeline.py`

Examples:
- `_resolve_effective_risk_engine_meta`
- `_can_reuse_cached_universe_loadings`
- `_build_universe_ticker_loadings`
- `_compute_exposures_modes`

Why they stayed:
- they are still active test seams across `backend/tests/test_operating_model_contract.py`
- deleting them now would mostly create churn and test rewiring rather than real runtime
  simplification
- they still help isolate orchestration-level policy from helper-module implementations

Conclusion:
- leave them alone for now

### Large deferred hotspots

Still deferred:
- `backend/analytics/health.py`
- `backend/services/neon_mirror.py`
- `backend/risk_model/daily_factor_returns.py`
- `backend/risk_model/raw_cross_section_history.py`

Why they stayed:
- they still own real workflow or domain behavior
- simplification there would no longer be “high-confidence deletion”; it would be correctness work

## Net Result Of The Second Pass

The second pass confirmed that the remaining easy wins were small but real:

- one dead helper deleted
- one duplicate cache-selection pattern collapsed
- a handful of import leftovers removed

It also made the stopping point clearer:

- further simplification would now require either
  - touching deliberate test seams, or
  - entering deferred large-module correctness territory

That is the right place to stop for this campaign.
