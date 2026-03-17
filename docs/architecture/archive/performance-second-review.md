# Performance Second Review

Date: 2026-03-17
Status: Completed second review after first optimization pass
Owner: Codex

## Review Focus

Re-reviewed:

- API response shaping
- risk / exposures / health / data routes
- serving payload generation
- holdings / what-if / dashboard refresh
- pipeline stage planning and repeated data reads

The goal was to identify what the first pass unlocked and whether another safe optimization tier
was available without turning this into architecture work.

## Findings After First Pass

### Risk route

Current state:
- `dashboard_payload_service.load_risk_response()` now batch-loads `risk` and `model_sanity`
  when the default serving-output loader is in use
- custom loader monkeypatch seams still work

Assessment:
- good first-pass win
- remaining work here is mostly normalization logic, not repeated IO

Conclusion:
- leave as-is

### Exposures route

Current state:
- `/api/exposures` still reads a single payload
- `/api/exposures/history` now prefers the smaller `universe_factors` catalog surface and stops
  after the first available catalog

Assessment:
- the obvious redundant payload loading was removed
- the remaining fallback history-resolution query path is less common and more sensitive to
  factor-name edge cases

Conclusion:
- defer further optimization unless profiling shows factor-history traffic is material

### Health route

Current state:
- `/api/health/diagnostics` still reads one durable diagnostics payload
- deeper `backend/analytics/health.py` remains a deferred hotspot

Assessment:
- no clean low-risk efficiency win remains on the active health route surface

Conclusion:
- defer deep health diagnostics optimization

### Data diagnostics route

Current state:
- `table_stats(...)` now uses one min/max date query instead of two
- source-table inventory no longer performs avoidable outer existence checks before calling
  `table_stats(...)`

Assessment:
- worthwhile second-tier cleanup for a query-heavy diagnostics surface
- still intentionally more expensive than normal routes

Conclusion:
- good stopping point for this pass

### Portfolio what-if

Current state:
- preview path now batch-loads current durable serving payloads when the default loader is active
- fallback behavior and monkeypatch seams are preserved

Assessment:
- meaningful reduction in durable payload round-trips on one of the heavier interactive routes
- remaining cost is mostly the actual analytics recomputation, which is expected

Conclusion:
- keep the math path intact; no further low-risk shortcut identified

### Pipeline / stage planning

Current state:
- reviewed `backend/analytics/pipeline.py`
- the main expensive work is still the intended work:
  - snapshot rebuild
  - full-universe loadings build
  - risk decomposition
  - exposure-mode computation

Assessment:
- repeated metadata/cache reads are small relative to the main compute cost
- no clear low-risk optimization beat the existing reuse rules

Conclusion:
- document as hotspot, defer deeper optimization until profiling data exists

## Second-Pass Implementation Chosen

The only additional code change after the first pass was on the diagnostics path:

- remove repeated table existence/schema checks where the same information can be derived once
- collapse min/max date lookup into one SQL round-trip

This was the right boundary:

- the earlier payload batching fixed the biggest avoidable request-side IO waste
- the next remaining hotspots are mostly compute-heavy or deferred legacy areas

## Remaining Hotspots After This Review

- `backend/analytics/services/universe_loadings.py`
- `backend/risk_model/daily_factor_returns.py`
- `backend/risk_model/raw_cross_section_history.py`
- `backend/analytics/health.py`
- fallback factor-name resolution in `backend/data/history_queries.py`

These still merit profiling, but they are no longer high-confidence “just remove repeated work”
targets.
