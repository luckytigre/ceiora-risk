# cUSE4 Efficiency Implementation Plan

Date: 2026-03-09
Status: First-wave priorities partly implemented
Owner: Shaun + Codex

Implementation note:
- implemented on branch `codex/cuse4-priority-efficiency`:
  - `CUSE4-001` lightweight stage telemetry on existing orchestrator/job-run surfaces
  - `CUSE4-002` cache invalidation now also tracks cross-section minimum-age policy for factor-return caches
  - `CUSE4-003` bounded factor-return price loading plus bounded eligibility snapshot loading
  - `CUSE4-006` explicit holdings-only light-refresh scope with safe reuse of published universe analytics when source/model fingerprints still match
- not yet implemented:
  - deeper eligibility memoization/vectorization follow-ons
  - style-score/raw-history structural optimizations
  - parallel regressions or SQLite staging changes

## Purpose

This plan turns the cUSE4 pathway audit into a practical implementation roadmap for a hobby-grade system that should be:

- fast enough for remote, on-demand recalc
- affordable under bursty cloud usage
- durable under changing universe size, factor definitions, lookback windows, and holdings workflows
- simpler and safer by default than a maximally optimized research platform

This plan explicitly does **not** optimize for speed at any cost. Any change that materially increases fragility, invalidation risk, or operational complexity must clear a higher bar.

## Operating Principles

1. Correctness and durability beat raw speed.
2. Prefer bounded, explicit recompute over clever partial recompute when uncertainty is high.
3. Reuse work only when invalidation rules are clear and enforceable.
4. Keep heavy compute decoupled from serving refresh where dependencies allow.
5. Delay architectural shifts like parallel compute staging or engine replacement until profiling proves they are needed.
6. Remote jobs must be operationally safe before they are computationally fast.
7. Cost controls should be explicit, not assumed.

## Current Bottleneck Summary

Based on code audit, the main current issues are:

- factor-return recompute loads and reshapes too much price history before it knows what dates need work
- eligibility context materializes too much history and repeats work across nearby dates
- structural eligibility uses Python-heavy row-wise work in a hot loop
- `cold-core` raw-history rebuild uses Python-level per-RIC as-of joins
- style-score construction repeats expensive per-date normalization and orthogonalization work
- the compute path still leans heavily on SQLite + pandas, which limits cloud burst efficiency

## Durability Rules

All optimization tickets below assume these guardrails:

- Any methodology-sensitive cache must invalidate on:
  - factor definition changes
  - descriptor preprocessing changes
  - orthogonalization rule changes
  - eligibility rule changes
  - lookback window changes where outputs depend on the wider window
  - source-table corrections that touch relevant historical rows
  - universe changes that require historical expansion
- If invalidation confidence is low, the system should widen recompute scope rather than rely on stale partial state.
- New optimizations must preserve snapshot-style publish semantics. Partial results must not become live.
- Holdings edits are assumed to be outside core model-state inputs unless explicitly and intentionally changed by future design.
- Remote compute must start with a single-worker, staged-output model before any parallel or distributed design is considered.

## Ticket Tiers

- `Now`: high-value, low-to-moderate risk, should be done before cloud burst-compute cutover
- `Later`: useful once the first wave lands and real timings confirm need
- `Only if needed`: valuable but complexity-sensitive; defer unless profiling justifies

## Tickets

### CUSE4-001: Add End-to-End Stage Profiling and Resource Telemetry

- Priority: `Now`
- Goal: measure runtime, row counts, memory pressure, and input/output sizes for each heavy stage
- Scope:
  - instrument `raw_history`, `feature_build`, `estu_audit`, `factor_returns`, `risk_model`, `serving_refresh`
  - log date counts, row counts, factor counts, ticker counts, and cache hit/miss context
  - extend existing stage/job-run artifacts rather than adding a parallel telemetry system
- Code areas:
  - `backend/orchestration/run_model_pipeline.py`
  - `backend/risk_model/daily_factor_returns.py`
  - `backend/risk_model/raw_cross_section_history.py`
- Why this matters:
  - prevents optimizing the wrong thing
  - establishes baseline before cloud migration decisions
- Defense:
  - changes no math, no outputs, and no workflow semantics
  - remains valid as universe, factors, and lookback windows change because it observes, not shortcuts
  - low fragility risk and high learning value
- Risks:
  - log noise if instrumentation is too verbose
- Mitigation:
  - make detailed logging operator-facing and stage-scoped
- Acceptance criteria:
  - one run produces clear per-stage timings and volume metrics
  - operator can identify top 2 costliest stages from logs or artifact alone

### CUSE4-002: Add Explicit Cache/Invalidation Contracts for Core Model Work

- Priority: `Now`
- Goal: unify and surface the version/invalidation signals that already exist, instead of building a generic planner too early
- Scope:
  - consolidate existing version stamps and methodology metadata for:
    - factor returns
    - risk engine state
    - raw-history build assumptions
    - eligibility methodology where applicable
  - record these stamps on existing orchestrator/job-run records where possible
  - expose these stamps in one operator-visible place
  - document the conservative widening rules when settings change
- Code areas:
  - `backend/orchestration/run_model_pipeline.py`
  - `backend/risk_model/daily_factor_returns.py`
  - `backend/risk_model/raw_cross_section_history.py`
  - cache metadata paths
- Why this matters:
  - optimization without invalidation discipline is where fragility starts
- Defense:
  - this is the safety layer that keeps future shortcuts robust under factor changes, universe changes, and lookback changes
  - it does not reduce flexibility; it makes flexibility safer
- Risks:
  - trying to over-generalize this could become a second policy engine
- Mitigation:
  - start by surfacing and standardizing what the code already tracks, then add only the missing stamps needed by later tickets
- Acceptance criteria:
  - methodology-setting changes can be detected from metadata
  - planner can explain why it chose incremental vs wider recompute

### CUSE4-002A: Add Remote-Run Safety Envelope and Cost Guardrails

- Priority: `Now`
- Goal: make remote on-demand recalcs operationally safe and cost-bounded before optimizing throughput
- Scope:
  - implement this as a small extension of the existing orchestrator/job-run model, not a separate control plane
  - treat this as cutover-preparation for future remote heavy-compute execution; it must not conflict with the current rule that `cloud-serve` only permits `serve-refresh`
  - immutable run manifest capturing:
    - requested profile
    - model/config versions
    - universe scope
    - effective lookback settings
    - requested source/output snapshots
  - idempotent run key so accidental duplicate submits do not launch duplicate heavy jobs
  - one-run-at-a-time lease/lock for heavy core jobs
  - explicit cancellation and timeout semantics
  - staged artifact retention and garbage-collection rules
  - remote execution guardrails:
    - max worker count
    - memory/CPU ceilings
    - serial fallback mode
    - reject remote full rebuild unless explicitly approved
- Code areas:
  - existing orchestration/job-run state
  - remote trigger surface
  - operator controls/docs
- Why this matters:
  - remote burst compute can become fragile and expensive even if the math is correct
  - this is the operational foundation for trustworthy on-demand recalc
- Defense:
  - these controls do not constrain your modeling flexibility
  - they stay robust as the universe, factors, and lookback windows evolve because they govern job execution, not model content
  - a simple single-worker remote path is aligned with the hobby-grade goal and avoids premature distributed complexity
  - by extending the existing orchestrator instead of adding a new control plane, this stays closer to today’s operating model
- Risks:
  - some extra orchestration state to manage
- Mitigation:
  - keep the first version deliberately narrow: one remote worker, one manifest, one final publish
- Acceptance criteria:
  - duplicate job submissions do not create duplicate heavy runs
  - operator can see exactly what a remote run intended to compute
  - remote full rebuild requires explicit opt-in
  - failed or cancelled runs do not publish partial state

### CUSE4-003: Bound the Entire Factor-Return Input Path to the Needed Window

- Priority: `Now`
- Goal: resolve uncached dates first, then bound all upstream loads needed for factor returns to the minimum safe window
- Scope:
  - determine uncached/recompute dates before full-history reshaping
  - derive the minimum required input-history window from explicit dependency metadata rather than a hand-tuned buffer
  - load only the price window needed to calculate those dates plus the required dependency window
  - bound exposure snapshot and PIT panel loads to the same effective recompute window
  - fold in snapshot-level eligibility reuse and vectorized exclusion-reason work where needed to make the bounded path actually effective
  - preserve full rebuild path when versioning or history requirements demand it
- Code areas:
  - `backend/risk_model/daily_factor_returns.py`
  - `backend/risk_model/eligibility.py`
- Why this matters:
  - this concentrates on the real `core-weekly` waste in the current code path, not just one sub-step
  - likely the highest-value safe speedup for `core-weekly` and many ad hoc recomputes
- Defense:
  - this does not change any factor math
  - if factor definitions or lookback requirements change, the invalidation layer from `CUSE4-002` forces a larger rebuild
  - if uncertainty exists, planner widens the date window instead of taking a risky shortcut
  - this remains robust under factor additions because dependency metadata, not manual assumptions, determines how much prior history must be loaded
  - this remains robust under universe growth because all upstream loads are bounded from the requested date set, not from assumptions about fixed ticker counts
- Risks:
  - incorrect windowing could underload prior prices needed for return calculation
- Mitigation:
  - require dependency-derived preload rules and fallback-to-full behavior on ambiguity
- Acceptance criteria:
  - `core-weekly` loads materially fewer price rows when only recent dates are uncached
  - outputs match full-rebuild baseline for:
    - same recompute dates
    - boundary-adjacent dates
    - at least one changed-lookback scenario
    - at least one changed-factor-schema scenario

### CUSE4-004: Optional Follow-On Eligibility Memoization if CUSE4-003 Leaves Residual Waste

- Priority: `Later`
- Goal: add deeper snapshot-level memoization only if the broader bounded-load work in `CUSE4-003` still leaves eligibility as a top bottleneck
- Scope:
  - memoize structural eligibility keyed by resolved exposure snapshot date plus explicit context fingerprint covering:
    - eligibility methodology version
    - relevant universe definition
    - upstream PIT/source lineage used by the eligibility calculation
    - security-master lineage if it can affect membership/identity
  - reuse the result across all dates mapping to that snapshot
- Code areas:
  - `backend/risk_model/eligibility.py`
  - `backend/risk_model/daily_factor_returns.py`
- Why this matters:
  - useful only if profiling shows residual repeated snapshot work after `CUSE4-003`
- Defense:
  - structural eligibility is still computed from the same underlying snapshot and PIT inputs
  - if source history, universe membership, or eligibility rules change, the key changes or wider rebuild is forced
  - this remains robust as the universe expands because it reuses exact snapshot-level work rather than assuming fixed ticker membership
- Risks:
  - stale reuse if the key is too weak
- Mitigation:
  - key must include snapshot date plus explicit context fingerprint; no reuse on partial confidence
- Acceptance criteria:
  - repeated dates mapping to the same exposure snapshot do not rebuild full eligibility from scratch
  - eligibility results match baseline

### CUSE4-005: Optional Follow-On Vectorization of Remaining Eligibility Hot Spots

- Priority: `Later`
- Goal: remove any remaining Python row-by-row work from structural eligibility generation after `CUSE4-003`
- Scope:
  - replace `DataFrame.apply()` reason assembly with vectorized mask-based construction
  - keep output reason tokens unchanged for downstream compatibility
- Code areas:
  - `backend/risk_model/eligibility.py`
- Why this matters:
  - worthwhile only if the broader bounded-load and reuse work still leaves this path hot
- Defense:
  - output contract stays the same
  - factor additions, universe growth, and holdings changes do not affect the safety of this change because it does not alter dependency structure
- Risks:
  - subtle differences in reason ordering or formatting
- Mitigation:
  - preserve exact token strings and ordering in tests
- Acceptance criteria:
  - exclusion reasons remain backward-compatible
  - structural eligibility runtime improves measurably on the same dataset

### CUSE4-006: Document and Expose the Current Trigger Matrix Through the Orchestrator

- Priority: `Now`
- Goal: keep rebuild behavior operator-visible and conservative by exposing existing scoped hooks rather than inventing a larger planner
- Scope:
  - surface existing scoped rebuild/start-end date hooks through the orchestrator and operator controls where safe
  - document the current small trigger matrix using canonical lane names:
    - holdings-only changes -> `serve-refresh`
    - latest-source updates -> `source-daily` or `source-daily-plus-core-if-due`
    - scheduled/manual core refresh -> `core-weekly`
    - methodology/history structural change -> `cold-core`
    - explicit universe onboarding finalization -> `universe-add`
  - where source/model state is unchanged, add a holdings-only fast path that reuses existing full-universe analytics and only re-projects positions/holdings outputs
  - add lightweight tests that guard the current assumption that holdings-only changes do not silently enter core model stages
  - update operator descriptions and run semantics if needed
- Code areas:
  - `backend/orchestration/run_model_pipeline.py`
  - `backend/analytics/pipeline.py`
  - operator-facing docs/routes
- Why this matters:
  - better remote usability and lower burst-compute spend
  - addresses a current user-facing bottleneck even when the heavy core model path is unchanged
- Defense:
  - this improves alignment with your actual workflows
  - changing holdings should continue to affect projections without falsely implying model-methodology change
  - if you later decide holdings edits must trigger a deeper recompute, the rule can still be explicitly added
  - this keeps the implementation small by documenting and testing the current dependency boundaries instead of building a new dependency system
- Risks:
  - dependency mistakes could cause under-refresh
- Mitigation:
  - define a small explicit trigger matrix and enforce it with tests around the current lane/profile behavior
- Acceptance criteria:
  - operator can explain why a job ran `serve-refresh`, `source-daily`, `source-daily-plus-core-if-due`, `core-weekly`, `cold-core`, or `universe-add`
  - holdings-only workflows do not trigger raw-history or full factor recompute unless explicitly requested
  - holdings-only refresh can reuse existing universe analytics when source/model state is unchanged

### CUSE4-007: Extend the Rebuild Policy Matrix Only if the Small Version Proves Insufficient

- Priority: `Only if needed`
- Goal: avoid expanding the rebuild policy surface unless operational pain proves the small matrix in `CUSE4-006` is not enough
- Scope:
  - only after evidence, consider adding one or two more explicit rebuild classes
  - do not build a generic planner
- Code areas:
  - `backend/orchestration/run_model_pipeline.py`
  - `backend/risk_model/raw_cross_section_history.py`
  - docs/runbooks
- Why this matters:
  - keeps the system simple by default
- Defense:
  - this is intentionally constrained to protect the hobby-grade operating model
  - a wider rebuild remains available whenever factor definitions, source history, lookback rules, or universe growth make partial scope ambiguous
- Risks:
  - expanded policy can become hard to trust
- Mitigation:
  - require profiling and real operator pain before adding categories
- Acceptance criteria:
  - any policy expansion is justified by measured pain and documented simply

### CUSE4-008: Reduce Repeated Style-Score Build Overhead Per Cross-Section

- Priority: `Later`
- Goal: trim CPU cost from repeated median fill, dummy creation, standardization, and orthogonalization work
- Scope:
  - remove obvious redundant conversions/copies
  - reduce repeated setup per cross-section where safe
  - include both:
    - raw-history style-score construction
    - factor-return style canonicalization path
  - preserve exact score semantics
- Code areas:
  - `backend/risk_model/raw_cross_section_history.py`
  - `backend/risk_model/descriptors.py`
  - `backend/risk_model/daily_factor_returns.py`
- Why this matters:
  - likely a meaningful `cold-core` speedup without changing model intent
- Defense:
  - this ticket should remain implementation-only
  - factor additions and weight changes remain robust if code continues to derive behavior from the factor schema rather than fixed assumptions
  - not a methodology shortcut
- Risks:
  - accidental changes to orthogonalization behavior
- Mitigation:
  - golden comparisons for representative cross-sections before/after
- Acceptance criteria:
  - score outputs match baseline within agreed tolerance
  - runtime improves measurably for raw-history rebuild

### CUSE4-009: Consider Set-Based As-Of Alignment for Raw-History Rebuild

- Priority: `Only if needed`
- Goal: replace Python-heavy per-RIC merge loops if profiling proves raw-history joins dominate runtime
- Scope:
  - evaluate more set-based SQL, DuckDB, or Polars paths for PIT alignment
  - preserve exact backward-as-of semantics
- Code areas:
  - `backend/risk_model/raw_cross_section_history.py`
  - `backend/data/cross_section_snapshot.py`
- Why this matters:
  - potential large `cold-core` improvement
- Defense:
  - explicitly deferred because this raises complexity
  - should only land if profiling shows it is worth the maintenance cost
  - exact join semantics must remain unchanged, so factor changes and universe growth continue to behave correctly
- Risks:
  - more moving parts and harder debugging
- Mitigation:
  - gate behind profiling evidence and equivalence tests
- Acceptance criteria:
  - measurable raw-history speedup
  - output equivalence against baseline on representative history

### CUSE4-010: Parallelize Per-Date Factor Regressions With Single-Writer Publish

- Priority: `Only if needed`
- Goal: exploit cloud burst compute only after the single-process waste is reduced
- Scope:
  - partition recompute by date chunks
  - compute in parallel workers
  - stage outputs and publish through one final writer
- Code areas:
  - `backend/risk_model/daily_factor_returns.py`
  - orchestration and cache-publish layers
- Why this matters:
  - this is the clearest path to getting real value from burst cloud CPUs
- Defense:
  - intentionally deferred because premature parallelism with SQLite-heavy flows would increase fragility
  - single-writer publish keeps state safe under retries and universe growth
  - methodology changes remain safe if worker inputs are versioned and staged, not written directly into live state
- Risks:
  - coordination complexity
  - write contention if implemented naively
- Mitigation:
  - require staged outputs and one final publish step
- Acceptance criteria:
  - reproducible outputs vs serial baseline
  - no partial live state if one worker fails

### CUSE4-011: Reassess SQLite as Heavy-Compute Staging Only After Earlier Tickets Land

- Priority: `Only if needed`
- Goal: decide whether file-based compute staging is still acceptable once the safe optimizations are done
- Scope:
  - evaluate whether SQLite is blocking:
    - burst cloud execution
    - concurrent worker staging
    - large intermediate writes
- Code areas:
  - core compute and cache layers
- Why this matters:
  - may become relevant for remote burst compute, but should not be pre-optimized
- Defense:
  - keeping SQLite longer is aligned with your desire for a durable hobby-grade tool
  - only change this if evidence says it is the bottleneck after simpler wins
  - this avoids unnecessary architectural sprawl while the project is still evolving
- Risks:
  - delaying too long could cap cloud-parallel upside
- Mitigation:
  - revisit only after `CUSE4-001` through `CUSE4-006` are complete
- Acceptance criteria:
  - explicit decision memo: keep SQLite staging or migrate part of the compute path

## Recommended Delivery Order

1. `CUSE4-001` profiling
2. `CUSE4-002` invalidation contracts
3. `CUSE4-002A` remote-run safety envelope and cost guardrails
4. `CUSE4-003` bounded factor-return input path
5. Re-profile
6. `CUSE4-006` document/expose the current trigger matrix and holdings-only fast path
7. `CUSE4-008` style-score / canonicalization overhead reduction
8. Re-profile again
9. Decide whether `CUSE4-004`, `CUSE4-005`, and `CUSE4-007` are still warranted
10. Decide whether `CUSE4-009` to `CUSE4-011` are warranted

## Exit Criteria Before Cloud Burst-Compute Cutover

Before moving heavy recalc into a burst-compute cloud workflow, aim to have:

- stage profiling data from real runs
- explicit invalidation/version rules
- one safe remote-run path with immutable manifest, idempotent keying, lock/lease, and staged publish
- explicit cost guardrails for remote runs
- a tested boundary proving holdings-only changes do not silently enter core model stages
- a cheap holdings-only refresh path when source/model state is unchanged
- factor-return recompute no longer full-history by default
- repeated eligibility work reduced
- clear dependency boundary between heavy core recompute and lighter serving refresh

That should give you a system that is meaningfully faster, still simple enough to trust, and better suited to affordable on-demand remote compute.
