# Audit: Documentation Consistency

Date: 2026-03-16
Reviewer: Independent architecture audit
Status: Docs-vs-code review

## Summary

Documentation quality is materially better than it was before the restructure. The repo now has a credible architecture package and the canonical docs point to it.

The main documentation problems are not gross inaccuracies. They are more subtle:

- completion is overstated in a few places
- some target shapes are still aspirational
- dependency rules are stricter than actual code behavior

## What The Docs Get Right

### 1. `docs/architecture/` is now the correct place to start

This is true in practice. The package contains:

- diagnosis
- target structure
- dependency rules
- roadmap
- restructure tracker

That is a real improvement.

### 2. The operating model docs are broadly aligned with the code

`docs/ARCHITECTURE_AND_OPERATING_MODEL.md` is generally credible about:

- Neon becoming the main durable platform
- quick refresh vs deeper core lanes
- health diagnostics behavior
- frontend/operator surfaces

### 3. Top-level docs now at least distinguish active vs historical plans

`docs/README.md` and status headers now point readers toward `docs/architecture/restructure-plan.md` as the active architecture tracker.

That is directionally correct and helpful.

## Where Docs Diverge From Code

### 1. Dependency rules are stricter than the implemented routes

`docs/architecture/dependency-rules.md` says routes should be thin and should not own storage branching or inline truth assembly.

But several routes still import data-layer modules directly:

- `backend/api/routes/exposures.py`
- `backend/api/routes/risk.py`
- `backend/api/routes/portfolio.py`
- `backend/api/routes/health.py`
- `backend/api/routes/universe.py`
- `backend/api/routes/readiness.py`

This is the clearest docs-vs-code mismatch.

### 2. `restructure-plan.md` marks phases complete that are only partly complete

The most overstated items are:

- orchestration decomposition
- storage surface cleanup
- frontend contract cleanup
- documentation consolidation

The work is substantial, but the completion labels read more final than the codebase actually is.

### 3. `target-architecture.md` contains shapes that are not the implemented shape

Examples:

- `backend/api/presenters/`
- `backend/analytics/payload_builders/`
- `backend/data/model_outputs/` as a subpackage

The code uses:

- `backend/api/routes/presenters.py`
- flat helper modules under `backend/analytics/`
- flat helper modules under `backend/data/`

Because the document already says the target shape is evolutionary, this is not fatal. But it should be clearer which parts are actual current structure and which are optional future shapes.

### 4. `PROJECT_HARDENING_ORGANIZATION_PLAN.md` is marked as completed precursor, but still reads like an active execution document

The status line is correct, but the body remains long and directive. That can still confuse readers about whether it is historical context or live guidance.

## Module Responsibility Documentation

### Good

- route/service/orchestrator ownership is much better described than before
- the architecture docs name concrete files, not abstract concepts only

### Still weak

- `backend/services/` mixed ownership is not fully documented as a compromise
- the remaining direct route-to-data pattern is not called out as an exception
- the multi-store nature of operator/runtime truth is described operationally but not cleanly captured as an architectural exception

## Bottom Line

The docs are mostly aligned with the code, but they currently present the restructuring as slightly more complete and slightly more uniformly layered than it really is.

The best next documentation fix would be:

1. explicitly list the remaining accepted exceptions to the dependency rules
2. soften “completed” language where the code still has known structural exceptions
3. distinguish “implemented current shape” from “optional future target shape” more clearly in `target-architecture.md`
