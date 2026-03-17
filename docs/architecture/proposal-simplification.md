# Proposal B: Systems Simplifier

Date: 2026-03-16
Perspective: Systems Simplifier
Goal: remove accidental complexity and collapse overlap

## Core Opinion

The repo has enough structure already. The biggest win is simplification, not architecture expansion.

The current problems come from:
- too many places assembling truth
- too many large files with mixed concerns
- too many plan docs at the same level
- repeated normalization and fallback logic

The simplifier view is:
- delete and consolidate before inventing layers
- extract only where duplication already hurts
- prefer fewer named surfaces with clearer ownership

## Simplification Priorities

### 1) Collapse Repeated Route Logic

Patterns repeated today:
- load serving payload
- verify readiness
- normalize factor ids / TRBC fields / risk shares
- inject snapshot metadata

This should move into one explicit dashboard-serving service plus small presenters, not remain spread across route files.

### 2) Collapse Plan Sprawl

Current docs have too many active-looking plans.

Simplifier recommendation:
- keep `ARCHITECTURE_AND_OPERATING_MODEL.md` as the product architecture reference
- keep `OPERATIONS_PLAYBOOK.md` as the runbook
- keep one active restructuring plan in `docs/architecture/restructure-plan.md`
- treat most other top-level plans as historical execution context

### 3) Reduce Oversized Modules By Direct Extraction

Do not redesign them first. Just remove roles that obviously do not belong there.

Examples:
- move profile catalog out of `run_model_pipeline.py`
- move route payload assembly out of routes
- move `core_reads.py` latest-prices cache maintenance out of general query code
- move model-output persistence helpers into smaller files grouped by responsibility

### 4) Stop Letting “Services” Mean Everything

`backend/services/` currently contains:
- route-facing application services
- Neon infrastructure
- refresh management
- heavy operational workflows

The simplifier view does not insist on renaming the package yet, but it does insist on categorizing it internally and documenting what belongs there.

### 5) Keep Frontend Truth Quiet And Focused

Frontend already improved. Do not overbuild state management.

Instead:
- keep `analyticsTruth.ts` as the shared truth-summary layer
- avoid page-local recomputation of freshness semantics
- split large types into smaller surface-specific types only when the contracts are stable

## Simplifier Target Shape

- thin routes
- thin entry scripts
- fewer large modules
- fewer live plans
- fewer redundant payload-normalization blocks
- fewer questions about which surface is authoritative

## What This Perspective Would Delete Or Defer

Delete or demote:
- outdated active-seeming plan prominence in docs
- route-local payload normalizers duplicated across pages

Defer:
- major package renames
- broad domain/application/infrastructure package migration
- generalized repository layers

## Main Advantage

This approach has the least migration risk. It yields a cleaner repo without a large conceptual jump.

## Main Risk

If taken too literally, simplification can underinvest in explicit boundaries and leave some coupling in place.

## First Changes This Perspective Would Prioritize

1. Introduce one dashboard-serving service for `portfolio`, `risk`, and `exposures`
2. Split orchestration profile metadata from execution logic
3. Mark historical plan docs as subordinate or archive-linked from one master plan
