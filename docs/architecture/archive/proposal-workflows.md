# Proposal C: Workflow / Orchestration Architect

Date: 2026-03-16
Perspective: Workflow / Orchestration Architect
Goal: make execution paths obvious and separate reusable routines from command/control logic

## Core Opinion

The most important missing architecture boundary is between:
- tools
- routines
- workflows
- jobs
- adapters
- entrypoints

Today those concepts exist, but they are not clearly represented in module structure.

## Working Vocabulary

This proposal would standardize these meanings:

- Tool
  - a narrow helper that does one thing
  - examples: SQL sync helper, factor-name normalizer

- Routine
  - reusable computation or data shaping
  - examples: risk decomposition, factor history load, payload normalization

- Adapter
  - provider/store specific boundary
  - examples: Neon connection, SQLite cache access, serving payload persistence

- Workflow
  - a reusable multi-step business sequence
  - examples: stage refresh snapshot build, holdings import apply, what-if preview

- Job
  - a named operational run with status and checkpoints
  - examples: `serve-refresh`, `core-weekly`, `cold-core`

- Entrypoint
  - API route, CLI command, or local script that invokes a workflow or job

## Current Issues Through This Lens

1. `run_model_pipeline.py` is both workflow engine and job entrypoint and profile catalog.
2. `analytics/pipeline.py` is both routine library and workflow coordinator.
3. some services are really adapters, while some adapters are acting like workflows.
4. API routes still call routines directly in places instead of going through workflow/service seams.

## Recommended Structure

### Jobs

Keep job definitions in `backend/orchestration`, but split them into:
- profile definitions
- plan construction
- stage execution
- result publication

### Workflows

Treat these as explicit application workflows:
- holdings import/apply
- what-if preview
- dashboard payload load and readiness resolution
- refresh manager launch/reconcile

Put them in service modules with narrow names, not in route code.

### Routines

Keep reusable routines near their domain:
- risk-model math in `risk_model`
- serving payload builders in `analytics`
- route output presenters in `api/routes/presenters.py` or a small presenter module

### Adapters

Keep provider/store adapters in `backend/data` and Neon sync infrastructure modules.
Do not let them own workflow policy.

## Main Structural Recommendation

The repo should make these paths visually obvious:

1. API request path
   - route -> service/workflow -> adapters/domain

2. Refresh path
   - refresh entrypoint -> refresh manager -> orchestration job -> analytics/domain/adapters

3. CLI rebuild path
   - CLI entry -> orchestration job -> stage runner -> post-run publish

4. Serving read path
   - route -> dashboard-serving service -> durable payload / fallback rules

## Main Advantage

This structure makes the codebase easier to operate and easier to change safely because “who coordinates what” becomes obvious.

## Main Risk

If overdone, workflow separation can produce too many small wrappers. The rule should be: only make a workflow module when it materially clarifies execution ownership.

## First Changes This Perspective Would Prioritize

1. Split orchestration metadata/planning from execution.
2. Create an explicit dashboard-serving workflow/service.
3. Make route files pure entrypoints wherever practical.
