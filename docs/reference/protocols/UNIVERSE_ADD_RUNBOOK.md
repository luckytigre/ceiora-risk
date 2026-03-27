# Universe Add Runbook

## Purpose

Use this workflow when adding new names to the platform universe.

This is the only approved operational flow for onboarding new RICs into:
- `security_registry`
- `security_policy_current`
- `security_master` compatibility surfaces
- `security_prices_eod`
- `security_fundamentals_pit`
- `security_classification_pit`

Current policy:
- this workflow is still initiated manually with Codex
- it is not yet exposed as a self-service UI action

## Preconditions

- The new names have been validated for their intended platform role.
- For each new name, you know whether it should be:
  - a single-name equity with PIT ingest enabled, or
  - a price-only / projection-style vehicle with PIT ingest disabled.
- The merge source includes at minimum:
  - `ric`
  - `ticker` is recommended but can be backfilled from the RIC or later LSEG enrichment.
- Preferred metadata also includes:
  - `isin`
  - `exchange_name`

## Step 1: Merge Into `security_registry`

- Update the canonical registry artifact:
  - `data/reference/security_registry_seed.csv`
- Bootstrap-sync the registry into the authoritative registry/policy surfaces and the compatibility mirror.
- Export and commit the refreshed primary registry artifact:
  - `python3 -m backend.scripts.export_security_registry_seed`
- If the compatibility artifact is still being maintained, also export:
  - `python3 -m backend.scripts.export_security_master_seed`
- The compatibility export must preserve `coverage_role`; do not treat a lossy export as valid.

## Step 2: Validate the Merge

Run checks immediately after the merge:
- duplicate RIC check
- duplicate alias check (`same ticker + same ISIN` should collapse to one canonical primary RIC)
- blank `ric` check
- confirm new rows are present in `security_registry`
- confirm policy flags for the new rows match the intended ingest/model path
- confirm compatibility rows are materialized for legacy readers if those readers are still in use

Recommended spot checks:
- sample a few new names and verify `ticker`, `ric`, `exchange_name`, and any seed-carried identifiers
- confirm the exported `data/reference/security_registry_seed.csv` contains the new names
- if maintaining the compatibility artifact, confirm `data/reference/security_master_seed.csv` contains the new names and still preserves `projection_only` as `coverage_role=projection_only`
- if `DATA_BACKEND=neon`, confirm the canonical registry/policy/compat rows are mirrored into Neon before relying on cloud/app reads

## Step 3: Backfill Canonical Source Tables For New RICs Only

Prices:
- pull full retained price history for the new RICs into `security_prices_eod`

Fundamentals:
- backfill monthly PIT history only for names whose active policy enables fundamentals ingest

Classification:
- backfill monthly PIT history only for names whose active policy enables classification ingest

Policy:
- do not clear the full source tables for routine universe adds
- use targeted backfills for the new names only
- explicit `--rics` / subset backfills are allowed even while new names are still pending in the readiness surfaces; LSEG enrichment during those runs is what populates the live source state

## Step 4: Coverage Audit For Added Names

Check:
- price coverage by date
- monthly fundamentals field coverage for PIT-enabled names
- monthly classification/TRBC coverage for PIT-enabled names
- presence in `security_registry`
- expected policy flags in `security_policy_current`
- projection-only names have projected outputs current to the active core package date
- projection-only names still publish as `projected_only` in both `portfolio` and `universe_loadings`; a downgrade to `ineligible` after projected outputs exist is a serving-payload failure, not an expected state
- presence in search/explore surfaces after serving refresh

## Step 5: Choose Refresh Depth

Use `serve-refresh` when:
- the add is small
- no historical model inputs were rewritten
- you only need the names to appear in serving outputs
- every newly added name already has the durable projected-output state required by the active core package, if any

Use `core-weekly` when:
- the add is moderate
- you want fresh factor returns/covariance/specific risk without rebuilding raw history
- any newly added projection-style / `projection_only` name still needs persisted `projected_instrument_*` rows for the active core package date

Use `cold-core` when:
- the add is large
- historical source data was materially rewritten
- methodology or raw-history inputs changed

## Commands

Serving-only finalization:
```bash
python3 -m backend.scripts.run_model_pipeline --profile serve-refresh
```

Projection-capable finalization after adding new projection-only names:
```bash
python3 -m backend.scripts.run_model_pipeline --profile core-weekly --force-core
```

Core recompute after add:
```bash
python3 -m backend.scripts.run_model_pipeline --profile core-weekly --force-core
```

Structural rebuild after large add or deep rewrite:
```bash
python3 -m backend.scripts.run_model_pipeline --profile cold-core --force-core
```

Operator verification:
```bash
curl -s "${BACKEND_CONTROL_ORIGIN:-http://localhost:8001}/api/operator/status" | jq
curl -s "${BACKEND_CONTROL_ORIGIN:-http://localhost:8001}/api/health" | jq
python3 -m backend.scripts.repair_serving_payloads_neon --dry-run --json | jq '.diff'
```

## Done Criteria

The universe add is complete only when:
- new names exist in `security_registry`
- current policy for the new names is correct in `security_policy_current`
- compatibility rows are refreshed if legacy readers still depend on them
- source backfills for the new names completed successfully
- coverage checks look sane
- the chosen refresh lane completed successfully
- operator status reflects the refreshed state
- frontend explore/search can see the names
