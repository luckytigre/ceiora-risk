# Universe Add Runbook

## Purpose

Use this workflow when adding new names to the platform universe.

This is the only approved operational flow for onboarding new RICs into:
- `security_master`
- `security_prices_eod`
- `security_fundamentals_pit`
- `security_classification_pit`

Current policy:
- this workflow is still initiated manually with Codex
- it is not yet exposed as a self-service UI action

## Preconditions

- The new names have been validated to be equity names you want in the coverage universe.
- The merge source includes at minimum:
  - `ric`
  - `ticker` is recommended but can be backfilled from the RIC or later LSEG enrichment.
- Preferred metadata also includes:
  - `isin`
  - `exchange_name`

## Step 1: Merge Into `security_master`

- Update the canonical merge input / registry artifact.
- Bootstrap-sync the registry into `security_master`.
- Export and commit the refreshed seed artifact:
  - `python3 -m backend.scripts.export_security_master_seed`

## Step 2: Validate the Merge

Run checks immediately after the merge:
- duplicate RIC check
- duplicate alias check (`same ticker + same ISIN` should collapse to one canonical primary RIC)
- blank `ric` check
- confirm new rows are present in `security_master`
- confirm newly added rows remain pending (`classification_ok = 0`, `is_equity_eligible = 0`) until LSEG enrichment runs

Recommended spot checks:
- sample a few new names and verify `ticker`, `ric`, `exchange_name`, and any seed-carried identifiers
- confirm the exported `data/reference/security_master_seed.csv` contains the new names
- if `DATA_BACKEND=neon`, confirm the canonical rows are mirrored into Neon `security_master` before relying on cloud/app reads

## Step 3: Backfill Canonical Source Tables For New RICs Only

Prices:
- pull full retained price history for the new RICs into `security_prices_eod`

Fundamentals:
- backfill monthly PIT history for the new RICs into `security_fundamentals_pit`

Classification:
- backfill monthly PIT history for the new RICs into `security_classification_pit`

Policy:
- do not clear the full source tables for routine universe adds
- use targeted backfills for the new names only
- explicit `--rics` / subset backfills are allowed even while new names are still pending in `security_master`; LSEG enrichment during those runs is what populates eligibility flags

## Step 4: Coverage Audit For Added Names

Check:
- price coverage by date
- monthly fundamentals field coverage
- monthly classification/TRBC coverage
- presence in `security_master`
- projection-only names have projected outputs current to the active core package date
- presence in search/explore surfaces after serving refresh

## Step 5: Choose Refresh Depth

Use `serve-refresh` when:
- the add is small
- no historical model inputs were rewritten
- you only need the names to appear in serving outputs

Use `core-weekly` when:
- the add is moderate
- you want fresh factor returns/covariance/specific risk without rebuilding raw history

Use `cold-core` when:
- the add is large
- historical source data was materially rewritten
- methodology or raw-history inputs changed

## Commands

Serving-only finalization:
```bash
python3 -m backend.scripts.run_model_pipeline --profile serve-refresh
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
curl -s "http://localhost:8000/api/operator/status" | jq
curl -s "http://localhost:8000/api/health" | jq
```

## Done Criteria

The universe add is complete only when:
- new names exist in `security_master`
- `security_master` identifiers/flags for the new names were refreshed from LSEG or derived from canonical classification data, not set manually
- source backfills for the new names completed successfully
- coverage checks look sane
- the chosen refresh lane completed successfully
- operator status reflects the refreshed state
- frontend explore/search can see the names
