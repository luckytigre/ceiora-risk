# Cold-Core Recovery Preflight (2026-04-13)

## Context
- Failed execution to recover: `job_f3e4960faae1`
- Failure stage: `risk_model`
- Failure error: `FileNotFoundError: /app/docs/reference/migrations/neon/NEON_CANONICAL_SCHEMA.sql`

## Control Deployment Baseline
- Current control revision: `ceiora-prod-control-00020-6zs`
- Current control image digest: `sha256:b39a4f2c8fd5618b64421884386f37e1d72b88534e7a71d1776118d8412fb05d`
- Rollback control image digest: `sha256:ca728a10e3081de353c1b24a6029f7e8a55bad31bc029398c49e155c5e573ec7`

## Neon Baseline (Pre-Mutation)
- `model_factor_returns_daily`: count `55438`, min `2021-04-13`, max `2026-03-26`
- `model_factor_covariance_daily`: count `2025`, min/max `2026-03-26`
- `model_specific_risk_daily`: count `3147`, min/max `2026-03-26`
- `barra_raw_cross_section_history`: count `3742449`, min `2021-04-13`, max `2026-03-26`
- `projected_instrument_loadings`: count `1620`, min/max `2026-03-26`
- `model_run_metadata`: count `2`, max `2026-04-12 02:42:29.423107+00`
- `serving_payload_snapshots`: table not present in Neon (`relation does not exist`)

## Go/No-Go Gate
- `PASS`: baseline snapshot captured and rollback digest identified.
