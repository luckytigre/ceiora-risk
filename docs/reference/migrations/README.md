# Migration Docs

## Neon
- `neon/NEON_CANONICAL_SCHEMA.sql`
  - includes `model_factor_returns_daily.robust_se` and `model_factor_returns_daily.t_stat`
  - current canonical raw cross-section schema excludes the retired `value_score` column
- `neon/NEON_HOLDINGS_SCHEMA.sql`
- `neon/NEON_HOLDINGS_MODEL_SPEC.md`
- `neon/NEON_HOLDINGS_IMPORT_BEHAVIOR.md`

Historical Neon execution notes and the retired Stage-1 operator runbook now live under `docs/archive/migrations/neon/`.

Portfolio thesis workspace: `neon/NEON_PORTFOLIO_THESES_SCHEMA.sql` is an explicit
additive migration requiring holdings/app-auth prerequisites. See
`../../operations/THESIS_WORKSPACE_ROLLOUT.md` for owner gates and rollback behavior.
