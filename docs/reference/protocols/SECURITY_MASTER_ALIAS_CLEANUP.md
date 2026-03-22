# Security Master Alias Cleanup

Use this workflow when cleaning obvious venue/consolidated aliases out of `security_master`.

This protocol is intentionally conservative.

## Scope

Auto-delete only when all are true:
- same `ticker`
- same `isin`
- exactly two rows in the duplicate group
- one row is a clear primary identity
- the other row is a clear secondary venue/consolidated alias
- no current holdings reference the alias being removed

Anything else stays in manual review.

## Primary keep rules

- keep `.N` when exchange metadata clearly says `New York Stock Exchange`
- keep `.N` when exchange metadata clearly says `NASDAQ Stock Exchange ...` and the competing row is a consolidated Nasdaq alias
- keep `.OQ` when exchange metadata clearly says `Nasdaq ...` and is not a consolidated alias
- keep `.A` when exchange metadata clearly says `American Stock Exchange` and the competing row is an `AMEX Consolidated` alias

## Secondary alias rules

These are delete candidates only when paired against a clear primary row:
- `.K` with `New York Consolidated` or `BATS Consolidated`
- `.K` with `AMEX Consolidated`
- `.K` with `Consolidated Issue listed on NASDAQ ...`
- `.P` with `NYSE Arca`
- `.PH` with `PSX`
- `.B` with `Boston`
- `.TH` with `Third Market`
- `.C` with `National SE when trading ...`
- `.DG` with `Direct Edge - EDGX ...`
- base/no suffix with `AMEX Consolidated`

Additional reviewed-safe rules:
- keep `.N` over `.A` when `.N` is `New York Stock Exchange` and `.A` is `American Stock Exchange`
- keep `.N` over `.C` when `.C` is a National/Cincinnati-style routed alias

## Script

Dry run:
```bash
./backend/.venv/bin/python -m backend.scripts.cleanup_security_master_second_pass_aliases --json
```

Apply:
```bash
./backend/.venv/bin/python -m backend.scripts.cleanup_security_master_second_pass_aliases --apply --json
```

## Outputs

Every run writes a timestamped report directory under:
- `/tmp/ceiora-security-master-backups/second-pass/`

Artifacts include:
- `delete_candidates.csv`
- `manual_review.csv`
- `holdings_alias_hits.json`
- `summary.json`

Apply runs also back up the matching local and Neon rows before deletion.
