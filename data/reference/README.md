# Reference Data

- `security_registry_seed.csv`: primary committed registry artifact. Current bootstrap and registry-maintenance flows should treat this as the source-controlled universe input.
- `security_master_seed.csv`: legacy compatibility export artifact. Keep it lossless while compatibility workflows, tests, or audits still consume it.

Policy:
- commit the approved `security_registry_seed.csv` artifact that defines the registry contract in use,
- do not commit live SQLite databases,
- regenerate `security_registry_seed.csv` with `python3 -m backend.scripts.export_security_registry_seed` for the primary registry-first workflow,
- regenerate `security_master_seed.csv` with `python3 -m backend.scripts.export_security_master_seed` only when the compatibility artifact is still being maintained,
- treat eligibility flags and runtime metadata as live DB state derived from canonical LSEG enrichment, not as seed-file authority.
