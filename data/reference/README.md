# Reference Data

- `security_master_seed.csv`: versioned registry-only seed artifact for the canonical universe registry.

Policy:
- commit the seed artifact that defines the universe registry,
- do not commit live SQLite databases,
- regenerate this file with `python3 -m backend.scripts.export_security_master_seed` after approved universe changes,
- treat eligibility flags and runtime metadata as live DB state derived from canonical LSEG enrichment, not as seed-file authority.
