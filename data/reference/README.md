# Reference Data

- `security_master_seed.csv`: versioned seed artifact for the canonical universe registry.

Policy:
- commit the seed artifact that defines the universe,
- do not commit live SQLite databases,
- regenerate this file with `python3 -m backend.scripts.export_security_master_seed` after approved universe changes.
