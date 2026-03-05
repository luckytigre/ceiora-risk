# Neon Holdings Model Spec

## Objective
Single canonical holdings store in Neon for:
- current mock holdings,
- future actual holdings,
- direct frontend edits,
- CSV imports with deterministic semantics,
- account-aware position ownership.

## Core Design
Use account + RIC as canonical position key.

## Tables

### 1) `holdings_accounts`
- `account_id` (text, PK)
- `account_name` (text, not null)
- `broker` (text, nullable)
- `base_currency` (text, nullable)
- `is_active` (boolean, default true)
- `created_at` (timestamptz, not null)
- `updated_at` (timestamptz, not null)

### 2) `holdings_positions_current`
- `account_id` (text, FK -> `holdings_accounts.account_id`)
- `ric` (text, not null)
- `ticker` (text, nullable convenience copy)
- `quantity` (numeric(20,6), not null)
- `source` (text, not null)  # ui_edit / csv_import / seed_mock / etc.
- `import_batch_id` (uuid, nullable)
- `updated_at` (timestamptz, not null)
- PK: (`account_id`, `ric`)

### 3) `holdings_import_batches`
- `import_batch_id` (uuid, PK)
- `account_id` (text, FK)
- `mode` (text, not null)  # replace_account / upsert_absolute / increment_delta
- `filename` (text, nullable)
- `row_count` (int, not null)
- `requested_by` (text, nullable)
- `created_at` (timestamptz, not null)
- `notes` (text, nullable)

### 4) `holdings_position_events` (audit ledger)
- `event_id` (bigserial, PK)
- `import_batch_id` (uuid, nullable)
- `account_id` (text, not null)
- `ric` (text, not null)
- `ticker` (text, nullable)
- `event_type` (text, not null)  # set_absolute / increment_delta / remove_position / ui_edit
- `quantity_before` (numeric(20,6), nullable)
- `quantity_delta` (numeric(20,6), nullable)
- `quantity_after` (numeric(20,6), not null)
- `created_at` (timestamptz, not null)
- `created_by` (text, nullable)
- rule:
  - `remove_position` events store `quantity_after = 0`
  - all non-remove events require non-zero `quantity_after`

## Import Modes (locked)

### A) `replace_account`
Definition: full replace for one account.
- For target account:
  - rows in CSV become the full resulting set (absolute quantities).
  - rows currently in DB but absent in CSV are removed (or set to zero then pruned).

### B) `upsert_absolute`
Definition: full replace for positions explicitly in CSV.
- For target account:
  - each CSV row sets absolute quantity for that RIC.
  - non-mentioned existing positions remain unchanged.

### C) `increment_delta`
Definition: additive update.
- For target account:
  - `new_qty = current_qty + csv_qty_delta`.
  - if resulting quantity is zero, row is removed from `holdings_positions_current`.

## Validation Rules
- `account_id` required.
- `ric` required and normalized uppercase.
- `quantity` numeric required.
- Unknown RIC behavior:
  - reject row and report in import warnings/errors.
  - no silent insert of unresolved identifiers.
- Ticker-only CSV rows:
  - resolver attempts `ticker -> ric` via canonical security mapping.
  - if mapping is ambiguous or missing, reject row with warning.
- Duplicate (`account_id`, `ric`) rows inside one CSV:
  - reject import by default for deterministic behavior.

## Account ID Convention
- Use stable slug-style identifiers, for example:
  - `ibkr_main`
  - `fidelity_roth`
  - `schwab_taxable`
- Recommended regex: `^[a-z0-9_\\-]{2,64}$`

## Display Precision (UI)
- Stored precision remains 6 decimals.
- Presentation can be adaptive:
  - large notional/whole-share names: fewer decimals.
  - small/fractional names: up to 6 when needed.

## Frontend Edit Behavior
- Direct edit always writes through `holdings_position_events`.
- `holdings_positions_current` is updated transactionally in same commit.
- UI supports account-scoped edits only.

## Why this model
- Keeps one canonical “current positions” table for fast app reads.
- Preserves full change history for audit/undo.
- Handles your three import semantics cleanly.
- Scales from single user to multi-account without redesign.
