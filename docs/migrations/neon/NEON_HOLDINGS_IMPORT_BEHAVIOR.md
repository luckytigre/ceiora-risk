# Neon Holdings CSV Import Behavior

## Input Columns
Accepted minimum columns:
- `account_id`
- one of: `ric` or `ticker`
- `quantity`

Optional:
- `source`
- `notes`

## Resolution Order per Row
1. If `ric` is present:
   - normalize uppercase and trim.
   - verify RIC exists in canonical security registry.
   - if missing: reject row with warning.
2. Else if only `ticker` is present:
   - normalize uppercase and trim.
   - resolve `ticker -> ric` from canonical mapping.
   - if no mapping: reject row with warning.
   - if multiple active mappings: apply deterministic pick (defined below).
3. Else:
   - reject row with warning (`missing_identifier`).

## Deterministic Ticker -> RIC Pick
When multiple candidate RICs exist for a ticker, choose in this order:
1. `classification_ok = 1` and `is_equity_eligible = 1`.
2. Prefer primary-style US equity suffix ranking:
   - `.N` then `.OQ` then `.O` then `.K` then `.P`
3. If still tied, choose lexicographically smallest RIC.
4. Record all non-selected candidates in import warnings for operator visibility.

## Quantity Rules
- Parse as numeric.
- Round/store to 6 decimals.
- Zero quantities are not stored in `holdings_positions_current`.
- Negative quantities are allowed (short positions).

## Import Modes

### `replace_account`
- Replace full account holdings with CSV-resolved rows.
- Existing account rows absent in CSV are removed.

### `upsert_absolute`
- Set absolute quantity for only rows explicitly present in CSV.
- Unmentioned rows in account remain unchanged.

### `increment_delta`
- Add CSV quantity to existing quantity (`new = current + delta`).
- If result equals zero, remove that position.

## Validation Fail Policy
- Import produces:
  - `accepted_rows`
  - `rejected_rows`
  - rejection reasons by code.
- Default policy:
  - reject invalid rows,
  - apply valid rows transactionally,
  - return warning summary to UI/API.
- `account_id` is required at import submit time in UI/API.
  - Import should prompt for account if absent from file metadata/form.

## Suggested Rejection Codes
- `missing_identifier`
- `unknown_ric`
- `unknown_ticker`
- `ambiguous_ticker`
- `invalid_quantity`
- `duplicate_row_in_file`
- `invalid_account_id`
