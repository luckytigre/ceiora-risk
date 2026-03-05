-- Neon holdings schema (initial version)
-- Notes:
-- - quantity precision fixed at numeric(20,6)
-- - zero quantity rows should be pruned by application logic
-- - cost basis fields intentionally deferred

CREATE TABLE IF NOT EXISTS holdings_accounts (
    account_id TEXT PRIMARY KEY,
    account_name TEXT NOT NULL,
    broker TEXT,
    base_currency TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_holdings_accounts_account_id_fmt
        CHECK (account_id ~ '^[a-z0-9_\\-]{2,64}$')
);

CREATE TABLE IF NOT EXISTS holdings_import_batches (
    import_batch_id UUID PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES holdings_accounts(account_id),
    mode TEXT NOT NULL,
    filename TEXT,
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    requested_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes TEXT,
    CONSTRAINT chk_holdings_import_mode
        CHECK (mode IN ('replace_account', 'upsert_absolute', 'increment_delta'))
);

CREATE TABLE IF NOT EXISTS holdings_positions_current (
    account_id TEXT NOT NULL REFERENCES holdings_accounts(account_id),
    ric TEXT NOT NULL,
    ticker TEXT,
    quantity NUMERIC(20,6) NOT NULL,
    source TEXT NOT NULL,
    import_batch_id UUID REFERENCES holdings_import_batches(import_batch_id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, ric),
    CONSTRAINT chk_holdings_positions_nonzero
        CHECK (quantity <> 0)
);

CREATE TABLE IF NOT EXISTS holdings_position_events (
    event_id BIGSERIAL PRIMARY KEY,
    import_batch_id UUID REFERENCES holdings_import_batches(import_batch_id),
    account_id TEXT NOT NULL REFERENCES holdings_accounts(account_id),
    ric TEXT NOT NULL,
    ticker TEXT,
    event_type TEXT NOT NULL,
    quantity_before NUMERIC(20,6),
    quantity_delta NUMERIC(20,6),
    quantity_after NUMERIC(20,6) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT,
    CONSTRAINT chk_holdings_event_type
        CHECK (event_type IN ('set_absolute', 'increment_delta', 'remove_position', 'ui_edit')),
    CONSTRAINT chk_holdings_events_quantity_after
        CHECK (
            (event_type = 'remove_position' AND quantity_after = 0)
            OR (event_type <> 'remove_position' AND quantity_after <> 0)
        )
);

CREATE INDEX IF NOT EXISTS idx_holdings_positions_current_account
    ON holdings_positions_current (account_id);

CREATE INDEX IF NOT EXISTS idx_holdings_positions_current_ticker
    ON holdings_positions_current (ticker);

CREATE INDEX IF NOT EXISTS idx_holdings_events_account_created
    ON holdings_position_events (account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_holdings_events_ric_created
    ON holdings_position_events (ric, created_at DESC);
